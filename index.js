var inflate = require('tiny-inflate')
var Pbf = require('pbf')
var FileFormat = require('./proto/fileformat.js')
var OsmFormat = require('./proto/osmformat.js')

var memberTypes = {
  0: 'node',
  1: 'way',
  2: 'relation'
}

module.exports = function(input) {
  var output = []

  var blobHeaderLength, blobHeader, blob, blobData

  pbf = new Pbf(input)

  blobHeaderLength = new DataView(new Uint8Array(input).buffer).getInt32(pbf.pos, false)

  pbf.pos += 4
  pbf.length = pbf.pos + blobHeaderLength
  blobHeader = FileFormat.BlobHeader.read(pbf)

  //console.log(blobHeader)

  pbf.pos = pbf.length
  pbf.length = pbf.pos + blobHeader.datasize
  blob = FileFormat.Blob.read(pbf)

  // todo: uncompressed data
  blobData = new Buffer(blob.raw_size)
  inflate(blob.zlib_data.slice(2), blobData)

  var osmHeader = new Pbf(blobData)
  osmHeader = OsmFormat.HeaderBlock.read(osmHeader)
  // todo: check osm header data (required_features)

  //console.log(osmHeader)


  // read data blocks
  while (pbf.pos < input.byteLength) {

    pbf.pos = pbf.length
    blobHeaderLength = new DataView(new Uint8Array(input).buffer).getInt32(pbf.pos, false)
    pbf.pos += 4
    pbf.length = pbf.pos + blobHeaderLength
    blobHeader = FileFormat.BlobHeader.read(pbf)

    //console.log(blobHeaderLength, blobHeader)

    pbf.pos = pbf.length
    pbf.length = pbf.pos + blobHeader.datasize
    blob = FileFormat.Blob.read(pbf)
    // todo: uncompressed data
    blobData = new Buffer(blob.raw_size)
    inflate(blob.zlib_data.slice(2), blobData)

    var osmData = new Pbf(blobData)
    osmData = OsmFormat.PrimitiveBlock.read(osmData)

    //console.log(osmData)
    //console.log(osmData.primitivegroup[0].dense)

    var strings = osmData.stringtable.s.map(function(x) {
      return new Buffer(x).toString('utf8')
    })

    //console.log(strings)

    osmData.granularity = osmData.granularity || 100
    osmData.date_granularity = osmData.date_granularity || 1000
    osmData.primitivegroup.forEach(function(p) {
      switch(true) {
        case p.changesets.length > 0:
        default:
          console.error("unsupported osmpbf primitive group data", p)
        break
        case p.relations.length > 0:
          for (var i=0; i<p.relations.length; i++) {
            var tags = {}
            for (var j=0; j<p.relations[i].keys.length; j++)
              tags[strings[p.relations[i].keys[j]]] = strings[p.relations[i].vals[j]]
            var members = [], ref = 0
            for (var j=0; j<p.relations[i].memids.length; j++)
              members.push({
                type: memberTypes[p.relations[i].types[j]],
                ref: ref += p.relations[i].memids[j],
                role: strings[p.relations[i].roles_sid[j]]
              })
            output.push({
              type: 'relation',
              id: p.relations[i].id,
              version: p.relations[i].info.version,
              timestamp: new Date(p.relations[i].info.timestamp*osmData.date_granularity).toISOString().substr(0, 19) + 'Z',
              changeset: p.relations[i].info.changeset,
              uid: p.relations[i].info.uid,
              user: strings[p.relations[i].info.user_sid],
              // todo: visible
              members: members,
              tags: tags
            })
          }
        break
        case p.ways.length > 0:
          for (var i=0; i<p.ways.length; i++) {
            var tags = {}
            for (var j=0; j<p.ways[i].keys.length; j++)
              tags[strings[p.ways[i].keys[j]]] = strings[p.ways[i].vals[j]]
            var nodes = [], ref = 0
            for (var j=0; j<p.ways[i].refs.length; j++)
              nodes.push(ref += p.ways[i].refs[j])
            output.push({
              type: 'way',
              id: p.ways[i].id,
              version: p.ways[i].info.version,
              timestamp: new Date(p.ways[i].info.timestamp*osmData.date_granularity).toISOString().substr(0, 19) + 'Z',
              changeset: p.ways[i].info.changeset,
              uid: p.ways[i].info.uid,
              user: strings[p.ways[i].info.user_sid],
              // todo: visible
              nodes: nodes,
              tags: tags
            })
          }
        break
        case p.nodes.length > 0:
          for (var i=0; i<p.nodes.length; i++) {
            var tags = {}
            for (var j=0; j<p.nodes[i].keys.length; j++)
              tags[strings[p.nodes[i].keys[j]]] = strings[p.nodes[i].vals[j]]
            output.push({
              type: 'node',
              id: p.nodes[i].id,
              version: p.nodes[i].info.version,
              timestamp: new Date(p.nodes[i].info.timestamp*osmData.date_granularity).toISOString().substr(0, 19) + 'Z',
              changeset: p.nodes[i].info.changeset,
              uid: p.nodes[i].info.uid,
              user: strings[p.nodes[i].info.user_sid],
              // todo: visible
              tags: tags
            })
          }
        break
        case p.dense !== null:
          var id=0,lat=0,lon=0,timestamp=0,changeset=0,uid=0,user=0 //todo:visible
          var j=0
          for (var i=0; i<Math.max(p.dense.id.length, p.dense.lat.length); i++) {
            id += p.dense.id[i]
            lat += p.dense.lat[i]
            lon += p.dense.lon[i]
            timestamp += p.dense.denseinfo.timestamp[i]
            changeset += p.dense.denseinfo.changeset[i]
            uid += p.dense.denseinfo.uid[i]
            user += p.dense.denseinfo.user_sid[i]
            var tags = {}
            while (p.dense.keys_vals[j] != 0) {
              tags[strings[p.dense.keys_vals[j]]] = strings[p.dense.keys_vals[j+1]]
              j += 2
            }
            j++
            output.push({
              type: 'node',
              id: id,
              version: p.dense.denseinfo.version[i],
              lat: 1E-9 * (osmData.lat_offset + (osmData.granularity * lat)),
              lon: 1E-9 * (osmData.lon_offset + (osmData.granularity * lon)),
              timestamp: new Date(timestamp*osmData.date_granularity).toISOString().substr(0, 19) + 'Z',
              changeset: changeset,
              uid: uid,
              user: strings[user],
              tags: tags
            })
          }
      }
    })


  }

  return {
    version: 0.6,
    //todo: other stuff
    elements: output
  }
}
