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
            if (p.relations[i].info === null) p.relations[i].info = {}
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
            var out = {
              type: 'relation',
              id: p.relations[i].id,
              members: members,
              tags: tags
            }
            if (p.relations[i].info !== null) {
              if (p.relations[i].info.version !== undefined)   out.version   = p.relations[i].info.version
              if (p.relations[i].info.timestamp !== undefined) out.timestamp = new Date(p.relations[i].info.timestamp*osmData.date_granularity).toISOString().substr(0, 19) + 'Z'
              if (p.relations[i].info.changeset !== undefined) out.changeset = p.relations[i].info.changeset
              if (p.relations[i].info.uid !== undefined)       out.uid       = p.relations[i].info.uid
              if (p.relations[i].info.user_sid !== undefined)  out.user      = strings[p.relations[i].info.user_sid]
              if (p.relations[i].info.visible !== undefined)   out.visible   = p.relations[i].info.visible
            }
            output.push(out)
          }
        break
        case p.ways.length > 0:
          for (var i=0; i<p.ways.length; i++) {
            if (p.ways[i].info === null) p.ways[i].info = {}
            var tags = {}
            for (var j=0; j<p.ways[i].keys.length; j++)
              tags[strings[p.ways[i].keys[j]]] = strings[p.ways[i].vals[j]]
            var nodes = [], ref = 0
            for (var j=0; j<p.ways[i].refs.length; j++)
              nodes.push(ref += p.ways[i].refs[j])
            var out = {
              type: 'way',
              id: p.ways[i].id,
              nodes: nodes,
              tags: tags
            }
            if (p.ways[i].info !== null) {
              if (p.ways[i].info.version !== undefined)   out.version   = p.ways[i].info.version
              if (p.ways[i].info.timestamp !== undefined) out.timestamp = new Date(p.ways[i].info.timestamp*osmData.date_granularity).toISOString().substr(0, 19) + 'Z'
              if (p.ways[i].info.changeset !== undefined) out.changeset = p.ways[i].info.changeset
              if (p.ways[i].info.uid !== undefined)       out.uid       = p.ways[i].info.uid
              if (p.ways[i].info.user_sid !== undefined)  out.user      = strings[p.ways[i].info.user_sid]
              if (p.ways[i].info.visible !== undefined)   out.visible   = p.ways[i].info.visible
            }
            output.push(out)
          }
        break
        case p.nodes.length > 0:
          for (var i=0; i<p.nodes.length; i++) {
            if (p.nodes[i].info === null) p.nodes[i].info = {}
            var tags = {}
            for (var j=0; j<p.nodes[i].keys.length; j++)
              tags[strings[p.nodes[i].keys[j]]] = strings[p.nodes[i].vals[j]]
            var out = {
              type: 'node',
              id: p.nodes[i].id,
              tags: tags
            }
            if (p.nodes[i].info !== null) {
              if (p.nodes[i].info.version !== undefined)   out.version   = p.nodes[i].info.version
              if (p.nodes[i].info.timestamp !== undefined) out.timestamp = new Date(p.nodes[i].info.timestamp*osmData.date_granularity).toISOString().substr(0, 19) + 'Z'
              if (p.nodes[i].info.changeset !== undefined) out.changeset = p.nodes[i].info.changeset
              if (p.nodes[i].info.uid !== undefined)       out.uid       = p.nodes[i].info.uid
              if (p.nodes[i].info.user_sid !== undefined)  out.user      = strings[p.nodes[i].info.user_sid]
              if (p.nodes[i].info.visible !== undefined)   out.visible   = p.nodes[i].info.visible
            }
            output.push(out)
          }
        break
        case p.dense !== null:
          var id=0,lat=0,lon=0,timestamp=0,changeset=0,uid=0,user=0 //todo:visible
          var hasDenseinfo = true
          if (p.dense.denseinfo === null) {
            hasDenseinfo = false
            p.dense.denseinfo = {
              timestamp: [],
              changeset: [],
              uid: [],
              user_sid: [],
              version: []
            }
          }
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
            if (p.dense.keys_vals.length > 0) {
              while (p.dense.keys_vals[j] != 0) {
                tags[strings[p.dense.keys_vals[j]]] = strings[p.dense.keys_vals[j+1]]
                j += 2
              }
              j++
            }
            var out = {
              type: 'node',
              id: id,
              lat: 1E-9 * (osmData.lat_offset + (osmData.granularity * lat)),
              lon: 1E-9 * (osmData.lon_offset + (osmData.granularity * lon)),
              tags: tags
            }
            if (hasDenseinfo) {
              if (p.dense.denseinfo.version !== null)   out.version   = p.dense.denseinfo.version[i]
              if (p.dense.denseinfo.timestamp !== null) out.timestamp = new Date(timestamp*osmData.date_granularity).toISOString().substr(0, 19) + 'Z'
              if (p.dense.denseinfo.changeset !== null) out.changeset = changeset
              if (p.dense.denseinfo.uid !== null)       out.uid       = uid
              if (p.dense.denseinfo.user_sid !== null)  out.user      = strings[user]
              if (p.dense.denseinfo.visible !== null)   out.visible   = p.dense.denseinfo.visible[i]
            }
            output.push(out)
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
