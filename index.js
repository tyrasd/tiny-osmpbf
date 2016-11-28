var inflate = require('tiny-inflate')
var Pbf = require('pbf')
var FileFormat = require('./proto/fileformat.js')
var OsmFormat = require('./proto/osmformat.js')

var memberTypes = {
  0: 'node',
  1: 'way',
  2: 'relation'
}

var supportedFeatures = {
  "OsmSchema-V0.6": true,
  "DenseNodes": true,
  "HistoricalInformation": true
}


// extracts and decompresses a data blob
function extractBlobData(blob) {
  // todo: add tests for non-zlib cases
  switch (true) {
    // error cases:

    // * lzma compressed data (support for this kind of data is not required by the specs)
    case blob.lzma_data !== null:
      throw new Error("unsupported osmpbf blob data type: lzma_data")
    // * formerly used for bzip2 compressed data, deprecated since 2010
    case blob.OBSOLETE_bzip2_data !== null:
      throw new Error("unsupported osmpbf blob data type: OBSOLETE_bzip2_data")
    // * empty data blob??
    default:
      throw new Error("unsupported osmpbf blob data type: <empty blob>")

    // supported data formats:

    // * uncompressed data
    case blob.raw !== null:
      return blob.raw
    // * zlib "deflate" compressed data
    case blob.zlib_data !== null:
      var blobData = new Buffer(blob.raw_size)
      inflate(blob.zlib_data.slice(2), blobData)
      return blobData
  }
}

/* main function of the library
 * input: osmpbf data as a javascript arraybuffer
 * handler: (optional) callback that is called for each osm element
 * return value: a OSM-JSON object with file metadata and (if no custom handler
 *               is specified) all parsed osm elements in an array
 */
module.exports = function(input, handler) {
  // default element handler: save them in an array to be returned at the end
  var elements = undefined
  if (handler === undefined) {
    var elements = []
    handler = function(element) {
      elements.push(element)
    }
  }

  var blobHeaderLength, blobHeader, blob, blobData

  /* A osm pbf file contains a repeating sequence of fileblocks:
  * blobHeaderLength: length of the following blobHeader message (32 bit
  *                   integer, big endian/network byte order)
  * blobHeader: pbf-serialized BlobHeader message, containing the size of
  *             the following blobData message
  * blob: pbf-serialized Blob message (contains compressed osm data)
  */

  pbf = new Pbf(input)
  pbf.length = 0
  // helper function to wind pbf reader forward
  pbf.forward = function(nextLength, relative) {
    this.pos = this.length
    this.length += nextLength
  }

  pbf.forward(4)
  blobHeaderLength = new DataView(new Uint8Array(input).buffer).getInt32(pbf.pos, false)

  // we now know the length of the first blobHeader: wind the pbf buffer forward and parse the data

  pbf.forward(blobHeaderLength)
  blobHeader = FileFormat.BlobHeader.read(pbf)

  /* A BlobHeader contains information about the following data blob.
   *
   * Definition:
   *   message BlobHeader {
   *     required string type = 1;
   *     optional bytes indexdata = 2;
   *     required int32 datasize = 3;
   *   }
   *
   * Example:
   *   { type: 'OSMHeader', indexdata: null, datasize: 72 }
   */

  pbf.forward(blobHeader.datasize)
  blob = FileFormat.Blob.read(pbf)

  /* A blob is used to store an (either uncompressed or zlib/deflate compressed)
   * blob of osm data.
   *
   * Definition:
   *   message Blob {
   *     optional bytes raw = 1; // No compression
   *     optional int32 raw_size = 2; // When compressed, the uncompressed size
   *     // Possible compressed versions of the data.
   *     optional bytes zlib_data = 3;
   *     // PROPOSED feature for LZMA compressed data. SUPPORT IS NOT REQUIRED.
   *     optional bytes lzma_data = 4;
   *     // Formerly used for bzip2 compressed data. Depreciated (sic) in 2010.
   *     optional bytes OBSOLETE_bzip2_data = 5 [deprecated=true]; // Don't reuse this tag number.
   *   }
   */

  blobData = extractBlobData(blob)

  // blobData is still now a protocol buffer (pbf) message

  var osmHeader = new Pbf(blobData)
  osmHeader = OsmFormat.HeaderBlock.read(osmHeader)

  /* The first blob of an osm pbf file must contain an osmHeader message. It
   * contains several metadata fields about the osmpbf file (timestamps, source
   * string, etc.). It also indicated which features a parser must support in
   * order to correctly parse the file.
   *
   * Definition:
   *   message HeaderBlock {
   *     optional HeaderBBox bbox = 1;
   *     // Additional tags to aid in parsing this dataset
   *     repeated string required_features = 4;
   *     repeated string optional_features = 5;
   *     optional string writingprogram = 16;
   *     optional string source = 17; // From the bbox field.
   *     // Tags that allow continuing an Osmosis replication:
   *     // replication timestamp, expressed in seconds since the epoch,
   *     // otherwise the same value as in the "timestamp=..." field
   *     // in the state.txt file used by Osmosis
   *     optional int64 osmosis_replication_timestamp = 32;
   *     // replication sequence number (sequenceNumber in state.txt)
   *     optional int64 osmosis_replication_sequence_number = 33;
   *     // replication base URL (from Osmosis' configuration.txt file)
   *     optional string osmosis_replication_base_url = 34;
   *   }
   *
   * Example:
   *   { bbox: null,
   *     required_features: [ 'OsmSchema-V0.6', 'DenseNodes' ],
   *     optional_features: [],
   *     writingprogram: 'Overpass API prototype',
   *     source: '',
   *     osmosis_replication_timestamp: 1462060800,
   *     osmosis_replication_sequence_number: 0,
   *     osmosis_replication_base_url: '' }
   */

  // check for required_features
  var missingFeatures = osmHeader.required_features.filter(function(requiredFeature) {
    return !supportedFeatures[requiredFeature]
  })
  if (missingFeatures.length > 0) {
    throw new Error("unsupported required osmpbf feature(s): " + missingFeatures.join(', '))
  }

  // read all data blobs
  while (pbf.pos < input.byteLength) {

    pbf.forward(4)
    blobHeaderLength = new DataView(new Uint8Array(input).buffer).getInt32(pbf.pos, false)
    pbf.forward(blobHeaderLength)
    blobHeader = FileFormat.BlobHeader.read(pbf)

    // the blobHeader contains the size of the following data blob

    pbf.forward(blobHeader.datasize)
    blob = FileFormat.Blob.read(pbf)

    blobData = extractBlobData(blob)

    var osmData = new Pbf(blobData)
    osmData = OsmFormat.PrimitiveBlock.read(osmData)

    /* The actual OSM data is stored in a list of PrimitiveBlock messages. Each
     * one contains a some metadata about the data in this block (e.g. lat/lon
     * offsets), a stringtable for tag keys/values (and user names) and a list
     * of "PrimitiveGroup"s, each containing a list of OSM element of the same
     * type (i.e. nodes, ways or relations).
     *
     * Definition:
     *   message PrimitiveBlock {
     *     required StringTable stringtable = 1;
     *     repeated PrimitiveGroup primitivegroup = 2;
     *     // Granularity, units of nanodegrees, used to store coordinates in this block
     *     optional int32 granularity = 17 [default=100];
     *     // Offset value between the output coordinates coordinates and the granularity grid, in units of nanodegrees.
     *     optional int64 lat_offset = 19 [default=0];
     *     optional int64 lon_offset = 20 [default=0];
     *     // Granularity of dates, normally represented in units of milliseconds since the 1970 epoch.
     *     optional int32 date_granularity = 18 [default=1000];
     *     // Proposed extension:
     *     //optional BBox bbox = XX;
     *   }
     *
     * Example:
     *   { stringtable: { s: [ [Object], [Object], [Object], [Object] ] },
     *     primitivegroup:
     *      [ { nodes: [],
     *          dense: [Object],
     *          ways: [],
     *          relations: [],
     *          changesets: [] } ],
     *     granularity: 100,
     *     lat_offset: 0,
     *     lon_offset: 0,
     *     date_granularity: 1000 }
     */

    // unpack stringtable into js object
    var strings = osmData.stringtable.s.map(function(x) {
      return new Buffer(x).toString('utf8')
    })

    // date granularity: set default values if not specified in the pbf file
    osmData.date_granularity = osmData.date_granularity || 1000
    // coordinate granularity: set default, invert and pre-scale to nano-degrees
    // (inversion helps to eliminate double precision rounding errors later on)
    if (!osmData.granularity || osmData.granularity === 100)
      osmData.granularity = 1E7
    else
      osmData.granularity = 1E9/osmData.granularity
    // pre-scale lat/lon offsets
    osmData.lat_offset *= 1E-9
    osmData.lon_offset *= 1E-9

    // iterate over all groups of osm objects
    osmData.primitivegroup.forEach(function(p) {
      /* Each "primitivegroup" can either be a list of changesets, relations, ways, nodes or "dense" nodes
       *
       * Definition:
       *   message PrimitiveGroup {
       *     repeated Node     nodes = 1;
       *     optional DenseNodes dense = 2;
       *     repeated Way      ways = 3;
       *     repeated Relation relations = 4;
       *     repeated ChangeSet changesets = 5;
       *   }
       */
      switch(true) {
        // error cases:

        // * changesets
        case p.changesets.length > 0:
          throw new Error("unsupported osmpbf primitive group data: changesets")
        // * empty primitivegroup ???
        default:
          throw new Error("unsupported osmpbf primitive group data: <empty primitivegroup>")

        // supported data cases:

        /* A list of osm relations.
         *
         * Definition:
         *   message Relation {
         *     enum MemberType {
         *       NODE = 0;
         *       WAY = 1;
         *       RELATION = 2;
         *     }
         *     required int64 id = 1;
         *     // Parallel arrays.
         *     repeated uint32 keys = 2 [packed = true];
         *     repeated uint32 vals = 3 [packed = true];
         *     optional Info info = 4;
         *     // Parallel arrays
         *     repeated int32 roles_sid = 8 [packed = true];
         *     repeated sint64 memids = 9 [packed = true]; // DELTA encoded
         *     repeated MemberType types = 10 [packed = true];
         *   }
         *
         *   message Info {
         *      optional int32 version = 1 [default = -1];
         *      optional int32 timestamp = 2;
         *      optional int64 changeset = 3;
         *      optional int32 uid = 4;
         *      optional int32 user_sid = 5; // String IDs
         *      // The visible flag is used to store history information. It indicates that
         *      // the current object version has been created by a delete operation on the
         *      // OSM API.
         *      // When a writer sets this flag, it MUST add a required_features tag with
         *      // value "HistoricalInformation" to the HeaderBlock.
         *      // If this flag is not available for some object it MUST be assumed to be
         *      // true if the file has the required_features tag "HistoricalInformation"
         *      // set.
         *      optional bool visible = 6;
         *   }
         */
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
            var out = {
              type: 'relation',
              id: p.relations[i].id,
              members: members,
              tags: tags
            }
            if (p.relations[i].info !== null) {
              if (p.relations[i].info.version !== 0)   out.version   = p.relations[i].info.version
              if (p.relations[i].info.timestamp !== 0) out.timestamp = new Date(p.relations[i].info.timestamp*osmData.date_granularity).toISOString().substr(0, 19) + 'Z'
              if (p.relations[i].info.changeset !== 0) out.changeset = p.relations[i].info.changeset
              if (p.relations[i].info.uid !== 0)       out.uid       = p.relations[i].info.uid
              if (p.relations[i].info.user_sid !== 0)  out.user      = strings[p.relations[i].info.user_sid]
              if (p.relations[i].info.visible !== undefined) out.visible   = p.relations[i].info.visible
            }
            handler(out)
          }
        break

        /* A list of osm ways
         *
         * Definition:
         *   message Way {
         *      required int64 id = 1;
         *      // Parallel arrays.
         *      repeated uint32 keys = 2 [packed = true];
         *      repeated uint32 vals = 3 [packed = true];
         *      optional Info info = 4;
         *      repeated sint64 refs = 8 [packed = true];  // DELTA coded
         *   }
         */
        case p.ways.length > 0:
          for (var i=0; i<p.ways.length; i++) {
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
              if (p.ways[i].info.version !== 0)   out.version   = p.ways[i].info.version
              if (p.ways[i].info.timestamp !== 0) out.timestamp = new Date(p.ways[i].info.timestamp*osmData.date_granularity).toISOString().substr(0, 19) + 'Z'
              if (p.ways[i].info.changeset !== 0) out.changeset = p.ways[i].info.changeset
              if (p.ways[i].info.uid !== 0)       out.uid       = p.ways[i].info.uid
              if (p.ways[i].info.user_sid !== 0)  out.user      = strings[p.ways[i].info.user_sid]
              if (p.ways[i].info.visible !== undefined) out.visible   = p.ways[i].info.visible
            }
            handler(out)
          }
        break

        /* A basic list of osm nodes (dense nodes are more common, see below)
         *
         * Definition:
         *   message Node {
         *     required sint64 id = 1;
         *     // Parallel arrays.
         *     repeated uint32 keys = 2 [packed = true]; // String IDs.
         *     repeated uint32 vals = 3 [packed = true]; // String IDs.
         *     optional Info info = 4;
         *     required sint64 lat = 8;
         *     required sint64 lon = 9;
         *   }
         */
        case p.nodes.length > 0:
          for (var i=0; i<p.nodes.length; i++) {
            var tags = {}
            for (var j=0; j<p.nodes[i].keys.length; j++)
              tags[strings[p.nodes[i].keys[j]]] = strings[p.nodes[i].vals[j]]
            var out = {
              type: 'node',
              id: p.nodes[i].id,
              lat: osmData.lat_offset + p.nodes[i].lat / osmData.granularity,
              lon: osmData.lon_offset + p.nodes[i].lon / osmData.granularity,
              tags: tags
            }
            if (p.nodes[i].info !== null) {
              if (p.nodes[i].info.version !== 0)   out.version   = p.nodes[i].info.version
              if (p.nodes[i].info.timestamp !== 0) out.timestamp = new Date(p.nodes[i].info.timestamp*osmData.date_granularity).toISOString().substr(0, 19) + 'Z'
              if (p.nodes[i].info.changeset !== 0) out.changeset = p.nodes[i].info.changeset
              if (p.nodes[i].info.uid !== 0)       out.uid       = p.nodes[i].info.uid
              if (p.nodes[i].info.user_sid !== 0)  out.user      = strings[p.nodes[i].info.user_sid]
              if (p.nodes[i].info.visible !== undefined) out.visible   = p.nodes[i].info.visible
            }
            handler(out)
          }
        break

        /* A "dense" list of osm nodes that uses a better packed & delta-encoded
         * format:
         *
         * Definition:
         *   message DenseNodes {
         *     repeated sint64 id = 1 [packed = true]; // DELTA coded
         *     //repeated Info info = 4;
         *     optional DenseInfo denseinfo = 5;
         *     repeated sint64 lat = 8 [packed = true]; // DELTA coded
         *     repeated sint64 lon = 9 [packed = true]; // DELTA coded
         *     // Special packing of keys and vals into one array. May be empty if all nodes in this block are tagless.
         *     repeated int32 keys_vals = 10 [packed = true];
         *   }
         *
         *   message DenseInfo {
         *      repeated int32 version = 1 [packed = true];
         *      repeated sint64 timestamp = 2 [packed = true]; // DELTA coded
         *      repeated sint64 changeset = 3 [packed = true]; // DELTA coded
         *      repeated sint32 uid = 4 [packed = true]; // DELTA coded
         *      repeated sint32 user_sid = 5 [packed = true]; // String IDs for usernames. DELTA coded
         *      // The visible flag is used to store history information. It indicates that
         *      // the current object version has been created by a delete operation on the
         *      // OSM API.
         *      // When a writer sets this flag, it MUST add a required_features tag with
         *      // value "HistoricalInformation" to the HeaderBlock.
         *      // If this flag is not available for some object it MUST be assumed to be
         *      // true if the file has the required_features tag "HistoricalInformation"
         *      // set.
         *      repeated bool visible = 6 [packed = true];
         *   }
         */
        case p.dense !== null:
          var id=0,lat=0,lon=0,timestamp=0,changeset=0,uid=0,user=0
          var hasDenseinfo = true
          if (p.dense.denseinfo === null) {
            hasDenseinfo = false
            p.dense.denseinfo = {
              version: [],
              timestamp: [],
              changeset: [],
              uid: [],
              user_sid: [],
              visible: []
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
            /* tag keys and values are encoded as a single array of stringid's:
             * the pattern is: ((<keyid> <valid>)* '0' )*
             * (each node's tags are encoded as alternating <keyid> <valid>, a
             * single stringid of 0 delimits tags of one node from tags of the
             * next node.)
             * if no node in the primitivegroup has a tag, it can be left empty.
             */
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
              lat: osmData.lat_offset + lat / osmData.granularity,
              lon: osmData.lon_offset + lon / osmData.granularity,
              tags: tags
            }
            if (hasDenseinfo) {
              if (p.dense.denseinfo.version.length > 0)   out.version   = p.dense.denseinfo.version[i]
              if (p.dense.denseinfo.timestamp.length > 0) out.timestamp = new Date(timestamp*osmData.date_granularity).toISOString().substr(0, 19) + 'Z'
              if (p.dense.denseinfo.changeset.length > 0) out.changeset = changeset
              if (p.dense.denseinfo.uid.length > 0)       out.uid       = uid
              if (p.dense.denseinfo.user_sid.length > 0)  out.user      = strings[user]
              if (p.dense.denseinfo.visible.length > 0)   out.visible   = p.dense.denseinfo.visible[i]
            }
            handler(out)
          }
        break
      }
    })

  }

  // return collected data in OSM-JSON format (as used by Overpass API)
  var output = {
    "version": 0.6,
    "generator": osmHeader.writingprogram || "tiny-osmpbf",
  }
  if (osmHeader.source !== "" || osmHeader.osmosis_replication_timestamp !== 0) {
    output.osm3s = {}
    if (osmHeader.source !== "") {
      output.osm3s.copyright = osmHeader.source
    }
    if (osmHeader.osmosis_replication_timestamp !== 0) {
      output.osm3s.timestamp_osm_base = new Date(osmHeader.osmosis_replication_timestamp*1000).toISOString().substr(0, 19) + 'Z'
    }
  }
  if (osmHeader.bbox !== null) {
    output.bounds = {
      "minlat": 1E-9 * osmHeader.bbox.bottom,
      "minlon": 1E-9 * osmHeader.bbox.left,
      "maxlat": 1E-9 * osmHeader.bbox.top,
      "maxlon": 1E-9 * osmHeader.bbox.right
    }
  }
  output.elements = elements
  return output
}
