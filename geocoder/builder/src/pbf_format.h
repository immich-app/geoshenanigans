// Protobuf field numbers of the OSM PBF format (osmformat.proto and
// fileformat.proto), shared by the PBF readers and writers.
#pragma once

namespace BlobHeaderTag {
    constexpr int TYPE = 1;      // string
    constexpr int DATASIZE = 3;  // int32
}
namespace BlobTag {
    constexpr int RAW = 1;       // bytes
    constexpr int RAW_SIZE = 2;  // int32
    constexpr int ZLIB = 3;      // bytes
}
namespace HeaderBlockTag {
    constexpr int BBOX = 1;                         // HeaderBBox
    constexpr int REQUIRED_FEATURES = 4;            // repeated string
    constexpr int OPTIONAL_FEATURES = 5;            // repeated string
    constexpr int WRITINGPROGRAM = 16;              // string
    constexpr int SOURCE = 17;                      // string
    constexpr int REPLICATION_TIMESTAMP = 32;       // int64, seconds
    constexpr int REPLICATION_SEQUENCE_NUMBER = 33; // int64
    constexpr int REPLICATION_BASE_URL = 34;        // string
}
namespace HeaderBBoxTag {
    constexpr int LEFT = 1;    // sint64, nanodegrees
    constexpr int RIGHT = 2;
    constexpr int TOP = 3;
    constexpr int BOTTOM = 4;
}
namespace PrimitiveBlockTag {
    constexpr int STRINGTABLE = 1;   // StringTable
    constexpr int PRIMITIVEGROUP = 2; // repeated PrimitiveGroup
    constexpr int GRANULARITY = 17;  // int32 (default 100)
    constexpr int DATE_GRANULARITY = 18; // int32, milliseconds (default 1000)
    constexpr int LAT_OFFSET = 19;   // int64 (default 0)
    constexpr int LON_OFFSET = 20;   // int64 (default 0)
}
namespace PrimitiveGroupTag {
    constexpr int NODES = 1;
    constexpr int DENSE = 2;
    constexpr int WAYS = 3;
    constexpr int RELATIONS = 4;
    constexpr int CHANGESETS = 5;
}
namespace StringTableTag {
    constexpr int S = 1; // repeated bytes
}
namespace InfoTag {
    constexpr int VERSION = 1;   // int32
    constexpr int TIMESTAMP = 2; // int64, date_granularity units
    constexpr int CHANGESET = 3; // int64
    constexpr int UID = 4;       // int32
    constexpr int USER_SID = 5;  // uint32
    constexpr int VISIBLE = 6;   // bool
}
namespace DenseInfoTag {
    constexpr int VERSION = 1;   // packed int32
    constexpr int TIMESTAMP = 2; // packed sint64, delta
    constexpr int CHANGESET = 3; // packed sint64, delta
    constexpr int UID = 4;       // packed sint32, delta
    constexpr int USER_SID = 5;  // packed sint32, delta
    constexpr int VISIBLE = 6;   // packed bool
}
namespace NodeTag {
    constexpr int ID = 1;    // sint64
    constexpr int KEYS = 2;  // packed uint32
    constexpr int VALS = 3;  // packed uint32
    constexpr int INFO = 4;  // Info
    constexpr int LAT = 8;   // sint64
    constexpr int LON = 9;   // sint64
}
namespace DenseNodesTag {
    constexpr int ID = 1;
    constexpr int DENSEINFO = 5;
    constexpr int LAT = 8;
    constexpr int LON = 9;
    constexpr int KEYS_VALS = 10;
}
namespace WayTag {
    constexpr int ID = 1;
    constexpr int KEYS = 2;
    constexpr int VALS = 3;
    constexpr int INFO = 4;
    constexpr int REFS = 8;
}
namespace RelationTag {
    constexpr int ID = 1;
    constexpr int KEYS = 2;
    constexpr int VALS = 3;
    constexpr int INFO = 4;
    constexpr int ROLES_SID = 8;
    constexpr int MEMIDS = 9;
    constexpr int TYPES = 10;
}
