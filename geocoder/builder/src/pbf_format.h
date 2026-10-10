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
namespace PrimitiveBlockTag {
    constexpr int STRINGTABLE = 1;   // StringTable
    constexpr int PRIMITIVEGROUP = 2; // repeated PrimitiveGroup
    constexpr int GRANULARITY = 17;  // int32 (default 100)
    constexpr int LAT_OFFSET = 19;   // int64 (default 0)
    constexpr int LON_OFFSET = 20;   // int64 (default 0)
}
namespace PrimitiveGroupTag {
    constexpr int NODES = 1;
    constexpr int DENSE = 2;
    constexpr int WAYS = 3;
    constexpr int RELATIONS = 4;
}
namespace StringTableTag {
    constexpr int S = 1; // repeated bytes
}
namespace DenseNodesTag {
    constexpr int ID = 1;
    constexpr int LAT = 8;
    constexpr int LON = 9;
    constexpr int KEYS_VALS = 10;
}
namespace WayTag {
    constexpr int ID = 1;
    constexpr int KEYS = 2;
    constexpr int VALS = 3;
    constexpr int REFS = 8;
}
namespace RelationTag {
    constexpr int ID = 1;
    constexpr int KEYS = 2;
    constexpr int VALS = 3;
    constexpr int ROLES_SID = 8;
    constexpr int MEMIDS = 9;
    constexpr int TYPES = 10;
}
