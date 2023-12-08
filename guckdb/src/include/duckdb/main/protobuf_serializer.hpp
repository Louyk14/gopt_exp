//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/protobuf_serializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

    class PbSerializer {
    public:
        PbSerializer(){};

        void Serialize(std::string* info, PhysicalOperator* physical_operator, unordered_map<int, std::string>& tableid2name);
        void SerializeToFile(std::string& file_name, PhysicalOperator* physical_operator, unordered_map<int, std::string>& tableid2name);
    };
}
