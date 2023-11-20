#include "duckdb/main/protobuf_serializer.hpp"
#include "duckdb/protocode/algebra.pb.h"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"

#include <unordered_set>
#include <string>
#include <chrono>
#include <iostream>
#include <fstream>

using namespace duckdb;
using namespace std;

void PbSerializer::Serialize(std::string *info, duckdb::PhysicalOperator* physical_operator, unordered_map<int, string>& tableid2name) {
    substrait::Rel* relation_entry = physical_operator->ToSubstraitClass(tableid2name);
    relation_entry->SerializeToString(info);

    relation_entry->Clear();
    delete relation_entry;
}

void PbSerializer::SerializeToFile(string& file_name, PhysicalOperator* physical_operator, unordered_map<int, string>& tableid2name) {
    substrait::Rel* relation_entry = physical_operator->ToSubstraitClass(tableid2name);
    ofstream out_file(file_name, ios::out|ios::binary);
    relation_entry->SerializePartialToOstream(&out_file);

    delete relation_entry;
}