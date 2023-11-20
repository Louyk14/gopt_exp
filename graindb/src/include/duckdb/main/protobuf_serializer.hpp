//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/protobuf_serializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include <vector>
#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/execution/operator/list.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/execution/operator/join/physical_sip_join.hpp"
#include "duckdb/execution/operator/join/physical_merge_sip_join.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

namespace duckdb {

class PbSerializer {
public:
	PbSerializer(){};

	void Serialize(string* info, PhysicalOperator* physical_operator, unordered_map<int, string>& tableid2name);
	void SerializeToFile(string& file_name, PhysicalOperator* physical_operator, unordered_map<int, string>& tableid2name);
	unique_ptr<duckdb::PhysicalOperator> DeSerialize(ClientContext& context, string* info);
	unique_ptr<duckdb::PhysicalOperator> DeSerializeFromFile(ClientContext& context, string& file_name);

	unique_ptr<duckdb::PhysicalOperator> DispatchTask(ClientContext& context, const substrait::Rel& cur, unordered_map<string, TableCatalogEntry*>& table_entry,
	                                      unordered_map<string, int>& table_index, int& index);


	// tools for generator

	void add_table_entry(ClientContext& context, string& table_name, unordered_map<string, TableCatalogEntry*>& table_entry,
	                     unordered_map<string, int>& table_index, int& index);

	unique_ptr<PhysicalTableScan> get_scan_function(ClientContext& context, string& table_name, idx_t table_index,
	                                                vector<idx_t>& columns_ids, vector<TypeId>& getTypes,
	                                                unordered_map<idx_t, vector<TableFilter>> table_filters = unordered_map<idx_t, vector<TableFilter>>());


	unique_ptr<PhysicalProjection> GeneratePhysicalProjection(ClientContext& context, const substrait::ProjectRel& project_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
	                                                          unordered_map<std::string, int> &table_index, int& index);
    unique_ptr<PhysicalFilter> GeneratePhysicalFilter(ClientContext& context, const substrait::FilterRel& filter_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                              unordered_map<std::string, int> &table_index, int& index);
    unique_ptr<PhysicalTableScan> GeneratePhysicalTableScan(ClientContext& context, const substrait::ReadRel& read_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
	                                                        unordered_map<std::string, int> &table_index, int& index);
    unique_ptr<PhysicalChunkScan> GeneratePhysicalChunkScan(ClientContext& context, const substrait::ReadRel& read_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                            unordered_map<std::string, int> &table_index, int& index);
	unique_ptr<PhysicalSIPJoin> GeneratePhysicalSIPJoin(ClientContext& context, const substrait::SIPJoinRel& sip_join_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
	                                                    unordered_map<std::string, int> &table_index, int& index);
	unique_ptr<PhysicalMergeSIPJoin> GeneratePhysicalMergeSIPJoin(ClientContext& context, const substrait::MergeSIPJoinRel& merge_sip_join_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
	                                                              unordered_map<std::string, int> &table_index, int& index);
	unique_ptr<PhysicalHashJoin> GeneratePhysicalHashJoin(ClientContext& context, const substrait::HashJoinRel& hash_join_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
	                                                      unordered_map<std::string, int> &table_index, int& index);
    unique_ptr<PhysicalDelimJoin> GeneratePhysicalDelimJoin(ClientContext& context, const substrait::DelimJoinRel& delim_join_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                          unordered_map<std::string, int> &table_index, int& index);
    unique_ptr<PhysicalTopN> GeneratePhysicalTopN(ClientContext& context, const substrait::FetchRel& topn_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                            unordered_map<std::string, int> &table_index, int& index);
    unique_ptr<PhysicalHashAggregate> GeneratePhysicalHashAggregate(ClientContext& context, const substrait::AggregateRel& aggregate_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                  unordered_map<std::string, int> &table_index, int& index);
    unique_ptr<PhysicalUnion> GeneratePhysicalUnion(ClientContext& context, const substrait::SetRel& union_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                                    unordered_map<std::string, int> &table_index, int& index);
    unique_ptr<PhysicalOrder> GeneratePhysicalOrder(ClientContext& context, const substrait::FetchRel& order_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                    unordered_map<std::string, int> &table_index, int& index);
    unique_ptr<PhysicalRecursiveCTE> GeneratePhysicalRecursiveCTE(ClientContext& context, const substrait::RecursiveCTERel& recur_cte_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                    unordered_map<std::string, int> &table_index, int& index);
};
}
