//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_table_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/extra_operator_info.hpp"

namespace duckdb {

//! Represents a scan of a base table
class PhysicalTableScan : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::TABLE_SCAN;

public:
	//! Table scan that immediately projects out filter columns that are unused in the remainder of the query plan
	PhysicalTableScan(vector<LogicalType> types, TableFunction function, idx_t table_index, unique_ptr<FunctionData> bind_data,
	                  vector<LogicalType> returned_types, vector<column_t> column_ids, vector<unique_ptr<Expression>> filter, vector<idx_t> projection_ids,
	                  vector<string> names, unique_ptr<TableFilterSet> table_filters, idx_t estimated_cardinality,
	                  ExtraOperatorInfo extra_info);

    //! The table id referenced in logical plan
    idx_t table_index;
	//! The table function
	TableFunction function;
	//! Bind data of the function
	unique_ptr<FunctionData> bind_data;
	//! The types of ALL columns that can be returned by the table function
	vector<LogicalType> returned_types;
	//! The column ids used within the table function
	vector<column_t> column_ids;
	//! The projected-out column ids
	vector<idx_t> projection_ids;
	//! The names of the columns
	vector<string> names;
	//! The table filters
	unique_ptr<TableFilterSet> table_filters;
	//! Currently stores any filters applied to file names (as strings)
	ExtraOperatorInfo extra_info;
    //! Expression
    unique_ptr<Expression> expression;

    mutex parallel_lock;

public:
	string GetName() const override;
	string ParamsToString() const override;

	bool Equals(const PhysicalOperator &other) const override;

public:
    bool PushdownZoneFilter(idx_t table_index, const shared_ptr<bitmask_vector> &zone_filter,
                            const shared_ptr<bitmask_vector> &zone_sel) override;
    unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;
	idx_t GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	                    LocalSourceState &lstate) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return false;
	}

	bool SupportsBatchIndex() const override {
		return function.get_batch_index != nullptr;
	}

	double GetProgress(ClientContext &context, GlobalSourceState &gstate) const override;

    string GetSubstraitInfo(unordered_map<ExpressionType, idx_t>& func_map, idx_t& func_num, idx_t depth = 0) const override;
    substrait::Rel* ToSubstraitClass(unordered_map<int, string>& tableid2name) const override;

public:
    //! The rows filter
    shared_ptr<rows_vector> rows_filter;
    shared_ptr<bitmask_vector> row_bitmask;
    shared_ptr<bitmask_vector> zone_bitmask;
    row_t rows_count;
};

} // namespace duckdb
