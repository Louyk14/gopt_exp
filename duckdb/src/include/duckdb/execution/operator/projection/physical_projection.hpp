//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_projection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class PhysicalProjection : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::PROJECTION;

public:
	PhysicalProjection(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
	                   idx_t estimated_cardinality);

	vector<unique_ptr<Expression>> select_list;

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

	string ParamsToString() const override;

	static unique_ptr<PhysicalOperator>
	CreateJoinProjection(vector<LogicalType> proj_types, const vector<LogicalType> &lhs_types,
	                     const vector<LogicalType> &rhs_types, const vector<idx_t> &left_projection_map,
	                     const vector<idx_t> &right_projection_map, const idx_t estimated_cardinality);
    string GetSubstraitInfo(unordered_map<ExpressionType, idx_t>& func_map,idx_t& func_num, idx_t depth = 0) const override;
    substrait::Rel* ToSubstraitClass(unordered_map<int, string>& tableid2name) const override;

};

} // namespace duckdb
