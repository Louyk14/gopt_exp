//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_projection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalProjection : public PhysicalOperator {
public:
	PhysicalProjection(vector<TypeId> &types, vector<unique_ptr<Expression>> select_list)
	    : PhysicalOperator(PhysicalOperatorType::PROJECTION, types), select_list(move(select_list)) {
	}
	PhysicalProjection(LogicalOperator &op, vector<unique_ptr<Expression>> select_list)
	    : PhysicalProjection(op.types, move(select_list)) {
	}

	vector<unique_ptr<Expression>> select_list;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
	                      SelectionVector *sel = nullptr, Vector *rid_vector = nullptr,
	                      DataChunk *rai_chunk = nullptr) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	string ExtraRenderInformation() const override;
	string GetSubstraitInfo(unordered_map<ExpressionType, idx_t>& func_map,idx_t& func_num, idx_t depth = 0) const override;
	substrait::Rel* ToSubstraitClass(unordered_map<int, string>& tableid2name) const override;
};

} // namespace duckdb
