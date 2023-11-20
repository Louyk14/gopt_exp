//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_union.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalUnion : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::UNION;

public:
	PhysicalUnion(vector<LogicalType> types, unique_ptr<PhysicalOperator> top, unique_ptr<PhysicalOperator> bottom,
	              idx_t estimated_cardinality);

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	vector<const_reference<PhysicalOperator>> GetSources() const override;
    substrait::Rel* ToSubstraitClass(unordered_map<int, string>& tableid2name) const override;
};

} // namespace duckdb
