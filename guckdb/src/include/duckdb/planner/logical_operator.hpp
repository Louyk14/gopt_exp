//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/logical_operator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/optimizer/join_order/estimated_properties.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

#include <algorithm>
#include <functional>

namespace duckdb {

// nested loop join, hash join, adjacency join, default join, scan, lookup
enum class OpMark : int8_t {
    NO_OP = 0,
    NLJ = 1,
    HASH_JOIN = 2,
    SIP_JOIN = 3,
    JOIN = 4,
    MERGED_SIP_JOIN = 5,
    SCAN = 6,
    UNION = 7,
    ADAPTIVE_SIP_JOIN = 8,
    ADAPTIVE_MERGE_SIP_JOIN = 9
};

//! LogicalOperator is the base class of the logical operators present in the
//! logical query tree
class LogicalOperator {
public:
	explicit LogicalOperator(LogicalOperatorType type);
	LogicalOperator(LogicalOperatorType type, vector<unique_ptr<Expression>> expressions);
	virtual ~LogicalOperator();

	//! The type of the logical operator
	LogicalOperatorType type;
	//! The set of children of the operator
	vector<unique_ptr<LogicalOperator>> children;
	//! The set of expressions contained within the operator, if any
	vector<unique_ptr<Expression>> expressions;
	//! The types returned by this logical operator. Set by calling LogicalOperator::ResolveTypes.
	vector<LogicalType> types;
	//! Estimated Cardinality
	idx_t estimated_cardinality;
	bool has_estimated_cardinality;

    OpMark op_mark;

public:
	virtual vector<ColumnBinding> GetColumnBindings();
	static vector<ColumnBinding> GenerateColumnBindings(idx_t table_idx, idx_t column_count);
	static vector<LogicalType> MapTypes(const vector<LogicalType> &types, const vector<idx_t> &projection_map);
	static vector<ColumnBinding> MapBindings(const vector<ColumnBinding> &types, const vector<idx_t> &projection_map);

	//! Resolve the types of the logical operator and its children
	void ResolveOperatorTypes();

	virtual string GetName() const;
	virtual string ParamsToString() const;
	virtual string ToString() const;
	DUCKDB_API void Print();
	//! Debug method: verify that the integrity of expressions & child nodes are maintained
	virtual void Verify(ClientContext &context);

	void AddChild(unique_ptr<LogicalOperator> child);
	virtual idx_t EstimateCardinality(ClientContext &context);

    virtual ColumnBinding PushdownColumnBinding(ColumnBinding &binding) {
        return children[0]->PushdownColumnBinding(binding);
    }

	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	virtual unique_ptr<LogicalOperator> Copy(ClientContext &context) const;

	virtual bool RequireOptimizer() const {
		return true;
	}

	//! Allows LogicalOperators to opt out of serialization
	virtual bool SupportSerialization() const {
		return true;
	};

	//! Returns the set of table indexes of this operator
	virtual vector<idx_t> GetTableIndex() const;

protected:
	//! Resolve types for this specific operator
	virtual void ResolveTypes() = 0;

public:
	template <class TARGET>
	TARGET &Cast() {
		if (TARGET::TYPE != LogicalOperatorType::LOGICAL_INVALID && type != TARGET::TYPE) {
			throw InternalException("Failed to cast logical operator to type - logical operator type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (TARGET::TYPE != LogicalOperatorType::LOGICAL_INVALID && type != TARGET::TYPE) {
			throw InternalException("Failed to cast logical operator to type - logical operator type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};
} // namespace duckdb
