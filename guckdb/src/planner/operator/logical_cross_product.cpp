#include "duckdb/planner/operator/logical_cross_product.hpp"

namespace duckdb {

LogicalCrossProduct::LogicalCrossProduct(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right)
    : LogicalUnconditionalJoin(LogicalOperatorType::LOGICAL_CROSS_PRODUCT, std::move(left), std::move(right)) {
}

unique_ptr<LogicalOperator> LogicalCrossProduct::Create(unique_ptr<LogicalOperator> left,
                                                        unique_ptr<LogicalOperator> right) {
	if (left->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return right;
	}
	if (right->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return left;
	}
	return make_uniq<LogicalCrossProduct>(std::move(left), std::move(right));
}

ColumnBinding LogicalCrossProduct::PushdownColumnBinding(ColumnBinding &binding) {
    unordered_set<idx_t> left_tables, right_tables;
    auto left_bindings = children[0]->GetColumnBindings();
    for (auto &lb: left_bindings) {
        left_tables.insert(lb.table_index);
    }
    if (left_tables.find(binding.table_index) != left_tables.end()) {
        auto child_binding = children[0]->PushdownColumnBinding(binding);
        if (child_binding.column_index != DConstants::INVALID_INDEX) {
            return child_binding;
        }
    }
    auto right_bindings = children[1]->GetColumnBindings();
    for (auto &rb: right_bindings) {
        right_tables.insert(rb.table_index);
    }
    if (right_tables.find(binding.table_index) != right_tables.end()) {
        auto child_binding = children[1]->PushdownColumnBinding(binding);
        if (child_binding.column_index != DConstants::INVALID_INDEX) {
            return child_binding;
        }
    }

    return ColumnBinding(binding.table_index, DConstants::INVALID_INDEX);
}


} // namespace duckdb
