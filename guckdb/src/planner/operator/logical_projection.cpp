#include "duckdb/planner/operator/logical_projection.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

LogicalProjection::LogicalProjection(idx_t table_index, vector<unique_ptr<Expression>> select_list)
    : LogicalOperator(LogicalOperatorType::LOGICAL_PROJECTION, std::move(select_list)), table_index(table_index) {
}

vector<ColumnBinding> LogicalProjection::GetColumnBindings() {
	return GenerateColumnBindings(table_index, expressions.size());
}

ColumnBinding LogicalProjection::PushdownColumnBinding(ColumnBinding &binding) {
    auto child_binding = children[0]->PushdownColumnBinding(binding);
    if (child_binding.column_index != DConstants::INVALID_INDEX) {
        auto new_ref_expr = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, child_binding);
        expressions.push_back(move(new_ref_expr));
        return ColumnBinding(table_index, expressions.size() - 1);
    }
    return ColumnBinding(table_index, DConstants::INVALID_INDEX);
}

void LogicalProjection::ResolveTypes() {
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

vector<idx_t> LogicalProjection::GetTableIndex() const {
	return vector<idx_t> {};
    // return vector<idx_t> {table_index};
}

string LogicalProjection::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
