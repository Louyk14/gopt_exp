#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
namespace duckdb {

PhysicalFilter::PhysicalFilter(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality)
    : CachingPhysicalOperator(PhysicalOperatorType::FILTER, std::move(types), estimated_cardinality) {
	D_ASSERT(select_list.size() > 0);
	if (select_list.size() > 1) {
		// create a big AND out of the expressions
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : select_list) {
			conjunction->children.push_back(std::move(expr));
		}
		expression = std::move(conjunction);
	} else {
		expression = std::move(select_list[0]);
	}
}

class FilterState : public CachingOperatorState {
public:
	explicit FilterState(ExecutionContext &context, Expression &expr)
	    : executor(context.client, expr), sel(STANDARD_VECTOR_SIZE) {
	}

	ExpressionExecutor executor;
	SelectionVector sel;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, executor, "filter", 0);
	}
};

unique_ptr<OperatorState> PhysicalFilter::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<FilterState>(context, *expression);
}

OperatorResultType PhysicalFilter::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<FilterState>();
	idx_t result_count = state.executor.SelectExpression(input, state.sel);
	if (result_count == input.size()) {
		// nothing was filtered: skip adding any selection vectors
		chunk.Reference(input);
	} else {
		chunk.Slice(input, state.sel, result_count);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalFilter::ParamsToString() const {
	auto result = expression->GetName();
	result += "\n[INFOSEPARATOR]\n";
	result += StringUtil::Format("EC: %llu", estimated_cardinality);
	return result;
}

substrait::Expression* formulateExpression(Expression* expr) {
    substrait::Expression* result_expression = new substrait::Expression();
    if (expr->type == ExpressionType::BOUND_REF) {
        BoundReferenceExpression* lexp = (BoundReferenceExpression*) expr;

        substrait::Expression_FieldReference* field_reference = new substrait::Expression_FieldReference();
        substrait::Expression_ReferenceSegment* direct_reference = new substrait::Expression_ReferenceSegment();
        substrait::Expression_ReferenceSegment_MapKey* map_key_variable = new substrait::Expression_ReferenceSegment_MapKey();
        substrait::Expression_Literal* variable_name = new substrait::Expression_Literal();
        substrait::Expression_ReferenceSegment* child_variable_type = new substrait::Expression_ReferenceSegment();
        substrait::Expression_ReferenceSegment_MapKey* map_key_type = new substrait::Expression_ReferenceSegment_MapKey();
        substrait::Expression_Literal* type_left = new substrait::Expression_Literal();
        substrait::Expression_ReferenceSegment* child_variable_index = new substrait::Expression_ReferenceSegment();
        substrait::Expression_ReferenceSegment_StructField* field_variable_index = new substrait::Expression_ReferenceSegment_StructField();

        field_variable_index->set_field(lexp->index);
        child_variable_index->set_allocated_struct_field(field_variable_index);
        string* type_left_str = new string(TypeIdToString(lexp->return_type.InternalType()));
        type_left->set_allocated_string(type_left_str);
        map_key_type->set_allocated_map_key(type_left);
        map_key_type->set_allocated_child(child_variable_index);
        child_variable_type->set_allocated_map_key(map_key_type);
        string* alias_str = new string(lexp->alias);
        variable_name->set_allocated_string(alias_str);
        map_key_variable->set_allocated_map_key(variable_name);
        map_key_variable->set_allocated_child(child_variable_type);
        direct_reference->set_allocated_map_key(map_key_variable);
        field_reference->set_allocated_direct_reference(direct_reference);
        result_expression->set_allocated_selection(field_reference);
        return result_expression;
        // return field_reference;
    }
    else if (expr->type == ExpressionType::VALUE_CONSTANT) {
        BoundConstantExpression* lexp = (BoundConstantExpression*) expr;

        substrait::Expression_FieldReference* field_reference = new substrait::Expression_FieldReference();
        substrait::Expression_ReferenceSegment* direct_reference = new substrait::Expression_ReferenceSegment();
        substrait::Expression_ReferenceSegment_MapKey* map_key_variable = new substrait::Expression_ReferenceSegment_MapKey();
        substrait::Expression_Literal* variable_value = new substrait::Expression_Literal();
        substrait::Expression_ReferenceSegment* child_variable_type = new substrait::Expression_ReferenceSegment();
        substrait::Expression_ReferenceSegment_MapKey* map_key_type = new substrait::Expression_ReferenceSegment_MapKey();
        substrait::Expression_Literal* type = new substrait::Expression_Literal();

        string* type_str = new string(TypeIdToString(lexp->return_type.InternalType()));
        type->set_allocated_string(type_str);
        map_key_type->set_allocated_map_key(type);
        child_variable_type->set_allocated_map_key(map_key_type);

        if (lexp->return_type == LogicalTypeId::BIGINT) {
            string* value_str = new string(to_string(lexp->value.GetValue<int64_t>()));
            variable_value->set_allocated_string(value_str);
        }
        else if (lexp->return_type == LogicalTypeId::VARCHAR) {
            string* value_str = new string(lexp->value.GetValue<string>());
            variable_value->set_allocated_string(value_str);
        }

        map_key_variable->set_allocated_map_key(variable_value);
        map_key_variable->set_allocated_child(child_variable_type);
        direct_reference->set_allocated_map_key(map_key_variable);
        field_reference->set_allocated_direct_reference(direct_reference);
        result_expression->set_allocated_selection(field_reference);
        return result_expression;
    }
}

substrait::Rel* PhysicalFilter::ToSubstraitClass(unordered_map<int, string>& tableid2name) const {
    substrait::Rel *filter_rel = new substrait::Rel();
    substrait::FilterRel *filter = new substrait::FilterRel();

    if (expression.get()->type != ExpressionType::CONJUNCTION_AND) {
        BoundComparisonExpression *expr = (BoundComparisonExpression *) this->expression.get();
        substrait::Expression *expression_left = formulateExpression(expr->left.get());
        substrait::Expression *expression_right = formulateExpression(expr->right.get());

        *filter->add_lcondition() = *expression_left;
        *filter->add_rcondition() = *expression_right;
        delete expression_left;
        delete expression_right;
    }
    else if (expression.get()->type == ExpressionType::CONJUNCTION_AND) {
        BoundConjunctionExpression* expr = (BoundConjunctionExpression*) this->expression.get();
        for (int i = 0; i < expr->children.size(); ++i) {
            BoundComparisonExpression *subexpr = (BoundComparisonExpression *) expr->children[i].get();
            substrait::Expression *expression_left = formulateExpression(subexpr->left.get());
            substrait::Expression *expression_right = formulateExpression(subexpr->right.get());

            *filter->add_lcondition() = *expression_left;
            *filter->add_rcondition() = *expression_right;
            delete expression_left;
            delete expression_right;
        }
    }

    for (int i = 0; i < children.size(); ++i) {
        filter->set_allocated_input(children[i]->ToSubstraitClass(tableid2name));
    }

    filter->set_type(ExpressionTypeToString(expression->type));

    substrait::RelCommon *common = new substrait::RelCommon();
    substrait::RelCommon_Emit *emit = new substrait::RelCommon_Emit();

    for (int i = 0; i < types.size(); ++i) {
        emit->add_output_types(TypeIdToString(types[i].InternalType()));
    }
    emit->add_output_names(PhysicalOperatorToString(type));
    emit->add_output_names(ExpressionTypeToString(expression.get()->type));
    common->set_allocated_emit(emit);

    filter->set_allocated_common(common);
    filter_rel->set_allocated_filter(filter);

    return filter_rel;
}


} // namespace duckdb
