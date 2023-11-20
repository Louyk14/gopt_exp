#include "duckdb/execution/operator/filter/physical_filter.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

using namespace duckdb;
using namespace std;

class PhysicalFilterState : public PhysicalOperatorState {
public:
	PhysicalFilterState(PhysicalOperator *child, Expression &expr) : PhysicalOperatorState(child), executor(expr) {
	}

	ExpressionExecutor executor;
};

PhysicalFilter::PhysicalFilter(vector<TypeId> types, vector<unique_ptr<Expression>> select_list)
    : PhysicalOperator(PhysicalOperatorType::FILTER, types) {
	assert(select_list.size() > 0);
	if (select_list.size() > 1) {
		// create a big AND out of the expressions
		auto conjunction = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : select_list) {
			conjunction->children.push_back(move(expr));
		}
		expression = move(conjunction);
	} else {
		expression = move(select_list[0]);
	}
}

void PhysicalFilter::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
                                      SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	auto state = reinterpret_cast<PhysicalFilterState *>(state_);
	SelectionVector filter_sel(STANDARD_VECTOR_SIZE);
	idx_t initial_count;
	idx_t result_count;
	do {
		// fetch a chunk from the child and run the filter
		// we repeat this process until either (1) passing tuples are found, or (2) the child is completely exhausted
		children[0]->GetChunk(context, chunk, state->child_state.get(), sel, rid_vector, rai_chunk);
		if (chunk.size() == 0) {
			return;
		}
		initial_count = chunk.size();
		result_count = state->executor.SelectExpression(chunk, filter_sel);
	} while (result_count == 0 && rid_vector == nullptr);

	if (result_count == initial_count) {
		// nothing was filtered: skip adding any selection vectors
		return;
	}
	chunk.Slice(filter_sel, result_count);
	if (sel != nullptr) {
		auto sel_data = sel->Slice(filter_sel, result_count);
		sel->Initialize(move(sel_data));
	}
}

unique_ptr<PhysicalOperatorState> PhysicalFilter::GetOperatorState() {
	return make_unique<PhysicalFilterState>(children[0].get(), *expression);
}

string PhysicalFilter::ExtraRenderInformation() const {
	return expression->GetName();
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
        string* type_left_str = new string(TypeIdToString(lexp->return_type));
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

        string* type_str = new string(TypeIdToString(lexp->return_type));
        type->set_allocated_string(type_str);
        map_key_type->set_allocated_map_key(type);
        child_variable_type->set_allocated_map_key(map_key_type);

        if (lexp->return_type == TypeId::INT64) {
            string* value_str = new string(to_string(lexp->value.GetValue<int64_t>()));
            variable_value->set_allocated_string(value_str);
        }
        else if (lexp->return_type == TypeId::VARCHAR) {
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

void getFilterExpression(const unique_ptr<Expression>& expression, substrait::FilterRel *filter) {
    if (expression.get()->type == ExpressionType::OPERATOR_IS_NULL) {
        BoundOperatorExpression* expr = (BoundOperatorExpression*) expression.get();
        for (int i = 0; i < expr->children.size(); ++i) {
            substrait::Expression *expression_left = formulateExpression(expr->children[i].get());
            *filter->add_lcondition() = *expression_left;
            delete expression_left;

            substrait::Expression* expression_right = new substrait::Expression();
            *filter->add_rcondition() = *expression_right;
            delete expression_right;

            filter->add_filter_type(ExpressionTypeToString(expr->type));
        }
    }
    else if (expression.get()->type != ExpressionType::CONJUNCTION_AND) {
        BoundComparisonExpression *expr = (BoundComparisonExpression *) expression.get();
        substrait::Expression *expression_left = formulateExpression(expr->left.get());
        substrait::Expression *expression_right = formulateExpression(expr->right.get());

        *filter->add_lcondition() = *expression_left;
        *filter->add_rcondition() = *expression_right;
        filter->add_filter_type(ExpressionTypeToString(expr->type));
        delete expression_left;
        delete expression_right;
    }
    else if (expression.get()->type == ExpressionType::CONJUNCTION_AND) {
        BoundConjunctionExpression* expr = (BoundConjunctionExpression*) expression.get();
        for (int i = 0; i < expr->children.size(); ++i) {
            getFilterExpression(expr->children[i], filter);
            /*BoundComparisonExpression *subexpr = (BoundComparisonExpression *) expr->children[i].get();
            substrait::Expression *expression_left = formulateExpression(subexpr->left.get());
            substrait::Expression *expression_right = formulateExpression(subexpr->right.get());

            *filter->add_lcondition() = *expression_left;
            *filter->add_rcondition() = *expression_right;
            filter->add_filter_type(ExpressionTypeToString(expr->type));
            delete expression_left;
            delete expression_right;*/
        }
    }
}

substrait::Rel* PhysicalFilter::ToSubstraitClass(unordered_map<int, string>& tableid2name) const {
    substrait::Rel *filter_rel = new substrait::Rel();
    substrait::FilterRel *filter = new substrait::FilterRel();

    getFilterExpression(move(expression), filter);

    for (int i = 0; i < children.size(); ++i) {
        filter->set_allocated_input(children[i]->ToSubstraitClass(tableid2name));
    }

    filter->set_type(ExpressionTypeToString(expression->type));

    substrait::RelCommon *common = new substrait::RelCommon();
    substrait::RelCommon_Emit *emit = new substrait::RelCommon_Emit();

    for (int i = 0; i < types.size(); ++i) {
        emit->add_output_types(TypeIdToString(types[i]));
    }
    emit->add_output_names(PhysicalOperatorToString(type));
    emit->add_output_names(ExpressionTypeToString(expression.get()->type));
    common->set_allocated_emit(emit);

    filter->set_allocated_common(common);
    filter_rel->set_allocated_filter(filter);

    return filter_rel;
}