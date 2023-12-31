#include "duckdb/execution/operator/filter/physical_filter.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"

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

        substrait::Expression_Literal* literal = new substrait::Expression_Literal();
        result_expression->set_allocated_literal(literal);
        substrait::Expression_Literal_List* list = new substrait::Expression_Literal_List();
        literal->set_allocated_list(list);

        substrait::Expression_Literal* expr_type = new substrait::Expression_Literal();
        expr_type->set_i64(static_cast<int>(expr->type));
        *list->add_values() = *expr_type;
        delete expr_type;

        substrait::Expression_Literal* alias = new substrait::Expression_Literal();
        string* alias_str = new string(lexp->alias);
        alias->set_allocated_string(alias_str);
        *list->add_values() = *alias;
        delete alias;

        substrait::Expression_Literal* return_type = new substrait::Expression_Literal();
        string* type_left_str = new string(TypeIdToString(lexp->return_type));
        return_type->set_allocated_string(type_left_str);
        *list->add_values() = *return_type;
        delete return_type;

        substrait::Expression_Literal* index = new substrait::Expression_Literal();
        index->set_i64(lexp->index);
        *list->add_values() = *index;
        delete index;

        return result_expression;
        // return field_reference;
    }
    else if (expr->type == ExpressionType::VALUE_CONSTANT) {
        BoundConstantExpression* lexp = (BoundConstantExpression*) expr;

        substrait::Expression_Literal* literal = new substrait::Expression_Literal();
        result_expression->set_allocated_literal(literal);
        substrait::Expression_Literal_List* list = new substrait::Expression_Literal_List();
        literal->set_allocated_list(list);

        substrait::Expression_Literal* expr_type = new substrait::Expression_Literal();
        expr_type->set_i64(static_cast<int>(expr->type));
        *list->add_values() = *expr_type;
        delete expr_type;

        substrait::Expression_Literal* return_type = new substrait::Expression_Literal();
        string* type_left_str = new string(TypeIdToString(lexp->return_type));
        return_type->set_allocated_string(type_left_str);
        *list->add_values() = *return_type;
        delete return_type;

        substrait::Expression_Literal* value = new substrait::Expression_Literal();
        if (lexp->return_type == TypeId::INT64) {
            string* value_str = new string(to_string(lexp->value.GetValue<int64_t>()));
            value->set_allocated_string(value_str);
        }
        else if (lexp->return_type == TypeId::INT32) {
            string* value_str = new string(to_string(lexp->value.GetValue<int32_t>()));
            value->set_allocated_string(value_str);
        }
        else if (lexp->return_type == TypeId::INT16) {
            string* value_str = new string(to_string(lexp->value.GetValue<int16_t>()));
            value->set_allocated_string(value_str);
        }
        else if (lexp->return_type == TypeId::VARCHAR) {
            string* value_str = new string(lexp->value.GetValue<string>());
            value->set_allocated_string(value_str);
        }
        else {
            std::cout << "value constant not support" << std::endl;
        }
        *list->add_values() = *value;
        delete value;

        return result_expression;
    }
    else if (expr->type == ExpressionType::CONJUNCTION_OR) {
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
    else if (expression.get()->type == ExpressionType::COMPARE_BETWEEN) {
        BoundBetweenExpression* expr = (BoundBetweenExpression*) expression.get();
        substrait::Expression *expression_input = formulateExpression(expr->input.get());
        substrait::Expression *expression_lower = formulateExpression(expr->lower.get());
        substrait::Expression *expression_upper = formulateExpression(expr->upper.get());
        substrait::Expression *slot = new substrait::Expression();

        substrait::Expression_Literal* literal = new substrait::Expression_Literal();
        slot->set_allocated_literal(literal);
        substrait::Expression_Literal_List* list = new substrait::Expression_Literal_List();
        literal->set_allocated_list(list);

        substrait::Expression_Literal* lower_incl = new substrait::Expression_Literal();
        lower_incl->set_boolean(expr->lower_inclusive);
        *list->add_values() = *lower_incl;
        delete lower_incl;

        substrait::Expression_Literal* upper_incl = new substrait::Expression_Literal();
        upper_incl->set_boolean(expr->upper_inclusive);
        *list->add_values() = *upper_incl;
        delete upper_incl;

        *filter->add_lcondition() = *expression_lower;
        *filter->add_lcondition() = *expression_input;
        *filter->add_rcondition() = *expression_upper;
        *filter->add_rcondition() = *slot;

        filter->add_filter_type(ExpressionTypeToString(expr->type));
        filter->add_filter_type(ExpressionTypeToString(expr->type));

        delete expression_input;
        delete expression_lower;
        delete expression_upper;
        delete slot;
    }
    else if (expression.get()->type == ExpressionType::CONJUNCTION_OR) {
        BoundConjunctionExpression* expr = (BoundConjunctionExpression*) expression.get();

        for (int i = 0; i < expr->children.size(); ++i) {
            getFilterExpression(expr->children[i], filter);
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