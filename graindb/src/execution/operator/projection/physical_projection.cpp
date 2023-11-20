#include "duckdb/execution/operator/projection/physical_projection.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

using namespace duckdb;
using namespace std;

class PhysicalProjectionState : public PhysicalOperatorState {
public:
	PhysicalProjectionState(PhysicalOperator *child, vector<unique_ptr<Expression>> &expressions)
	    : PhysicalOperatorState(child), executor(expressions) {
		assert(child);
	}

	ExpressionExecutor executor;
};

void PhysicalProjection::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
                                          SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	auto state = reinterpret_cast<PhysicalProjectionState *>(state_);

	// get the next chunk from the child
	children[0]->GetChunk(context, state->child_chunk, state->child_state.get(), sel, rid_vector, rai_chunk);
	if (state->child_chunk.size() == 0) {
		return;
	}

	state->executor.Execute(state->child_chunk, chunk);
}

unique_ptr<PhysicalOperatorState> PhysicalProjection::GetOperatorState() {
	return make_unique<PhysicalProjectionState>(children[0].get(), select_list);
}

string PhysicalProjection::ExtraRenderInformation() const {
	string extra_info;
	for (auto &expr : select_list) {
		extra_info += expr->GetName() + "\n";
	}
	return extra_info;
}

string PhysicalProjection::GetSubstraitInfo(unordered_map<ExpressionType, idx_t>& func_map,idx_t& func_num, duckdb::idx_t depth) const {
	string project_str = AssignBlank(depth) + "\"project\": {\n";

	string common_str = AssignBlank(++depth) + "\"common\": {\n";
	string emit_str = AssignBlank(++depth) + "\"emit\": {\n";
	string output_mapping_str = AssignBlank(++depth) + "\"outputMapping\": [\n";
	string output_mapping_names = AssignBlank(depth) + "\"outputNames\": [\n";
	string output_mapping_types = AssignBlank(depth) + "\"outputTypes\": [\n";

	++depth;
	string select_list_str = "";
	string select_list_names = "";
	string select_list_types = "";

	for (int i = 0; i < select_list.size(); ++i) {
		BoundReferenceExpression* bexp = (BoundReferenceExpression*) select_list[i].get();
		select_list_str += AssignBlank(depth) + to_string(bexp->index);
		select_list_names += AssignBlank(depth) + "\"" + bexp->alias + "\"";
		select_list_types += AssignBlank(depth) + "\"" + TypeIdToString(bexp->return_type) + "\"";
		if (i != select_list.size() - 1) {
			select_list_str += ",\n";
			select_list_names += ",\n";
			select_list_types += ",\n";
		}
		else {
			select_list_str += "\n";
			select_list_names += "\n";
			select_list_types += "\n";
		}
	}
	select_list_str += AssignBlank(--depth) + "]";
	select_list_names += AssignBlank(depth) + "]";
	select_list_types += AssignBlank(depth) + "]";
	output_mapping_str += select_list_str + ",\n";
	output_mapping_names += select_list_names + ",\n";
	output_mapping_types += select_list_types + "\n";
	emit_str += output_mapping_str + output_mapping_names + output_mapping_types + AssignBlank(--depth) + "}\n";
	common_str += emit_str + AssignBlank(--depth) + "},\n";

	string input_str = AssignBlank(depth) + "\"input\": {\n";
	for (int i = 0; i < children.size(); ++i) {
		input_str += children[i]->GetSubstraitInfo(func_map, func_num, depth + 1);
	}
	input_str += AssignBlank(depth) + "}\n";

	project_str += common_str + input_str + AssignBlank(--depth) + "}\n";

	return project_str;
}

substrait::Expression_ReferenceSegment_MapKey* formulateMapKey(string name) {
    substrait::Expression_ReferenceSegment_MapKey* op_type = new substrait::Expression_ReferenceSegment_MapKey();
    substrait::Expression_Literal* op_type_literal = new substrait::Expression_Literal();
    op_type->set_allocated_map_key(op_type_literal);
    string* op_type_name = new string(name);
    op_type_literal->set_allocated_string(op_type_name);

    return op_type;
}

substrait::Expression_ReferenceSegment_StructField* formulateStructField(int index) {
    substrait::Expression_ReferenceSegment_StructField* op_type = new substrait::Expression_ReferenceSegment_StructField();
    op_type->set_field(index);

    return op_type;
}

substrait::Expression_Nested_Map_KeyValue* getNestedHead(duckdb::Expression* duck_expr) {
    substrait::Expression_Nested_Map_KeyValue *op_key_value = new substrait::Expression_Nested_Map_KeyValue();
    if (duck_expr->type == ExpressionType::BOUND_REF) {
        BoundReferenceExpression *expr = (BoundReferenceExpression *) duck_expr;

        substrait::Expression* op_check_key = new substrait::Expression();
        op_key_value->set_allocated_key(op_check_key);
        substrait::Expression_FieldReference *op_field = new substrait::Expression_FieldReference();
        op_check_key->set_allocated_selection(op_field);

        substrait::Expression_ReferenceSegment* op_direct = new substrait::Expression_ReferenceSegment();
        substrait::Expression_ReferenceSegment_MapKey *op_name_mapkey = formulateMapKey(expr->alias);
        op_direct->set_allocated_map_key(op_name_mapkey);
        op_field->set_allocated_direct_reference(op_direct);

        substrait::Expression_ReferenceSegment *op_type_seg = new substrait::Expression_ReferenceSegment();
        substrait::Expression_ReferenceSegment_MapKey *op_type_mapkey = formulateMapKey(ExpressionTypeToString(expr->type));
        op_type_seg->set_allocated_map_key(op_type_mapkey);
        op_name_mapkey->set_allocated_child(op_type_seg);

        substrait::Expression_ReferenceSegment *op_return_seg = new substrait::Expression_ReferenceSegment();
        substrait::Expression_ReferenceSegment_MapKey *op_return_mapkey = formulateMapKey(TypeIdToString(expr->return_type));
        op_return_seg->set_allocated_map_key(op_return_mapkey);
        op_type_mapkey->set_allocated_child(op_return_seg);

        substrait::Expression_ReferenceSegment* op_index_seg = new substrait::Expression_ReferenceSegment();
        substrait::Expression_ReferenceSegment_StructField* op_index_struct = formulateStructField(expr->index);
        op_index_seg->set_allocated_struct_field(op_index_struct);
        op_return_mapkey->set_allocated_child(op_index_seg);
    }
    else if (duck_expr->type == ExpressionType::OPERATOR_IS_NOT_NULL) {
        BoundOperatorExpression *expr = (BoundOperatorExpression *) duck_expr;

        substrait::Expression *op_check_key = new substrait::Expression();
        op_key_value->set_allocated_key(op_check_key);
        substrait::Expression_FieldReference *op_field = new substrait::Expression_FieldReference();
        op_check_key->set_allocated_selection(op_field);

        substrait::Expression_ReferenceSegment *op_direct = new substrait::Expression_ReferenceSegment();
        substrait::Expression_ReferenceSegment_MapKey *op_name_mapkey = formulateMapKey(expr->alias);
        op_direct->set_allocated_map_key(op_name_mapkey);
        op_field->set_allocated_direct_reference(op_direct);

        substrait::Expression_ReferenceSegment *op_type_seg = new substrait::Expression_ReferenceSegment();
        substrait::Expression_ReferenceSegment_MapKey *op_type_mapkey = formulateMapKey(ExpressionTypeToString(expr->type));
        op_type_seg->set_allocated_map_key(op_type_mapkey);
        op_name_mapkey->set_allocated_child(op_type_seg);

        substrait::Expression_ReferenceSegment *op_return_seg = new substrait::Expression_ReferenceSegment();
        substrait::Expression_ReferenceSegment_MapKey *op_return_mapkey = formulateMapKey(TypeIdToString(expr->return_type));
        op_return_seg->set_allocated_map_key(op_return_mapkey);
        op_type_mapkey->set_allocated_child(op_return_seg);

        substrait::Expression *op_check_value = new substrait::Expression();
        op_key_value->set_allocated_value(op_check_value);
        substrait::Expression_FieldReference *child_field = new substrait::Expression_FieldReference();
        op_check_value->set_allocated_selection(child_field);

        substrait::Expression_ReferenceSegment_StructField *previous = NULL;
        for (int i = 0; i < expr->children.size(); ++i) {
            if (expr->children[i]->type == ExpressionType::BOUND_REF) {
                BoundReferenceExpression *bexp = (BoundReferenceExpression *) expr->children[i].get();

                substrait::Expression_ReferenceSegment *child_exp_seg = new substrait::Expression_ReferenceSegment();
                substrait::Expression_ReferenceSegment_MapKey *child_exp_mapkey = formulateMapKey(
                        ExpressionTypeToString(bexp->type));
                child_exp_seg->set_allocated_map_key(child_exp_mapkey);
                if (previous != NULL)
                    previous->set_allocated_child(child_exp_seg);
                else
                    child_field->set_allocated_direct_reference(child_exp_seg);

                substrait::Expression_ReferenceSegment *child_name_seg = new substrait::Expression_ReferenceSegment();
                substrait::Expression_ReferenceSegment_MapKey *child_name_mapkey = formulateMapKey(bexp->alias);
                child_name_seg->set_allocated_map_key(child_name_mapkey);
                child_exp_mapkey->set_allocated_child(child_name_seg);

                substrait::Expression_ReferenceSegment *child_return_seg = new substrait::Expression_ReferenceSegment();
                substrait::Expression_ReferenceSegment_MapKey *child_return_mapkey = formulateMapKey(
                        TypeIdToString(bexp->return_type));
                child_return_seg->set_allocated_map_key(child_return_mapkey);
                child_name_mapkey->set_allocated_child(child_return_seg);

                substrait::Expression_ReferenceSegment *child_index_seg = new substrait::Expression_ReferenceSegment();
                substrait::Expression_ReferenceSegment_StructField *child_index_field = formulateStructField(
                        bexp->index);
                child_index_seg->set_allocated_struct_field(child_index_field);
                child_return_mapkey->set_allocated_child(child_index_seg);

                previous = child_index_field;
            }
        }
    }

    return op_key_value;
}

substrait::Expression* getExpressionFromBound(duckdb::Expression* duck_expr);

substrait::Expression_Nested_Map_KeyValue* getNestedBody(duckdb::Expression* duck_expr) {
    BoundCaseExpression* expr = (BoundCaseExpression*) duck_expr;

    substrait::Expression* expr_true = getExpressionFromBound(expr->result_if_true.get());
    substrait::Expression* expr_false = getExpressionFromBound(expr->result_if_false.get());

    substrait::Expression_Nested_Map_KeyValue* key_value = new substrait::Expression_Nested_Map_KeyValue();
    key_value->set_allocated_key(expr_true);
    key_value->set_allocated_value(expr_false);

    return key_value;
}

substrait::Expression_Nested* getNestedSelectList(duckdb::Expression* duck_expr) {
    BoundCaseExpression* expr_case = (BoundCaseExpression*) duck_expr;

    BoundOperatorExpression* expr_check = (BoundOperatorExpression*) expr_case->check.get();
    substrait::Expression_Nested* sub_expr_nested = new substrait::Expression_Nested();
    substrait::Expression_Nested_Map* sub_expr_nested_map = new substrait::Expression_Nested_Map();
    sub_expr_nested->set_allocated_map(sub_expr_nested_map);

    substrait::Expression_Nested_Map_KeyValue* op_key_value_head = getNestedHead(expr_check);
    *sub_expr_nested_map->add_key_values() = *op_key_value_head;
    delete op_key_value_head;

    substrait::Expression_Nested_Map_KeyValue* op_key_value_body = getNestedBody(expr_case);
    *sub_expr_nested_map->add_key_values() = *op_key_value_body;
    delete op_key_value_body;

    return sub_expr_nested;
}

substrait::Expression* getExpressionFromBound(duckdb::Expression* duck_expr) {
    if (duck_expr->type == ExpressionType::BOUND_REF) {
        substrait::Expression* result_expr = new substrait::Expression();
        substrait::Expression_FieldReference* result_field = new substrait::Expression_FieldReference();
        result_expr->set_allocated_selection(result_field);

        BoundReferenceExpression* bexp = (BoundReferenceExpression*) duck_expr;

        substrait::Expression_ReferenceSegment *child_name_seg = new substrait::Expression_ReferenceSegment();
        substrait::Expression_ReferenceSegment_MapKey *child_name_mapkey = formulateMapKey(bexp->alias);
        child_name_seg->set_allocated_map_key(child_name_mapkey);
        result_field->set_allocated_direct_reference(child_name_seg);

        substrait::Expression_ReferenceSegment *child_return_seg = new substrait::Expression_ReferenceSegment();
        substrait::Expression_ReferenceSegment_MapKey *child_return_mapkey = formulateMapKey(TypeIdToString(bexp->return_type));
        child_return_seg->set_allocated_map_key(child_return_mapkey);
        child_name_mapkey->set_allocated_child(child_return_seg);

        substrait::Expression_ReferenceSegment *child_index_seg = new substrait::Expression_ReferenceSegment();
        substrait::Expression_ReferenceSegment_StructField *child_index_field = formulateStructField(bexp->index);
        child_index_seg->set_allocated_struct_field(child_index_field);
        child_return_mapkey->set_allocated_child(child_index_seg);

        return result_expr;
    }
    else if (duck_expr->type == ExpressionType::CASE_EXPR) {
        substrait::Expression* expression = new substrait::Expression();
        expression->set_allocated_nested(getNestedSelectList(duck_expr));
        return expression;
    }
    else if (duck_expr->type == ExpressionType::VALUE_CONSTANT) {
        BoundConstantExpression* bound_expr = (BoundConstantExpression*) duck_expr;

        substrait::Expression* expression = new substrait::Expression();
        substrait::Expression_Literal* literal = new substrait::Expression_Literal();
        expression->set_allocated_literal(literal);

        literal->set_type_variation_reference(static_cast<int>(bound_expr->value.type));
        if (bound_expr->value.type == TypeId::INT64)
            literal->set_i64(bound_expr->value.value_.bigint);
        else if (bound_expr->value.type == TypeId::INT32)
            literal->set_i32(bound_expr->value.value_.integer);
        else if (bound_expr->value.type == TypeId::INT16)
            literal->set_i16(bound_expr->value.value_.smallint);
        else if (bound_expr->value.type == TypeId::BOOL)
            literal->set_boolean(bound_expr->value.value_.boolean);
        else if (bound_expr->value.type == TypeId::VARCHAR)
            literal->set_string(bound_expr->value.str_value);
        else
            std::cout << "constant value unsupported" << std::endl;

        return expression;
    }
}


substrait::Rel* PhysicalProjection::ToSubstraitClass(unordered_map<int, string>& tableid2name) const {
	substrait::Rel* project_rel = new substrait::Rel();
	substrait::ProjectRel* project = new substrait::ProjectRel();
	substrait::RelCommon* common = new substrait::RelCommon();
	substrait::RelCommon_Emit* emit = new substrait::RelCommon_Emit();

	for (int i = 0; i < select_list.size(); ++i) {
        if (select_list[i]->type == ExpressionType::BOUND_REF) {
            BoundReferenceExpression* bexp = (BoundReferenceExpression*) select_list[i].get();
            emit->add_output_mapping(bexp->index);
            emit->add_output_names(bexp->alias);
            emit->add_output_types(TypeIdToString(bexp->return_type));
            // emit->add_output_exp_type(ExpressionTypeToString(select_list[i]->type));
            emit->add_output_exp_type("");
        }
        else if (select_list[i]->type == ExpressionType::VALUE_CONSTANT) {
            BoundConstantExpression* bexp = (BoundConstantExpression*) select_list[i].get();
            emit->add_output_mapping(-1);
            emit->add_output_names(bexp->alias);
            emit->add_output_types(TypeIdToString(bexp->return_type));
            // emit->add_output_exp_type(ExpressionTypeToString(select_list[i]->type));
            emit->add_output_exp_type(bexp->value.ToString());
        }
        else if (select_list[i]->type == ExpressionType::CASE_EXPR) {
            BoundCaseExpression* bexp = (BoundCaseExpression*) select_list[i].get();
            emit->add_output_mapping(-2);
            emit->add_output_names(bexp->alias);
            emit->add_output_types(TypeIdToString(bexp->return_type));
            emit->add_output_exp_type("");

            substrait::Expression* expression = new substrait::Expression();
            expression->set_allocated_nested(getNestedSelectList(bexp));
            *project->add_expressions() = *expression;
            delete expression;
        }
        else if (select_list[i]->type == ExpressionType::BOUND_FUNCTION) {
            BoundFunctionExpression* bexp = (BoundFunctionExpression*) select_list[i].get();
            emit->add_output_mapping(-3);
            emit->add_output_names(bexp->alias);
            emit->add_output_types(TypeIdToString(bexp->return_type));
            emit->add_output_exp_type("");

            substrait::AggregateFunction* aggregate_function = select_list[i].get()->ToAggregateFunction();
            *project->add_aggregate() = *aggregate_function;
            delete aggregate_function;
        }
        else {
            std::cout << "Unknown Select Type" << std::endl;
        }
	}

	common->set_allocated_emit(emit);
	project->set_allocated_common(common);

	for (int i = 0; i < children.size(); ++i) {
		project->set_allocated_input(children[i]->ToSubstraitClass(tableid2name));
	}

	project_rel->set_allocated_project(project);

	return project_rel;
}

