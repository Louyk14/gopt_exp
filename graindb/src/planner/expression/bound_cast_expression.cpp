#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

using namespace duckdb;
using namespace std;

BoundCastExpression::BoundCastExpression(TypeId target, unique_ptr<Expression> child, SQLType source_type,
                                         SQLType target_type)
    : Expression(ExpressionType::OPERATOR_CAST, ExpressionClass::BOUND_CAST, target), child(move(child)),
      source_type(source_type), target_type(target_type) {
}

unique_ptr<Expression> BoundCastExpression::AddCastToType(unique_ptr<Expression> expr, SQLType source_type,
                                                          SQLType target_type) {
	assert(expr);
	if (expr->expression_class == ExpressionClass::BOUND_PARAMETER) {
		auto &parameter = (BoundParameterExpression &)*expr;
		parameter.sql_type = target_type;
		parameter.return_type = GetInternalType(target_type);
	} else if (expr->expression_class == ExpressionClass::BOUND_DEFAULT) {
		auto &def = (BoundDefaultExpression &)*expr;
		def.sql_type = target_type;
		def.return_type = GetInternalType(target_type);
	} else if (source_type != target_type) {
		return make_unique<BoundCastExpression>(GetInternalType(target_type), move(expr), source_type, target_type);
	}
	return expr;
}

bool BoundCastExpression::CastIsInvertible(SQLType source_type, SQLType target_type) {
	if (source_type.id == SQLTypeId::BOOLEAN || target_type.id == SQLTypeId::BOOLEAN) {
		return false;
	}
	if (source_type.id == SQLTypeId::FLOAT || target_type.id == SQLTypeId::FLOAT) {
		return false;
	}
	if (source_type.id == SQLTypeId::DOUBLE || target_type.id == SQLTypeId::DOUBLE) {
		return false;
	}
	if (source_type.id == SQLTypeId::VARCHAR) {
		return target_type.id == SQLTypeId::DATE || target_type.id == SQLTypeId::TIMESTAMP;
	}
	if (target_type.id == SQLTypeId::VARCHAR) {
		return source_type.id == SQLTypeId::DATE || source_type.id == SQLTypeId::TIMESTAMP;
	}
	return true;
}

string BoundCastExpression::ToString() const {
	return "CAST[" + TypeIdToString(return_type) + "](" + child->GetName() + ")";
}

bool BoundCastExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundCastExpression *)other_;
	if (!Expression::Equals(child.get(), other->child.get())) {
		return false;
	}
	if (source_type != other->source_type || target_type != other->target_type) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundCastExpression::Copy() {
	auto copy = make_unique<BoundCastExpression>(return_type, child->Copy(), source_type, target_type);
	copy->CopyProperties(*this);
	return move(copy);
}

substrait::AggregateFunction* BoundCastExpression::ToAggregateFunction() const {
    substrait::AggregateFunction* aggregate_function = new substrait::AggregateFunction();
    aggregate_function->set_exp_type(ExpressionTypeToString(type));

    aggregate_function->set_binder(alias);

    substrait::FunctionArgument* child = new substrait::FunctionArgument();
    substrait::Expression* expr = new substrait::Expression();
    child->set_allocated_value(expr);

    BoundReferenceExpression* child_bound = (BoundReferenceExpression*) this->child.get();
    substrait::Expression_FieldReference* selection = new substrait::Expression_FieldReference();
    expr->set_allocated_selection(selection);
    substrait::Expression_ReferenceSegment* direct = new substrait::Expression_ReferenceSegment();
    selection->set_allocated_direct_reference(direct);
    substrait::Expression_ReferenceSegment_MapKey* name_map_key = new substrait::Expression_ReferenceSegment_MapKey();
    direct->set_allocated_map_key(name_map_key);
    substrait::Expression_Literal* name = new substrait::Expression_Literal();
    name_map_key->set_allocated_map_key(name);
    string* alias = new string(child_bound->alias);
    name->set_allocated_string(alias);

    substrait::Expression_ReferenceSegment* type_expr = new substrait::Expression_ReferenceSegment();
    name_map_key->set_allocated_child(type_expr);
    substrait::Expression_ReferenceSegment_MapKey* type_map_key = new substrait::Expression_ReferenceSegment_MapKey();
    type_expr->set_allocated_map_key(type_map_key);
    substrait::Expression_Literal* type = new substrait::Expression_Literal();
    type_map_key->set_allocated_map_key(type);
    string* type_str = new string(TypeIdToString(child_bound->return_type));
    type->set_allocated_string(type_str);

    substrait::Expression_ReferenceSegment* index_expr = new substrait::Expression_ReferenceSegment();
    type_map_key->set_allocated_child(index_expr);
    substrait::Expression_ReferenceSegment_StructField* index_field = new substrait::Expression_ReferenceSegment_StructField();
    index_expr->set_allocated_struct_field(index_field);
    index_field->set_field(child_bound->index);

    *aggregate_function->add_childs() = *child;
    delete child;

    substrait::FunctionArgument* arg_source = new substrait::FunctionArgument();
    string* source_str = new string(SQLTypeIdToString(source_type.id));
    arg_source->set_allocated_enum_(source_str);
    *aggregate_function->add_arguments() = *arg_source;
    delete arg_source;

    substrait::FunctionArgument* arg_target = new substrait::FunctionArgument();
    string* target_str = new string(SQLTypeIdToString(target_type.id));
    arg_target->set_allocated_enum_(target_str);
    *aggregate_function->add_arguments() = *arg_target;
    delete arg_target;

    return aggregate_function;
}
