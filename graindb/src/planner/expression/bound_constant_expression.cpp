#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"

using namespace duckdb;
using namespace std;

BoundConstantExpression::BoundConstantExpression(Value value)
    : Expression(ExpressionType::VALUE_CONSTANT, ExpressionClass::BOUND_CONSTANT, value.type), value(value) {
}

string BoundConstantExpression::ToString() const {
	return value.ToString();
}

bool BoundConstantExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundConstantExpression *)other_;
	return value == other->value;
}

hash_t BoundConstantExpression::Hash() const {
	hash_t result = Expression::Hash();
	return CombineHash(ValueOperations::Hash(value), result);
}

unique_ptr<Expression> BoundConstantExpression::Copy() {
	auto copy = make_unique<BoundConstantExpression>(value);
	copy->CopyProperties(*this);
	return move(copy);
}

substrait::AggregateFunction* BoundConstantExpression::ToAggregateFunction() const {
    substrait::AggregateFunction* aggregate_function = new substrait::AggregateFunction();
    aggregate_function->set_exp_type(ExpressionTypeToString(type));

    BoundConstantExpression* bound = (BoundConstantExpression*) this;

    substrait::Type* out_type = new substrait::Type();
    substrait::Type_String* tname = new substrait::Type_String();
    if (bound->return_type == TypeId::VARCHAR) {
        tname->set_type_variation_reference(0);
        aggregate_function->set_binder(bound->value.GetValue<string>());
    }
    else if (bound->return_type == TypeId::INT32) {
        tname->set_type_variation_reference(1);
        aggregate_function->set_binder(to_string(bound->value.GetValue<int32_t>()));
    }
    else if (bound->return_type == TypeId::INT64) {
        tname->set_type_variation_reference(2);
        aggregate_function->set_binder(to_string(bound->value.GetValue<int64_t>()));
    }
    out_type->set_allocated_string(tname);
    aggregate_function->set_allocated_output_type(out_type);

    return aggregate_function;
}
