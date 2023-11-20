#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

BoundAggregateExpression::BoundAggregateExpression(TypeId return_type, AggregateFunction function, bool distinct)
    : Expression(ExpressionType::BOUND_AGGREGATE, ExpressionClass::BOUND_AGGREGATE, return_type), function(function),
      distinct(distinct) {
}

string BoundAggregateExpression::ToString() const {
	string result = function.name + "(";
	if (distinct) {
		result += "DISTINCT ";
	}
	StringUtil::Join(children, children.size(), ", ",
	                 [](const unique_ptr<Expression> &child) { return child->GetName(); });
	result += ")";
	return result;
}
hash_t BoundAggregateExpression::Hash() const {
	hash_t result = Expression::Hash();
	result = CombineHash(result, duckdb::Hash(function.name.c_str()));
	result = CombineHash(result, duckdb::Hash(distinct));
	return result;
}

bool BoundAggregateExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundAggregateExpression *)other_;
	if (other->distinct != distinct) {
		return false;
	}
	if (other->function != function) {
		return false;
	}
	if (children.size() != other->children.size()) {
		return false;
	}
	for (idx_t i = 0; i < children.size(); i++) {
		if (!Expression::Equals(children[i].get(), other->children[i].get())) {
			return false;
		}
	}
	return true;
}

unique_ptr<Expression> BoundAggregateExpression::Copy() {
	auto copy = make_unique<BoundAggregateExpression>(return_type, function, distinct);
	for (auto &child : children) {
		copy->children.push_back(child->Copy());
	}
	copy->CopyProperties(*this);
	return move(copy);
}

substrait::AggregateFunction* BoundAggregateExpression::ToAggregateFunction() const {
    substrait::AggregateFunction *aggregate_function = new substrait::AggregateFunction();
    aggregate_function->set_exp_type(ExpressionTypeToString(type));

    aggregate_function->set_binder(function.name);
    aggregate_function->set_function_reference(distinct);

    substrait::Type *out_type = new substrait::Type();
    substrait::Type_String *tname = new substrait::Type_String();
    if (function.return_type.id == SQLTypeId::VARCHAR)
        tname->set_type_variation_reference(0);
    else if (function.return_type.id == SQLTypeId::INTEGER)
        tname->set_type_variation_reference(1);
    else if (function.return_type.id == SQLTypeId::BIGINT)
        tname->set_type_variation_reference(2);
    out_type->set_allocated_string(tname);
    aggregate_function->set_allocated_output_type(out_type);

    for (int j = 0; j < function.arguments.size(); ++j) {
        substrait::FunctionArgument *arg = new substrait::FunctionArgument();
        string *sql_type = new string(SQLTypeIdToString(function.arguments[j].id));
        arg->set_allocated_enum_(sql_type);
        *aggregate_function->add_arguments() = *arg;
        delete arg;
    }

    for (int j = 0; j < children.size(); ++j) {
        substrait::FunctionArgument *child = new substrait::FunctionArgument();
        substrait::AggregateFunction *func = new substrait::AggregateFunction();
        func = children[j].get()->ToAggregateFunction();
        child->set_allocated_function(func);
        *aggregate_function->add_childs() = *child;
        delete child;
    }

    return aggregate_function;
}
