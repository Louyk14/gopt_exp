#include "duckdb/main/protobuf_serializer.hpp"
#include "duckdb/protocode/algebra.pb.h"
#include "duckdb/common/types.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"

#include <unordered_set>
#include <string>
#include <chrono>
#include <iostream>
#include <stack>
#include <fstream>

using namespace duckdb;
using namespace std;

unique_ptr<Expression> formulateFunction(ClientContext& context, const substrait::AggregateFunction function, PhysicalOperatorType aggregate_type);
unique_ptr<BoundCaseExpression> getBoundCaseExpression(const substrait::Expression expr);


Value getValueFromString(string& input, TypeId type) {
    if (type == TypeId::BOOL) {
        if (input == "True") {
            Value v(true);
            return v;
        }
        else {
            Value v(false);
            return v;
        }
    }
    else if (type == TypeId::INT64) {
        int64_t value = stoll(input);
        Value v(value);
        return v;
    }
    else if (type == TypeId::INT32) {
        int32_t value = stoi(input);
        Value v(value);
        return v;
    }
    else if (type == TypeId::INT16) {
        int16_t value = stoi(input);
        Value v(value);
        return v;
    }
    else if (type == TypeId::INT8) {
        int8_t value = stoi(input);
        Value v(value);
        return v;
    }
    else if (type == TypeId::VARCHAR) {
        Value v(input);
        return v;
    }
}

unique_ptr<BoundReferenceExpression> getBoundReferenceExpression(const substrait::Expression& expr) {
    const substrait::Expression_ReferenceSegment_MapKey left_alias_pb = expr.selection().direct_reference().map_key();
    const substrait::Expression_ReferenceSegment_MapKey left_type_pb = left_alias_pb.child().map_key();
    const substrait::Expression_ReferenceSegment_StructField left_index_pb = left_type_pb.child().struct_field();

    unique_ptr<BoundReferenceExpression> lexp = make_unique<BoundReferenceExpression>(left_alias_pb.map_key().string(),
                                                                                      TypeIdFromString(left_type_pb.map_key().string()),
                                                                                      left_index_pb.field());

    return lexp;
}

unique_ptr<BoundReferenceExpression> getBoundReferenceExpression(const substrait::Expression_ReferenceSegment& expr) {
    const substrait::Expression_ReferenceSegment_MapKey left_alias_pb = expr.map_key();
    const substrait::Expression_ReferenceSegment_MapKey left_type_pb = left_alias_pb.child().map_key();
    const substrait::Expression_ReferenceSegment_StructField left_index_pb = left_type_pb.child().struct_field();

    unique_ptr<BoundReferenceExpression> lexp = make_unique<BoundReferenceExpression>(left_alias_pb.map_key().string(),
                                                                                      TypeIdFromString(left_type_pb.map_key().string()),
                                                                                      left_index_pb.field());

    return lexp;
}


unique_ptr<BoundConstantExpression> getBoundConstantExpressionFromExpressionLiteral(const substrait::Expression& expr) {
    const substrait::Expression_Literal literal = expr.literal();
    TypeId type = static_cast<TypeId>(literal.type_variation_reference());

    if (type == TypeId::INT64)
        return make_unique<BoundConstantExpression>(Value(literal.i64()));
    else if (type == TypeId::INT32)
        return make_unique<BoundConstantExpression>(Value(literal.i32()));
    else if (type == TypeId::INT16)
        return make_unique<BoundConstantExpression>(Value(literal.i16()));
    else if (type == TypeId::BOOL)
        return make_unique<BoundConstantExpression>(Value(literal.boolean()));
    else if (type == TypeId::VARCHAR)
        return make_unique<BoundConstantExpression>(Value(literal.string()));
}


unique_ptr<BoundConstantExpression> getBoundConstantExpression(const substrait::Expression& expr) {
    const substrait::Expression_ReferenceSegment_MapKey left_value_pb = expr.selection().direct_reference().map_key();
    const substrait::Expression_ReferenceSegment_MapKey left_type_pb = left_value_pb.child().map_key();

    string left_value = left_value_pb.map_key().string();
    string left_type = left_type_pb.map_key().string();

    TypeId value_type = TypeIdFromString(left_type);

    unique_ptr<BoundConstantExpression> lexp = make_unique<BoundConstantExpression>(getValueFromString(left_value, value_type));
    return lexp;
}


void GetDelimScans(PhysicalOperator *op, vector<PhysicalOperator *> &delim_scans) {
    if (op->type == PhysicalOperatorType::DELIM_SCAN) {
        delim_scans.push_back(op);
    }
    for (auto &child : op->children) {
        GetDelimScans(child.get(), delim_scans);
    }
}

void PbSerializer::Serialize(std::string *info, duckdb::PhysicalOperator* physical_operator, unordered_map<int, string>& tableid2name) {
	substrait::Rel* relation_entry = physical_operator->ToSubstraitClass(tableid2name);
	relation_entry->SerializeToString(info);

	relation_entry->Clear();
	delete relation_entry;
}

void PbSerializer::SerializeToFile(string& file_name, PhysicalOperator* physical_operator, unordered_map<int, string>& tableid2name) {
	substrait::Rel* relation_entry = physical_operator->ToSubstraitClass(tableid2name);
	ofstream out_file(file_name, ios::out|ios::binary);
	relation_entry->SerializePartialToOstream(&out_file);

	delete relation_entry;
}

unique_ptr<duckdb::PhysicalOperator> PbSerializer::DeSerialize(ClientContext& context, std::string *info) {
	substrait::Rel* relation_entry = new substrait::Rel();
	relation_entry->ParseFromString(*info);

	unordered_map<std::string, duckdb::TableCatalogEntry *> table_entry;
	unordered_map<std::string, int> table_index;
	int index = 1;
	unique_ptr<duckdb::PhysicalOperator> result = DispatchTask(context, *relation_entry, table_entry, table_index, index);
	delete relation_entry;
	return result;
}

unique_ptr<duckdb::PhysicalOperator> PbSerializer::DeSerializeFromFile(ClientContext& context, std::string& file_name) {
	ifstream in_file(file_name, ios::out|ios::binary);

	substrait::Rel* relation_entry = new substrait::Rel();
	if (!relation_entry->ParseFromIstream(&in_file)) {
		std::cerr << "Fail to the parse pb file: " + file_name << std::endl;
	}

	unordered_map<std::string, duckdb::TableCatalogEntry *> table_entry;
	unordered_map<std::string, int> table_index;
	int index = 1;
	unique_ptr<duckdb::PhysicalOperator> result = DispatchTask(context, *relation_entry, table_entry, table_index, index);
	delete relation_entry;
	return result;
}

unique_ptr<PhysicalOperator> PbSerializer::DispatchTask(ClientContext& context, const substrait::Rel& cur, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                            unordered_map<std::string, int> &table_index, int& index) {
	if (cur.has_project()) {
		return GeneratePhysicalProjection(context, cur.project(), table_entry, table_index, index);
	}
	else if (cur.has_read()) {
        if (cur.read().has_named_table())
            if (cur.read().has_filter() && cur.read().filter().has_nested())
		        return GeneratePhysicalTableScan(context, cur.read(), table_entry, table_index, index);
            else
                return GeneratePhysicalIndexScan(context, cur.read(), table_entry, table_index, index);
	    else
            return GeneratePhysicalChunkScan(context, cur.read(), table_entry, table_index, index);
    }
	else if (cur.has_sip_join()) {
		return GeneratePhysicalSIPJoin(context, cur.sip_join(), table_entry, table_index, index);
	}
	else if (cur.has_merge_sip_join()) {
		return GeneratePhysicalMergeSIPJoin(context, cur.merge_sip_join(), table_entry, table_index, index);
	}
	else if (cur.has_hash_join()) {
		return GeneratePhysicalHashJoin(context, cur.hash_join(), table_entry, table_index, index);
	}
    else if (cur.has_delim_join()) {
        return GeneratePhysicalDelimJoin(context, cur.delim_join(), table_entry, table_index, index);
    }
    else if (cur.has_filter()) {
        return GeneratePhysicalFilter(context, cur.filter(), table_entry, table_index, index);
    }
    else if (cur.has_fetch()) {
        if (cur.fetch().offset() == -1) {
            return GeneratePhysicalOrder(context, cur.fetch(), table_entry, table_index, index);
        }
        else {
            return GeneratePhysicalTopN(context, cur.fetch(), table_entry, table_index, index);
        }
    }
    else if (cur.has_aggregate()) {
        return GeneratePhysicalHashAggregate(context, cur.aggregate(), table_entry, table_index,index);
    }
    else if (cur.has_set()) {
        return GeneratePhysicalUnion(context, cur.set(), table_entry, table_index, index);
    }
    else if (cur.has_recur_cte()) {
        return GeneratePhysicalRecursiveCTE(context, cur.recur_cte(), table_entry, table_index, index);
    }
}

void PbSerializer::add_table_entry(duckdb::ClientContext &context, string& table_name, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                   unordered_map<std::string, int> &table_index, int &index) {
	table_index[table_name] = index++;

	auto table_or_view =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_name);
	table_entry[table_name] = (TableCatalogEntry *)table_or_view;
}

unique_ptr<PhysicalTableScan> PbSerializer::get_scan_function(ClientContext& context, string& table_name, idx_t table_index,
                                                              vector<idx_t>& columns_ids, vector<TypeId>& getTypes,
                                                              unordered_map<idx_t, vector<TableFilter>> table_filters) {
	auto table_or_view =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_name);

	auto table = (TableCatalogEntry *)table_or_view;
	auto logical_get = make_unique<LogicalGet>(table, table_index, columns_ids);
	logical_get->types = getTypes;

	auto scan_function =
	    make_unique<PhysicalTableScan>(*logical_get.get(), *logical_get.get()->table, logical_get.get()->table_index,
	                                   *logical_get.get()->table->storage, logical_get.get()->column_ids,
	                                   move(logical_get.get()->expressions), move(table_filters));

	return scan_function;
}


unique_ptr<Expression> getTrueFalseExpression(const substrait::Expression true_expr) {
    unique_ptr<Expression> res_if_true;
    if (true_expr.has_selection()) {
        res_if_true = move(getBoundReferenceExpression(true_expr));
    }
    else if (true_expr.has_nested()){
        res_if_true = move(getBoundCaseExpression(true_expr));
    }
    else if (true_expr.has_literal()) {
        res_if_true = move(getBoundConstantExpressionFromExpressionLiteral(true_expr));
    }

    return move(res_if_true);
}

unique_ptr<BoundCaseExpression> getBoundCaseExpression(const substrait::Expression expr) {
    const substrait::Expression_Nested_Map_KeyValue head = expr.nested().map().key_values(0);
    const substrait::Expression_Nested_Map_KeyValue body = expr.nested().map().key_values(1);

    const substrait::Expression check_expr = head.key();
    const substrait::Expression child_expr = head.value();

    const substrait::Expression_ReferenceSegment_MapKey alias_expr = check_expr.selection().direct_reference().map_key();
    const substrait::Expression_ReferenceSegment_MapKey type_expr = alias_expr.child().map_key();
    const substrait::Expression_ReferenceSegment_MapKey return_expr = type_expr.child().map_key();

    ExpressionType exp_type = ExpressionTypeFromString(type_expr.map_key().string());
    TypeId return_type = TypeIdFromString(return_expr.map_key().string());
    unique_ptr<Expression> check_global;

    if (exp_type == ExpressionType::BOUND_REF) {
        const substrait::Expression_ReferenceSegment_StructField index_expr = return_expr.child().struct_field();
        string alias = alias_expr.map_key().string();
        int index = index_expr.field();

        unique_ptr<BoundReferenceExpression> check = make_unique<BoundReferenceExpression>(alias, return_type, index);

        const substrait::Expression true_expr = body.key();
        const substrait::Expression false_expr = body.value();

        unique_ptr<Expression> res_if_true = move(getTrueFalseExpression(true_expr));
        unique_ptr<Expression> res_if_false = move(getTrueFalseExpression(false_expr));

        unique_ptr<BoundCaseExpression> bexpr = make_unique<BoundCaseExpression>(move(check), move(res_if_true), move(res_if_false));
        return bexpr;
    }
    else if (exp_type == ExpressionType::OPERATOR_IS_NOT_NULL) {
        unique_ptr<BoundOperatorExpression> check = make_unique<BoundOperatorExpression>(exp_type, return_type);
        vector<unique_ptr<Expression>> children;

        if (child_expr.selection().has_direct_reference()) {
            substrait::Expression_ReferenceSegment list_head = child_expr.selection().direct_reference();

            while (true) {
                ExpressionType expr_type = ExpressionTypeFromString(list_head.map_key().map_key().string());

                if (expr_type == ExpressionType::BOUND_REF) {
                    children.push_back(getBoundReferenceExpression(list_head.map_key().child()));
                    auto new_head = list_head.map_key().child().map_key().child().map_key().child().struct_field();
                    if (new_head.has_child())
                        list_head = new_head.child();
                    else
                        break;
                }
            }
        }
        check->children = move(children);

        const substrait::Expression true_expr = body.key();
        const substrait::Expression false_expr = body.value();

        unique_ptr<Expression> res_if_true = move(getTrueFalseExpression(true_expr));
        unique_ptr<Expression> res_if_false = move(getTrueFalseExpression(false_expr));

        unique_ptr<BoundCaseExpression> bexpr = make_unique<BoundCaseExpression>(move(check), move(res_if_true), move(res_if_false));
        return bexpr;
    }
    else {
        std::cout << "unexpected bound case expression type" << std::endl;
    }
}

unique_ptr<PhysicalProjection> PbSerializer::GeneratePhysicalProjection(ClientContext& context, const substrait::ProjectRel& project_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                                        unordered_map<std::string, int> &table_index, int& index) {
	// std::cout << "project " << index << std::endl;
	unique_ptr<PhysicalOperator> child_operator = move(DispatchTask(context, project_rel.input(), table_entry, table_index, index));

	const substrait::RelCommon common = project_rel.common();

	if (common.has_emit()) {
		vector<TypeId> result_types;
		vector<unique_ptr<Expression>> select_list;

        int expression_index = 0;
        int aggregate_function_index = 0;

		const substrait::RelCommon_Emit emit = common.emit();
		for (int i = 0; i < emit.output_mapping_size(); ++i) {
            if (emit.output_mapping(i) != -1 && emit.output_mapping(i) != -2 && emit.output_mapping(i) != -3) {
                // BOUND_REF
                result_types.push_back(TypeIdFromString(emit.output_types(i)));
                auto result_col = make_unique<BoundReferenceExpression>(emit.output_names(i),
                                                                        TypeIdFromString(emit.output_types(i)),
                                                                        emit.output_mapping(i));
                result_col->alias = emit.output_names(i);
                select_list.push_back(move(result_col));
            }
            else if (emit.output_mapping(i) == -1) {
                // VALUE_CONSTANT
                result_types.push_back(TypeIdFromString(emit.output_types(i)));
                string value = emit.output_exp_type(i);
                auto result_col = make_unique<BoundConstantExpression>(getValueFromString(value, TypeIdFromString(emit.output_types(i))));
                result_col->alias = emit.output_names(i);
                select_list.push_back(move(result_col));
            }
            else if (emit.output_mapping(i) == -2) {
                // CASE_EXPR
                result_types.push_back(TypeIdFromString(emit.output_types(i)));
                auto result_col = getBoundCaseExpression(project_rel.expressions(expression_index++));
                result_col->alias = emit.output_names(i);
                select_list.push_back(move(result_col));
            }
            else if (emit.output_mapping(i) == -3) {
                // BOUND_FUNCTION
                result_types.push_back(TypeIdFromString(emit.output_types(i)));
                auto result_col = formulateFunction(context, project_rel.aggregate(aggregate_function_index++), PhysicalOperatorType::AGGREGATE);
                result_col->alias = emit.output_names(i);
                select_list.push_back(move(result_col));
            }
		}

		auto projection = make_unique<PhysicalProjection>(result_types, move(select_list));
		projection->children.push_back(move(child_operator));

		// std::cout << "out projection" << std::endl;
		return projection;
	}
}

unique_ptr<Expression> resolveBoundExpression(substrait::Expression expr) {
    substrait::Expression_Literal literal = expr.literal();
    ExpressionType expr_type = static_cast<ExpressionType>(literal.list().values(0).i64());

    if (expr_type == ExpressionType::BOUND_REF) {
        string alias = literal.list().values(1).string();
        TypeId return_type = TypeIdFromString(literal.list().values(2).string());
        int index = literal.list().values(3).i64();

        unique_ptr<BoundReferenceExpression> lexp = make_unique<BoundReferenceExpression>(alias,
                                                                                          return_type,
                                                                                          index);
        return lexp;
    }
    else if (expr_type == ExpressionType::VALUE_CONSTANT) {
        TypeId return_type = TypeIdFromString(literal.list().values(1).string());
        string value_str = literal.list().values(2).string();
        unique_ptr<BoundConstantExpression> lexp = make_unique<BoundConstantExpression>(getValueFromString(value_str, return_type));
        return lexp;
    }
}

unique_ptr<PhysicalFilter> PbSerializer::GeneratePhysicalFilter(ClientContext& context, const substrait::FilterRel& filter_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                                        unordered_map<std::string, int> &table_index, int& index) {
    vector<TypeId> get_types;
    const substrait::RelCommon common = filter_rel.common();
    const substrait::RelCommon_Emit emit = common.emit();
    for (int i = 0; i < emit.output_types_size(); ++i) {
        get_types.push_back(TypeIdFromString(emit.output_types(i)));
    }

    vector<unique_ptr<Expression>> select_list;

    ExpressionType exp_type = ExpressionTypeFromString(filter_rel.type());

    for (int i = 0; i < filter_rel.lcondition_size(); ++i) {
        ExpressionType filter_type = ExpressionTypeFromString(filter_rel.filter_type(i));
        if (filter_type == ExpressionType::OPERATOR_IS_NULL) {
            const substrait::Expression expression_left = filter_rel.lcondition(i);
            unique_ptr<BoundOperatorExpression> expr = make_unique<BoundOperatorExpression>(filter_type, TypeId::BOOL);
            expr->children.push_back(move(resolveBoundExpression(expression_left)));
            select_list.push_back(move(expr));
        }
        else if (filter_type == ExpressionType::COMPARE_BETWEEN) {
            const substrait::Expression expression_lower = filter_rel.lcondition(i);
            const substrait::Expression expression_upper = filter_rel.rcondition(i);
            const substrait::Expression expression_input = filter_rel.lcondition(++i);
            const substrait::Expression slot = filter_rel.rcondition(i);

            const substrait::Expression_Literal_List list = slot.literal().list();
            bool lower_inclusive = list.values(0).boolean();
            bool upper_inclusive = list.values(1).boolean();

            unique_ptr<BoundBetweenExpression> expr = make_unique<BoundBetweenExpression>(resolveBoundExpression(expression_input),
                                                                                          resolveBoundExpression(expression_lower),
                                                                                          resolveBoundExpression(expression_upper),
                                                                                          lower_inclusive, upper_inclusive);
            select_list.push_back(move(expr));
        }
        else {
            const substrait::Expression expression_left = filter_rel.lcondition(i);
            const substrait::Expression expression_right = filter_rel.rcondition(i);

            unique_ptr<BoundComparisonExpression> expr = make_unique<BoundComparisonExpression>(filter_type,
                                                                                                move(resolveBoundExpression(
                                                                                                        expression_left)),
                                                                                                move(resolveBoundExpression(
                                                                                                        expression_right)));
            select_list.push_back(move(expr));
        }
    }

    if (exp_type == ExpressionType::CONJUNCTION_OR) {
        auto conjunction = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
        for (auto &expr : select_list) {
            conjunction->children.push_back(move(expr));
        }
        select_list.clear();
        select_list.push_back(move(conjunction));
    }

    // std::cout << "project " << index << std::endl;
    unique_ptr<PhysicalOperator> child_operator = move(DispatchTask(context, filter_rel.input(), table_entry, table_index, index));
    unique_ptr<PhysicalFilter> filter_operator = make_unique<PhysicalFilter>(get_types, move(select_list));

    filter_operator->children.push_back(move(child_operator));

    return filter_operator;
}


unique_ptr<PhysicalTableScan> PbSerializer::GeneratePhysicalTableScan(ClientContext& context, const substrait::ReadRel& read_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                                        unordered_map<std::string, int> &table_index, int& index) {
	// std::cout << "scan " << index << std::endl;
	const substrait::RelCommon common = read_rel.common();
	const substrait::ReadRel_NamedTable named_table = read_rel.named_table();

	vector<idx_t> get_ids;
	vector<TypeId> get_types;
	unordered_map<idx_t, vector<TableFilter>> filters;

	string table_name = named_table.names(0);
	if (table_index.find(table_name) == table_index.end()) {
		add_table_entry(context, table_name, table_entry, table_index, index);
	}

	int table_id = table_index[table_name];

	const substrait::RelCommon_Emit emit = common.emit();
	for (int i = 0; i < emit.output_mapping_size(); ++i) {
		get_ids.push_back(emit.output_mapping(i));
		get_types.push_back(TypeIdFromString(emit.output_types(i)));
	}

	if (read_rel.has_filter()) {
		const substrait::Expression filter = read_rel.filter();
		if (filter.has_nested()) {
            const substrait::Expression_Nested_List listed_filter = filter.nested().list();
            for (int i = 0; i < listed_filter.values_size(); ++i) {
                const substrait::Expression_ScalarFunction scalar_function = listed_filter.values(i).scalar_function();

                const substrait::FunctionArgument arg1 = scalar_function.arguments(0);
                const substrait::FunctionArgument arg2 = scalar_function.arguments(1);

                int arg1_index = arg1.value().selection().direct_reference().struct_field().field();

                Value compareVal;
                if (arg2.value().has_literal()) {
                    const substrait::Expression_Literal literal2 = arg2.value().literal();
                    if (literal2.has_string()) {
                        compareVal = Value(literal2.string());
                    } else if (literal2.has_i64()) {
                        compareVal = Value(literal2.i64());
                    } else if (literal2.has_i32()) {
                        compareVal = Value(literal2.i32());
                    } else if (literal2.has_i16()) {
                        compareVal = Value(literal2.i16());
                    } else if (literal2.has_i8()) {
                        compareVal = Value(literal2.i8());
                    } else if (literal2.has_boolean()) {
                        compareVal = Value(literal2.boolean());
                    }
                }

                ExpressionType compare_type = static_cast<ExpressionType>(scalar_function.function_reference());
                TableFilter filter(compareVal, compare_type, arg1_index);
                filters[arg1_index].push_back(filter);
            }
		}
	}

	auto scan_function = move(get_scan_function(context, table_name, table_id,
	                                                  get_ids, get_types, filters));

	// std::cout << "out scan" << std::endl;
	return scan_function;
}


unique_ptr<PhysicalIndexScan> PbSerializer::GeneratePhysicalIndexScan(ClientContext& context, const substrait::ReadRel& read_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                                      unordered_map<std::string, int> &table_index, int& index) {
    // std::cout << "scan " << index << std::endl;
    const substrait::RelCommon common = read_rel.common();
    const substrait::ReadRel_NamedTable named_table = read_rel.named_table();

    vector<idx_t> get_ids;
    vector<TypeId> get_types;

    string table_name = named_table.names(0);
    if (table_index.find(table_name) == table_index.end()) {
        add_table_entry(context, table_name, table_entry, table_index, index);
    }

    int table_id = table_index[table_name];

    const substrait::RelCommon_Emit emit = common.emit();
    for (int i = 0; i < emit.output_mapping_size(); ++i) {
        get_ids.push_back(emit.output_mapping(i));
        get_types.push_back(TypeIdFromString(emit.output_types(i)));
    }

    auto table_or_view =
            Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_name);

    auto table = (TableCatalogEntry *)table_or_view;
    auto logical_get = make_unique<LogicalGet>(move(table), table_id, move(get_ids));
    logical_get->types = get_types;

    int index_column_id = atoi(named_table.names(1).c_str());

    for (size_t j = 0; j < table->storage->info->indexes.size(); j++) {
        if (table->storage->info->indexes[j]->column_ids[0] == index_column_id) {
            unique_ptr<PhysicalIndexScan> index_scan_function
                    = make_unique<PhysicalIndexScan>(*logical_get, *table, *table->storage.get(), *table->storage->info->indexes[j],
                                                     logical_get->column_ids, table_id);

            int exp_type_index = 0;
            if (emit.output_names(0) == "LOW") {
                index_scan_function->low_index = true;
                string v = emit.output_names(1);
                index_scan_function->low_value = getValueFromString(v, TypeIdFromString(emit.output_exp_type(0)));
                index_scan_function->low_expression_type = static_cast<ExpressionType>(emit.output_order(exp_type_index++));
            }
            else {
                index_scan_function->low_index = false;
            }
            if (emit.output_names(2) == "HIGH") {
                index_scan_function->high_index = true;
                string v = emit.output_names(3);
                index_scan_function->high_value = getValueFromString(v, TypeIdFromString(emit.output_exp_type(1)));
                index_scan_function->high_expression_type = static_cast<ExpressionType>(emit.output_order(exp_type_index++));
            }
            else {
                index_scan_function->high_value = false;
            }
            if (emit.output_names(4) == "EQUAL") {
                index_scan_function->equal_index = true;
                string v = emit.output_names(5);
                index_scan_function->equal_value = getValueFromString(v, TypeIdFromString(emit.output_exp_type(2)));
            }

            return index_scan_function;
        }
    }
}


unique_ptr<PhysicalChunkScan> PbSerializer::GeneratePhysicalChunkScan(ClientContext& context, const substrait::ReadRel& read_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                                      unordered_map<std::string, int> &table_index, int& index) {
    const substrait::RelCommon common = read_rel.common();
    const substrait::RelCommon_Emit emit = common.emit();
    vector<TypeId> get_types;
    for (int i = 0; i < emit.output_types_size(); ++i) {
        get_types.push_back(TypeIdFromString(emit.output_types(i)));
    }

    PhysicalOperatorType op_type = PhysicalOperatorFromString(emit.output_exp_type(0));

    auto chunk_scan = make_unique<PhysicalChunkScan>(get_types, op_type);

    // std::cout << "out scan" << std::endl;
    return chunk_scan;
}


unique_ptr<PhysicalSIPJoin> PbSerializer::GeneratePhysicalSIPJoin(duckdb::ClientContext &context, const substrait::SIPJoinRel& sip_join_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                                  unordered_map<std::string, int> &table_index, int &index) {
	// std::cout << "sipjoin " << index << std::endl;
	const substrait::RelCommon common = sip_join_rel.common();
	unique_ptr<PhysicalOperator> left_child_operator = move(DispatchTask(context, sip_join_rel.left(), table_entry, table_index, index));
	unique_ptr<PhysicalOperator> right_child_operator = move(DispatchTask(context, sip_join_rel.right(), table_entry, table_index, index));

	vector<JoinCondition> join_conditions;
	const int rai_index = sip_join_rel.rai_index();
	for (int i = 0; i < sip_join_rel.left_keys_size(); ++i) {
		JoinCondition join_condition;
		const substrait::Expression_FieldReference left_key = sip_join_rel.left_keys(i);
		const substrait::Expression_ReferenceSegment_MapKey left_variable_name = left_key.direct_reference().map_key();
		const substrait::Expression_ReferenceSegment_MapKey left_type = left_variable_name.child().map_key();
		const substrait::Expression_ReferenceSegment_StructField left_variable_index = left_type.child().struct_field();
		join_condition.left = make_unique<BoundReferenceExpression>(
		    left_variable_name.map_key().string(), TypeIdFromString(left_type.map_key().string()), left_variable_index.field());

		const substrait::Expression_FieldReference right_key = sip_join_rel.right_keys(i);
		const substrait::Expression_ReferenceSegment_MapKey right_variable_name = right_key.direct_reference().map_key();
		const substrait::Expression_ReferenceSegment_MapKey right_type = right_variable_name.child().map_key();
		const substrait::Expression_ReferenceSegment_StructField right_variable_index = right_type.child().struct_field();
		join_condition.right = make_unique<BoundReferenceExpression>(
		    right_variable_name.map_key().string(), TypeIdFromString(right_type.map_key().string()), right_variable_index.field());
		join_condition.comparison = ExpressionType::COMPARE_EQUAL;

		if (i == rai_index) {
			auto rai_info = make_unique<RAIInfo>();
			string rai_name = sip_join_rel.rai_name();
			string table_name = rai_name.substr(0, rai_name.size() - 4);
			if (table_entry.find(table_name) == table_entry.end())
				add_table_entry(context, table_name, table_entry, table_index, index);

			rai_info->rai = table_entry[table_name]->storage->info->rais[0].get();
			rai_info->rai_type = rai_info->StringToRAIType(sip_join_rel.rai_type());
			rai_info->forward = sip_join_rel.rai_forward();
            if (rai_info->rai_type != RAIType::SELF) {
                rai_info->vertex = table_entry[sip_join_rel.rai_vertex()];
                rai_info->vertex_id = table_index[sip_join_rel.rai_vertex()];
            }

			for (int j = 0; j < 2; ++j) {
				string passing_table_name = sip_join_rel.rai_passing_tables(j);
				if (passing_table_name == "")
					rai_info->passing_tables[j] = 0;
				else {
					if (table_entry.find(table_name) == table_entry.end())
						add_table_entry(context, table_name, table_entry, table_index, index);
					rai_info->passing_tables[j] = table_index[passing_table_name];
				}
			}

			rai_info->left_cardinalities[0] = sip_join_rel.rai_left_cardinalities(0);
			rai_info->left_cardinalities[1] = sip_join_rel.rai_left_cardinalities(1);

			if (sip_join_rel.rai_compact_list()) {
				if (rai_info->forward) {
					rai_info->compact_list = &rai_info->rai->alist->compact_forward_list;
				}
				else {
					rai_info->compact_list = &rai_info->rai->alist->compact_backward_list;
				}
			}
			join_condition.rais.push_back(move(rai_info));
		}
		join_conditions.push_back(move(join_condition));
	}

	LogicalComparisonJoin join_op(JoinType::INNER);
    join_op.join_type = SubstraitSIPJoinTypeToJoinType(sip_join_rel.type());

	vector<TypeId> output_types;
	for (int i = 0; i < sip_join_rel.out_types_size(); ++i)
		output_types.push_back(TypeIdFromString(sip_join_rel.out_types(i)));
	join_op.types = output_types;


	vector<idx_t> left_projection_map, right_projection_map;
	const substrait::RelCommon_Emit emit = common.emit();
	for (int i = 0; i < emit.output_mapping_size(); ++i) {
		right_projection_map.push_back(emit.output_mapping(i));
	}

	auto sip_join_operator = make_unique<PhysicalSIPJoin>(context, join_op, move(left_child_operator),
	                                                 move(right_child_operator), move(join_conditions), join_op.join_type,
	                                                 left_projection_map, right_projection_map);

	// std::cout << "out sipjoin" << std::endl;
	return sip_join_operator;
}


unique_ptr<PhysicalMergeSIPJoin> PbSerializer::GeneratePhysicalMergeSIPJoin(duckdb::ClientContext &context, const substrait::MergeSIPJoinRel& merge_sip_join_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                                  unordered_map<std::string, int> &table_index, int &index) {
	// std::cout << "mergesipjoin " << index << std::endl;
	const substrait::RelCommon common = merge_sip_join_rel.common();
	unique_ptr<PhysicalOperator> left_child_operator = move(DispatchTask(context, merge_sip_join_rel.left(), table_entry, table_index, index));
	unique_ptr<PhysicalOperator> right_child_operator = move(DispatchTask(context, merge_sip_join_rel.right(), table_entry, table_index, index));

	vector<JoinCondition> join_conditions;
	const int rai_index = merge_sip_join_rel.rai_index();
	for (int i = 0; i < merge_sip_join_rel.left_keys_size(); ++i) {
		JoinCondition join_condition;
		const substrait::Expression_FieldReference left_key = merge_sip_join_rel.left_keys(i);
		const substrait::Expression_ReferenceSegment_MapKey left_variable_name = left_key.direct_reference().map_key();
		const substrait::Expression_ReferenceSegment_MapKey left_type = left_variable_name.child().map_key();
		const substrait::Expression_ReferenceSegment_StructField left_variable_index = left_type.child().struct_field();
		join_condition.left = make_unique<BoundReferenceExpression>(
		    left_variable_name.map_key().string(), TypeIdFromString(left_type.map_key().string()), left_variable_index.field());

		const substrait::Expression_FieldReference right_key = merge_sip_join_rel.right_keys(i);
		const substrait::Expression_ReferenceSegment_MapKey right_variable_name = right_key.direct_reference().map_key();
		const substrait::Expression_ReferenceSegment_MapKey right_type = right_variable_name.child().map_key();
		const substrait::Expression_ReferenceSegment_StructField right_variable_index = right_type.child().struct_field();
		join_condition.right = make_unique<BoundReferenceExpression>(
		    right_variable_name.map_key().string(), TypeIdFromString(right_type.map_key().string()), right_variable_index.field());
		join_condition.comparison = ExpressionType::COMPARE_EQUAL;

		if (i == rai_index) {
			auto rai_info = make_unique<RAIInfo>();
			string rai_name = merge_sip_join_rel.rai_name();
			string table_name = rai_name.substr(0, rai_name.size() - 4);
			if (table_entry.find(table_name) == table_entry.end())
				add_table_entry(context, table_name, table_entry, table_index, index);

			rai_info->rai = table_entry[table_name]->storage->info->rais[0].get();
			rai_info->rai_type = rai_info->StringToRAIType(merge_sip_join_rel.rai_type());
			rai_info->forward = merge_sip_join_rel.rai_forward();
			rai_info->vertex = table_entry[merge_sip_join_rel.rai_vertex()];
			rai_info->vertex_id = table_index[merge_sip_join_rel.rai_vertex()];

			for (int j = 0; j < 2; ++j) {
				string passing_table_name = merge_sip_join_rel.rai_passing_tables(j);
				if (passing_table_name == "")
					rai_info->passing_tables[j] = 0;
				else {
					if (table_entry.find(table_name) == table_entry.end())
						add_table_entry(context, table_name, table_entry, table_index, index);
					rai_info->passing_tables[j] = table_index[passing_table_name];
				}
			}

			rai_info->left_cardinalities[0] = merge_sip_join_rel.rai_left_cardinalities(0);
			rai_info->left_cardinalities[1] = merge_sip_join_rel.rai_left_cardinalities(1);

			if (merge_sip_join_rel.rai_compact_list()) {
				if (rai_info->forward) {
					rai_info->compact_list = &rai_info->rai->alist->compact_forward_list;
				}
				else {
					rai_info->compact_list = &rai_info->rai->alist->compact_backward_list;
				}
			}
			join_condition.rais.push_back(move(rai_info));
		}
		join_conditions.push_back(move(join_condition));
	}

	LogicalComparisonJoin join_op(JoinType::INNER);
    join_op.join_type = SubstraitMergeSIPJoinTypeToJoinType(merge_sip_join_rel.type());

	vector<TypeId> output_types;
	for (int i = 0; i < merge_sip_join_rel.out_types_size(); ++i)
		output_types.push_back(TypeIdFromString(merge_sip_join_rel.out_types(i)));
	join_op.types = output_types;

	vector<idx_t> merge_projection_map;
	vector<idx_t> left_projection_map, right_projection_map;
	const substrait::RelCommon_Emit emit = common.emit();
	for (int i = 0; i < emit.output_mapping_size(); ++i) {
		right_projection_map.push_back(emit.output_mapping(i));
	}

	auto merge_sip_join_operator = make_unique<PhysicalMergeSIPJoin>(context, join_op, move(left_child_operator),
	                                                      move(right_child_operator), move(join_conditions), join_op.join_type,
	                                                      left_projection_map, right_projection_map, merge_projection_map);

	// std::cout << "out mergesipjoin" << std::endl;
	return merge_sip_join_operator;
}

unique_ptr<PhysicalHashJoin> PbSerializer::GeneratePhysicalHashJoin(duckdb::ClientContext &context, const substrait::HashJoinRel& hash_join_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                                            unordered_map<std::string, int> &table_index, int &index) {
	// std::cout << "hashjoin " << index << std::endl;
	const substrait::RelCommon common = hash_join_rel.common();
	unique_ptr<PhysicalOperator> left_child_operator = move(DispatchTask(context, hash_join_rel.left(), table_entry, table_index, index));
	unique_ptr<PhysicalOperator> right_child_operator = move(DispatchTask(context, hash_join_rel.right(), table_entry, table_index, index));

	vector<JoinCondition> join_conditions;
	for (int i = 0; i < hash_join_rel.left_keys_size(); ++i) {
		JoinCondition join_condition;
		const substrait::Expression_FieldReference left_key = hash_join_rel.left_keys(i);
		const substrait::Expression_ReferenceSegment_MapKey left_variable_name = left_key.direct_reference().map_key();
		const substrait::Expression_ReferenceSegment_MapKey left_type = left_variable_name.child().map_key();
		const substrait::Expression_ReferenceSegment_StructField left_variable_index = left_type.child().struct_field();
		join_condition.left = make_unique<BoundReferenceExpression>(
		    left_variable_name.map_key().string(), TypeIdFromString(left_type.map_key().string()), left_variable_index.field());

		const substrait::Expression_FieldReference right_key = hash_join_rel.right_keys(i);
		const substrait::Expression_ReferenceSegment_MapKey right_variable_name = right_key.direct_reference().map_key();
		const substrait::Expression_ReferenceSegment_MapKey right_type = right_variable_name.child().map_key();
		const substrait::Expression_ReferenceSegment_StructField right_variable_index = right_type.child().struct_field();
		join_condition.right = make_unique<BoundReferenceExpression>(
		    right_variable_name.map_key().string(), TypeIdFromString(right_type.map_key().string()), right_variable_index.field());
		join_condition.comparison = ExpressionType::COMPARE_EQUAL;

		join_conditions.push_back(move(join_condition));
	}

	LogicalComparisonJoin join_op(JoinType::INNER);
    join_op.join_type = SubstraitHashJoinTypeToJoinType(hash_join_rel.type());

	vector<TypeId> output_types;
	for (int i = 0; i < hash_join_rel.out_types_size(); ++i)
		output_types.push_back(TypeIdFromString(hash_join_rel.out_types(i)));
	join_op.types = output_types;

	vector<idx_t> left_projection_map, right_projection_map;
	const substrait::RelCommon_Emit emit = common.emit();
	for (int i = 0; i < emit.output_mapping_size(); ++i) {
		right_projection_map.push_back(emit.output_mapping(i));
	}

	auto hash_join_operator = make_unique<PhysicalHashJoin>(context, join_op, move(left_child_operator),
	                                                                 move(right_child_operator), move(join_conditions), join_op.join_type,
	                                                                 left_projection_map, right_projection_map);

	// std::cout << "out hashjoin" << std::endl;
	return hash_join_operator;
}

unique_ptr<PhysicalDelimJoin> PbSerializer::GeneratePhysicalDelimJoin(duckdb::ClientContext &context, const substrait::DelimJoinRel& delim_join_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                                    unordered_map<std::string, int> &table_index, int &index) {
    const substrait::RelCommon common = delim_join_rel.common();
    // input_operator itself is children[0] in the original tree
    unique_ptr<PhysicalOperator> input_operator = move(DispatchTask(context, delim_join_rel.input(), table_entry, table_index, index));
    unique_ptr<PhysicalOperator> distinct_operator = move(DispatchTask(context, delim_join_rel.distinct(), table_entry, table_index, index));
    unique_ptr<PhysicalOperator> join_operator = move(DispatchTask(context, delim_join_rel.join(), table_entry, table_index, index));

    vector<TypeId> out_types;
    for (int i = 0; i < delim_join_rel.out_types_size(); ++i) {
        out_types.push_back(TypeIdFromString(delim_join_rel.out_types(i)));
    }
    LogicalComparisonJoin op(JoinType::INNER);
    op.types = out_types;

    join_operator->children[0] = move(input_operator);

    vector<PhysicalOperator*> delim_scans;
    GetDelimScans(join_operator->children[1].get(), delim_scans);
    unique_ptr<PhysicalDelimJoin> delim_join_operator = make_unique<PhysicalDelimJoin>(op, move(join_operator),
            move(delim_scans));

    PhysicalChunkScan* chunk_scan = (PhysicalChunkScan*) distinct_operator->children[0]->children[0].get();
    chunk_scan->collection = &delim_join_operator->lhs_data;

    delim_join_operator->distinct = move(distinct_operator);
    // delim_join_operator->children.push_back(move(input_operator));
    // std::cout << "out hashjoin" << std::endl;
    return delim_join_operator;
}

OrderType OrderTypeFromInt(int type) {
    if (type == 0)
        return OrderType::INVALID;
    else if (type == 1)
        return OrderType::ASCENDING;
    else if (type == 2)
        return OrderType::DESCENDING;
    return OrderType::INVALID;
}

unique_ptr<PhysicalTopN> PbSerializer::GeneratePhysicalTopN(ClientContext& context, const substrait::FetchRel& topn_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                   unordered_map<std::string, int> &table_index, int& index) {
    unique_ptr<PhysicalOperator> child_operator = move(DispatchTask(context, topn_rel.input(), table_entry, table_index, index));

    const substrait::RelCommon common = topn_rel.common();
    const substrait::RelCommon_Emit emit = common.emit();

    int offset = topn_rel.offset();
    int limit = topn_rel.count();
    vector<BoundOrderByNode> orders;

    for (int i = 0; i < emit.output_mapping_size(); ++i) {
        if (emit.output_exp_type(i) == "BOUND_REF") {
            OrderType type = OrderTypeFromInt(emit.output_order(i));
            unique_ptr<BoundReferenceExpression> expr = make_unique<BoundReferenceExpression>(emit.output_names(i),
                                                                                              TypeIdFromString(emit.output_types(i)),emit.output_mapping(i));
            BoundOrderByNode order(type, move(expr));
            orders.push_back(move(order));
        }
    }

    vector<TypeId> get_types;
    for (int i = 0; i < topn_rel.types_size(); ++i) {
        get_types.push_back(TypeIdFromString(topn_rel.types(i)));
    }

    LogicalComparisonJoin op(JoinType::INNER);
    op.types = get_types;

    // LogicalOperator &op, vector<BoundOrderByNode> orders, idx_t limit, idx_t offset
    unique_ptr<PhysicalTopN> topn_operator = make_unique<PhysicalTopN>(op, move(orders), limit, offset);
    topn_operator->children.push_back(move(child_operator));

    return topn_operator;
}

unique_ptr<PhysicalOrder> PbSerializer::GeneratePhysicalOrder(ClientContext& context, const substrait::FetchRel& order_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                            unordered_map<std::string, int> &table_index, int& index) {
    unique_ptr<PhysicalOperator> child_operator = move(DispatchTask(context, order_rel.input(), table_entry, table_index, index));

    const substrait::RelCommon common = order_rel.common();
    const substrait::RelCommon_Emit emit = common.emit();

    vector<BoundOrderByNode> orders;

    for (int i = 0; i < emit.output_mapping_size(); ++i) {
        if (emit.output_exp_type(i) == "BOUND_REF") {
            OrderType type = static_cast<OrderType>(emit.output_order(i));
            unique_ptr<BoundReferenceExpression> expr = make_unique<BoundReferenceExpression>(emit.output_names(i),
                                                                                              TypeIdFromString(emit.output_types(i)),emit.output_mapping(i));
            BoundOrderByNode order(type, move(expr));
            orders.push_back(move(order));
        }
    }

    vector<TypeId> get_types;
    for (int i = 0; i < order_rel.types_size(); ++i) {
        get_types.push_back(TypeIdFromString(order_rel.types(i)));
    }

    unique_ptr<PhysicalOrder> order_operator = make_unique<PhysicalOrder>(get_types, move(orders));
    order_operator->children.push_back(move(child_operator));

    return order_operator;
}


unique_ptr<BoundReferenceExpression> fromExpressionToNTI(const substrait::Expression& expr) {
    const substrait::Expression_ReferenceSegment_MapKey name_map_key = expr.selection().direct_reference().map_key();
    const substrait::Expression_ReferenceSegment_MapKey type_map_key = name_map_key.child().map_key();
    const substrait::Expression_ReferenceSegment_StructField index_field = type_map_key.child().struct_field();

    string name = name_map_key.map_key().string();
    TypeId type = TypeIdFromString(type_map_key.map_key().string());
    int index = index_field.field();

    unique_ptr<BoundReferenceExpression> bound_expr = make_unique<BoundReferenceExpression>(name, type, index);
    return bound_expr;
}

unique_ptr<Expression> formulateFunction(ClientContext& context, const substrait::AggregateFunction function, PhysicalOperatorType aggregate_type) {
    if (ExpressionTypeFromString(function.exp_type()) == duckdb::ExpressionType::BOUND_AGGREGATE) {
        string func_name = function.binder();
        SQLTypeId return_type = SQLTypeId::VARCHAR;
        int type = function.output_type().string().type_variation_reference();
        if (type == 0)
            return_type = SQLTypeId::VARCHAR;
        else if (type == 1)
            return_type = SQLTypeId::INTEGER;
        else if (type == 2)
            return_type = SQLTypeId::BIGINT;

        TypeId func_return_type = GetInternalType(return_type);

        vector<SQLType> arguments;
        for (int j = 0; j < function.arguments_size(); ++j) {
            const substrait::FunctionArgument arg = function.arguments(j);
            SQLTypeId sql_type = SQLTypeIdFromString(arg.enum_());
            arguments.push_back(sql_type);
        }

        unique_ptr<BoundAggregateExpression> bound_expr;
        bool distinct = function.function_reference();
        if (aggregate_type == PhysicalOperatorType::DISTINCT && func_name.empty()) {
            auto bound_function = FirstFun::GetFunction(SQLTypeFromInternalType(func_return_type));
            bound_expr = make_unique<BoundAggregateExpression>(func_return_type, move(bound_function), false);
        }
        else {
            auto func = Catalog::GetCatalog(context).GetEntry(context, CatalogType::SCALAR_FUNCTION, "main",
                                                              func_name);

            AggregateFunctionCatalogEntry *aggregate_func = (AggregateFunctionCatalogEntry *) func;
            idx_t best_function = Function::BindFunction(aggregate_func->name, aggregate_func->functions, arguments);
            // found a matching function!
            auto &bound_function = aggregate_func->functions[best_function];
            bound_expr = make_unique<BoundAggregateExpression>(func_return_type, move(bound_function), distinct);
        }

        for (int j = 0; j < function.childs_size(); ++j) {
            const substrait::FunctionArgument child = function.childs(j);
            bound_expr->children.push_back(move(formulateFunction(context, child.function(), aggregate_type)));
        }

        bound_expr->arguments = move(arguments);

        return bound_expr;
    }
    else if (ExpressionTypeFromString(function.exp_type()) == duckdb::ExpressionType::BOUND_FUNCTION) {
        string func_name = function.binder();
        SQLTypeId return_type = SQLTypeId::VARCHAR;
        int type = function.output_type().string().type_variation_reference();
        if (type == 0)
            return_type = SQLTypeId::VARCHAR;
        else if (type == 1)
            return_type = SQLTypeId::INTEGER;
        else if (type == 2)
            return_type = SQLTypeId::BIGINT;

        TypeId func_return_type = GetInternalType(return_type);

        vector<SQLType> arguments;
        for (int j = 0; j < function.arguments_size(); ++j) {
            const substrait::FunctionArgument arg = function.arguments(j);
            SQLTypeId sql_type = SQLTypeIdFromString(arg.enum_());
            arguments.push_back(sql_type);
        }

        auto func = Catalog::GetCatalog(context).GetEntry(context, CatalogType::SCALAR_FUNCTION, "main",
                                                          func_name);
        ScalarFunctionCatalogEntry* scalar_func = (ScalarFunctionCatalogEntry*) func;

        idx_t best_function = Function::BindFunction(func_name, scalar_func->functions, arguments);
        // found a matching function!
        auto &bound_function = scalar_func->functions[best_function];

        bool is_operator = function.function_reference();

        unique_ptr<BoundFunctionExpression> bound_expr = make_unique<BoundFunctionExpression>(func_return_type, move(bound_function), is_operator);

        for (int j = 0; j < function.childs_size(); ++j) {
            const substrait::FunctionArgument child = function.childs(j);
            bound_expr->children.push_back(move(formulateFunction(context, child.function(), aggregate_type)));
        }

        bound_expr->arguments = move(arguments);
        bound_expr->sql_return_type = bound_function.return_type;

        return bound_expr;
    }
    else if (ExpressionTypeFromString(function.exp_type()) == duckdb::ExpressionType::BOUND_REF) {
        string alias = function.binder();
        TypeId type = TypeIdFromString(function.childs(0).enum_());
        idx_t index = atoi(function.arguments(0).enum_().c_str());
        unique_ptr<BoundReferenceExpression> bound_expr = make_unique<BoundReferenceExpression>(alias, type ,index);

        return bound_expr;
    }
    else if (ExpressionTypeFromString(function.exp_type()) == duckdb::ExpressionType::OPERATOR_CAST) {
        string source_str = function.arguments(0).enum_();
        string target_str = function.arguments(1).enum_();
        TypeId target = GetInternalType(SQLTypeIdFromString(target_str));

        unique_ptr<Expression> children = move(fromExpressionToNTI(function.childs(0).value()));

        unique_ptr<BoundCastExpression> bound_expr = make_unique<BoundCastExpression>(target, move(children), SQLTypeIdFromString(source_str), SQLTypeIdFromString(target_str));

        return bound_expr;
    }
    else if (ExpressionTypeFromString(function.exp_type()) == duckdb::ExpressionType::VALUE_CONSTANT) {
        if (function.output_type().string().type_variation_reference() == 0) {
            Value v(function.binder());
            unique_ptr<BoundConstantExpression> bound = make_unique<BoundConstantExpression>(v);
            return bound;
        }
        else if (function.output_type().string().type_variation_reference() == 1) {
            int32_t value = atol(function.binder().c_str());
            Value v(value);
            unique_ptr<BoundConstantExpression> bound = make_unique<BoundConstantExpression>(v);
            return bound;
        }
        else if (function.output_type().string().type_variation_reference() == 2) {
            int64_t value = atoll(function.binder().c_str());
            Value v(value);
            unique_ptr<BoundConstantExpression> bound = make_unique<BoundConstantExpression>(v);
            return bound;
        }
    }
    else {
        std::cout << "ExpressionType Undefined" << std::endl;
    }
}

unique_ptr<PhysicalHashAggregate> PbSerializer::GeneratePhysicalHashAggregate(ClientContext& context, const substrait::AggregateRel& aggregate_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                                              unordered_map<std::string, int> &table_index, int& index) {
    const substrait::RelCommon common = aggregate_rel.common();
    const substrait::RelCommon_Emit emit = common.emit();

    vector<TypeId> get_types;
    for (int i = 0; i < emit.output_types_size(); ++i) {
        get_types.push_back(TypeIdFromString(emit.output_types(i)));
    }

    PhysicalOperatorType aggregate_type = PhysicalOperatorFromString(aggregate_rel.type());

    vector<unique_ptr<Expression>> groups;
    for (int i = 0; i < aggregate_rel.groupings_size(); ++i) {
        const substrait::AggregateRel_Grouping grouping = aggregate_rel.groupings(i);
        for (int j = 0; j < grouping.grouping_expressions_size(); ++j) {
            const substrait::Expression group_expr = grouping.grouping_expressions(j);
            const substrait::Expression_ReferenceSegment_MapKey name_map_key = group_expr.selection().direct_reference().map_key();
            string name = name_map_key.map_key().string();

            const substrait::Expression_ReferenceSegment_MapKey type_map_key = name_map_key.child().map_key();
            auto type = TypeIdFromString(type_map_key.map_key().string());

            const substrait::Expression_ReferenceSegment_StructField index_field = type_map_key.child().struct_field();
            int index = index_field.field();

            unique_ptr<BoundReferenceExpression> group = make_unique<BoundReferenceExpression>(name, type, index);
            groups.push_back(move(group));
        }
    }

    vector<unique_ptr<Expression>> expressions;
    for (int i = 0; i < aggregate_rel.measures_size(); ++i) {
        const substrait::AggregateRel_Measure measure = aggregate_rel.measures(i);
        const substrait::AggregateFunction function = measure.measure();
        expressions.push_back(move(formulateFunction(context, function, aggregate_type)));
    }

    unique_ptr<PhysicalHashAggregate> aggregate_operator = make_unique<PhysicalHashAggregate>(get_types, move(expressions), move(groups), aggregate_type);

    unique_ptr<PhysicalOperator> child_operator = move(DispatchTask(context, aggregate_rel.input(), table_entry, table_index, index));
    aggregate_operator->children.push_back(move(child_operator));

    return aggregate_operator;
}

unique_ptr<PhysicalUnion> PbSerializer::GeneratePhysicalUnion(duckdb::ClientContext &context,
                                                              const substrait::SetRel &union_rel,
                                                              unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                              unordered_map<std::string, int> &table_index,
                                                              int &index) {
    const substrait::RelCommon common = union_rel.common();
    const substrait::RelCommon_Emit emit = common.emit();
    vector<TypeId> get_types;

    for (int i = 0; i < emit.output_types_size(); ++i) {
        get_types.push_back(TypeIdFromString(emit.output_types(i)));
    }

    LogicalComparisonJoin op(JoinType::INNER);
    op.types = get_types;

    vector<unique_ptr<PhysicalOperator>> children;
    for (int i = 0; i < union_rel.inputs_size(); ++i) {
        unique_ptr<PhysicalOperator> child_operator = move(DispatchTask(context, union_rel.inputs(i), table_entry, table_index, index));
        children.push_back(move(child_operator));
    }

    unique_ptr<PhysicalUnion> union_operator = make_unique<PhysicalUnion>(op, move(children[0]), move(children[1]));

    return union_operator;
}

unique_ptr<PhysicalRecursiveCTE> PbSerializer::GeneratePhysicalRecursiveCTE(ClientContext& context, const substrait::RecursiveCTERel& recur_cte_rel, unordered_map<std::string, duckdb::TableCatalogEntry *> &table_entry,
                                                              unordered_map<std::string, int> &table_index, int& index) {
    const substrait::RelCommon common = recur_cte_rel.common();
    unique_ptr<PhysicalOperator> top_child_operator = move(DispatchTask(context, recur_cte_rel.top(), table_entry, table_index, index));
    unique_ptr<PhysicalOperator> bottom_child_operator = move(DispatchTask(context, recur_cte_rel.bottom(), table_entry, table_index, index));

    vector<TypeId> get_types;
    const substrait::RelCommon_Emit emit = common.emit();
    for (int i = 0; i < emit.output_types_size(); ++i) {
        get_types.push_back(TypeIdFromString(emit.output_types(i)));
    }

    LogicalComparisonJoin join_op(JoinType::INNER);
    join_op.types = get_types;

    bool is_union_all = recur_cte_rel.union_all();

    auto recur_cte_operator = make_unique<PhysicalRecursiveCTE>(join_op, is_union_all, move(top_child_operator), move(bottom_child_operator));
    auto working_table = std::make_shared<ChunkCollection>();
    recur_cte_operator->working_table = working_table;

    PhysicalOperator* cur_op = recur_cte_operator.get();
    for (int i = 0; i < emit.output_mapping_size(); ++i) {
        int index = emit.output_mapping(i);
        if (index == -1) {
            assert(cur_op->type == PhysicalOperatorType::CHUNK_SCAN);
            PhysicalChunkScan* chunk_scan_op = (PhysicalChunkScan*) cur_op;
            chunk_scan_op->collection = recur_cte_operator->working_table.get();
        }
        else {
            cur_op = cur_op->children[index].get();
        }
    }

    // std::cout << "out hashjoin" << std::endl;
    return recur_cte_operator;
}
