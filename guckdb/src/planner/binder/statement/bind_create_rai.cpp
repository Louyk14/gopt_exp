#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/parsed_data/create_rai_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_binder/rai_binder.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_create_rai.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/parsed_data/bound_create_rai_info.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

namespace duckdb {

unique_ptr<BoundCreateRAIInfo> Binder::BindCreateRAIInfo(unique_ptr<CreateInfo> info) {
	auto &base = (CreateRAIInfo &)*info;
	auto result = make_uniq<BoundCreateRAIInfo>(move(info));

	result->name = base.name;
	result->table = Bind(*base.table);
	result->rai_direction = base.direction;
	for (auto &ref : base.referenced_tables) {
		result->referenced_tables.push_back(move(Bind(*ref)));
	}

	RAIBinder binder(*this, context);
	for (auto &expr : base.columns) {
		result->columns.push_back(move(binder.Bind(expr)));
	}
	for (auto &expr : base.references) {
		result->references.push_back(move(binder.Bind(expr)));
	}

	auto plan = CreatePlan(*result);
	result->plan = move(plan);

	return result;
}

static unique_ptr<LogicalOperator> CreatePlanForPKFKRAI(BoundCreateRAIInfo &bound_info) {
	vector<column_t> referenced_column_ids;
	vector<column_t> base_column_ids;
	vector<TableCatalogEntry *> referenced_tables;

	auto from_tbl_ref = reinterpret_cast<BoundBaseTableRef *>(bound_info.referenced_tables[0].get());
	auto base_tbl_ref = reinterpret_cast<BoundBaseTableRef *>(bound_info.table.get());
	auto from_get = reinterpret_cast<LogicalGet *>(from_tbl_ref->get.get());
	auto base_get = reinterpret_cast<LogicalGet *>(base_tbl_ref->get.get());
	from_get->column_ids.push_back((column_t)-1);
	base_get->column_ids.push_back((column_t)-1);

	string col_name = bound_info.references[0]->GetName();
    const ColumnList& entry = from_get->GetTable()->GetColumns();
	if (entry.ColumnExists(col_name)) {
		referenced_column_ids.push_back(entry.GetColumnIndex(col_name).index);
        referenced_column_ids.push_back(entry.GetColumnIndex(col_name).index);
	} else {
		throw Exception("Column " + col_name + " in rai not found");
	}
	col_name = bound_info.references[1]->GetName();
	for (auto &col : bound_info.columns) {
        const ColumnList& entry = base_get->GetTable()->GetColumns();
        if (entry.ColumnExists(col->GetName())) {
            string col_name = col->GetName();
			base_column_ids.push_back(entry.GetColumnIndex(col_name).index);
		} else {
			throw Exception("Column " + col->GetName() + " in rai not found");
		}
	}

	referenced_tables.push_back(from_get->GetTable().get());
	referenced_tables.push_back(base_get->GetTable().get());

    LogicalGet* get_ref_from_tbl = (LogicalGet*) from_tbl_ref->get.get();
    LogicalGet* get_base_tbl = (LogicalGet*) base_tbl_ref->get.get();

    int ref_from_tbl_index = get_ref_from_tbl->table_index;
    int base_tbl_index = get_base_tbl->table_index;

	auto join = make_uniq<LogicalComparisonJoin>(JoinType::LEFT);
	join->AddChild(move(base_tbl_ref->get));
	join->AddChild(move(from_tbl_ref->get));
	JoinCondition join_condition;
	join_condition.comparison = ExpressionType::COMPARE_EQUAL;
	join_condition.left = move(bound_info.columns[0]);
	join_condition.right = move(bound_info.references[0]);
	join->conditions.push_back(move(join_condition));



	ColumnBinding proj_0(ref_from_tbl_index, 1);
	ColumnBinding proj_1(base_tbl_index, 1);
	vector<unique_ptr<Expression>> selection_list;
	selection_list.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, proj_0));
	selection_list.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, proj_1));
	auto projection = make_uniq<LogicalProjection>(0, move(selection_list));
	projection->AddChild(move(join));

	ColumnBinding order_0(0, 0);
	ColumnBinding order_1(0, 1);
	BoundOrderByNode order_0_node(OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT, make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, order_0));
	BoundOrderByNode order_1_node(OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT, make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, order_1));
	vector<BoundOrderByNode> orders;
	orders.push_back(move(order_1_node));
	auto order_by = make_uniq<LogicalOrder>(move(orders));
	order_by->AddChild(move(projection));

	auto create_rai = make_uniq<LogicalCreateRAI>(bound_info.name, *base_get->GetTable(), bound_info.rai_direction,
	                                                base_column_ids, referenced_tables, referenced_column_ids);
	create_rai->AddChild(move(order_by));

	return create_rai;
}

static unique_ptr<LogicalOperator> CreatePlanForNonPKFPRAI(BoundCreateRAIInfo &bound_info) {
	vector<column_t> referenced_column_ids;
	vector<column_t> base_column_ids;
	vector<TableCatalogEntry *> referenced_tables;

	auto ref_from_tbl = reinterpret_cast<BoundBaseTableRef *>(bound_info.referenced_tables[0].get());
	auto ref_to_tbl = reinterpret_cast<BoundBaseTableRef *>(bound_info.referenced_tables[1].get());
	auto base_tbl = reinterpret_cast<BoundBaseTableRef *>(bound_info.table.get());
	auto ref_from_get = reinterpret_cast<LogicalGet *>(ref_from_tbl->get.get());
	auto ref_to_get = reinterpret_cast<LogicalGet *>(ref_to_tbl->get.get());
	auto base_get = reinterpret_cast<LogicalGet *>(base_tbl->get.get());
	ref_from_get->column_ids.push_back((column_t)-1);
	ref_to_get->column_ids.push_back((column_t)-1);
	base_get->column_ids.push_back((column_t)-1);

	string col_name = bound_info.references[0]->GetName();
    const ColumnList& entry_from_get = ref_from_get->GetTable()->GetColumns();
    if (entry_from_get.ColumnExists(col_name)) {
		referenced_column_ids.push_back(entry_from_get.GetColumnIndex(col_name).index);
	} else {
		throw Exception("Column " + col_name + " in rai not found");
	}
	col_name = bound_info.references[1]->GetName();
    const ColumnList& entry_to_get = ref_to_get->GetTable()->GetColumns();
    if (entry_to_get.ColumnExists(col_name)) {
		referenced_column_ids.push_back(entry_to_get.GetColumnIndex(col_name).index);
	} else {
		throw Exception("Column " + col_name + " in rai not found");
	}
	for (auto &col : bound_info.columns) {
        string col_name = col->GetName();
        const ColumnList& entry = base_get->GetTable()->GetColumns();
        if (entry.ColumnExists(col_name)) {
			base_column_ids.push_back(entry.GetColumnIndex(col_name).index);
		} else {
			throw Exception("Column " + col->GetName() + " in rai not found");
		}
	}

	referenced_tables.push_back(ref_from_get->GetTable().get());
	referenced_tables.push_back(ref_to_get->GetTable().get());

    LogicalGet* get_ref_from_tbl = (LogicalGet*) ref_from_tbl->get.get();
    LogicalGet* get_base_tbl = (LogicalGet*) base_tbl->get.get();
    LogicalGet* get_ref_to_tbl = (LogicalGet*) ref_to_tbl->get.get();

    int ref_from_tbl_index = get_ref_from_tbl->table_index;
    int base_tbl_index = get_base_tbl->table_index;
    int ref_to_tbl_index = get_ref_to_tbl->table_index;

	auto join = make_uniq<LogicalComparisonJoin>(JoinType::LEFT);
	join->AddChild(move(base_tbl->get));
	join->AddChild(move(ref_from_tbl->get));
	JoinCondition join_condition;
	join_condition.comparison = ExpressionType::COMPARE_EQUAL;
	join_condition.left = move(bound_info.columns[0]);
	join_condition.right = move(bound_info.references[0]);
	join->conditions.push_back(move(join_condition));

	auto parent_join = make_uniq<LogicalComparisonJoin>(JoinType::LEFT);
	parent_join->AddChild(move(join));
	parent_join->AddChild(move(ref_to_tbl->get));
	JoinCondition parent_join_condition;
	parent_join_condition.comparison = ExpressionType::COMPARE_EQUAL;
	parent_join_condition.left = move(bound_info.columns[1]);
	parent_join_condition.right = move(bound_info.references[1]);
	parent_join->conditions.push_back(move(parent_join_condition));

    ColumnBinding proj_0(ref_from_tbl_index, 1);
	ColumnBinding proj_1(base_tbl_index, 2);
	ColumnBinding proj_2(ref_to_tbl_index, 1);
	vector<unique_ptr<Expression>> selection_list;
	selection_list.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, proj_0));
	selection_list.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, proj_1));
	selection_list.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, proj_2));

	auto projection = make_uniq<LogicalProjection>(4, move(selection_list));
	projection->AddChild(move(parent_join));

	ColumnBinding order_0(4, 0);
	ColumnBinding order_1(4, 1);
	BoundOrderByNode order_0_node(OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT, make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, order_0));
	BoundOrderByNode order_1_node(OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT, make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, order_1));
	vector<BoundOrderByNode> orders;
	orders.push_back(move(order_1_node));
	auto order_by = make_uniq<LogicalOrder>(move(orders));
	order_by->AddChild(move(projection));

	auto create_rai = make_uniq<LogicalCreateRAI>(bound_info.name, *base_get->GetTable(), bound_info.rai_direction,
	                                                base_column_ids, referenced_tables, referenced_column_ids);
	create_rai->AddChild(move(order_by));

	return create_rai;
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundCreateRAIInfo &bound_info) {
	if (bound_info.rai_direction == RAIDirection::PKFK) {
		return CreatePlanForPKFKRAI(bound_info);
	} else {
		return CreatePlanForNonPKFPRAI(bound_info);
	}
}

} // namespace duckdb
