#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/execution/operator/list.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/execution/operator/join/physical_sip_join.hpp"

#include <string>
#include <chrono>
#include <iostream>

/**
This file contains an example on how a query tree can be programmatically constructed. This is essentially hand-rolling
the binding+planning phase for one specific query.

Note that this API is currently very unstable, and is subject to change at any moment. In general, this API should not
be used currently outside of internal use cases.
**/

using namespace duckdb;
using namespace std;

void CreateGraph(Connection& con) {
	unique_ptr<QueryResult> result;

	con.Query("DROP TABLE a");
	con.Query("DROP TABLE b");
	con.Query("DROP TABLE c");

	con.Query("CREATE TABLE a(id INTEGER)");
	con.Query("CREATE TABLE b(a_id INTEGER, c_id INTEGER)");
	con.Query("CREATE TABLE c(id INTEGER)");

	const idx_t S_NUM = 2;
	const idx_t V_NUM = 2;
	const idx_t E_NUM = 1;
	//! Insert values into a
	for (idx_t i = 0; i < S_NUM; i++) {
		result = con.Query("INSERT INTO a VALUES (" + to_string(i) + ")");
	}
	//! Insert values into c
	for (idx_t i = 0; i < V_NUM; i++) {
		result = con.Query("INSERT INTO c VALUES (" + to_string(i) + ")");
	}
	//! Insert values into b
	for (idx_t i = 0; i < S_NUM; i++) {
		auto inserted_tuples = (i * E_NUM) + 1;
		string sql = "INSERT INTO b VALUES ";
		for (idx_t j = 0; j < inserted_tuples; j++) {
			sql += "(" + to_string(i) + ", " + to_string(j) + "), ";
		}
		sql = sql.substr(0, sql.size() - 2);
		sql += ";";
		result = con.Query(sql);
		vector<Value> values = {Value::BIGINT(inserted_tuples)};
	}
}

void test_trivial_ab() {
	DuckDB db(nullptr);
	Connection con(db);
	con.DisableProfiling();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	auto &context = *con.context;

	CreateGraph(con);

	// use sql for everything
	auto result = con.Query("SELECT * FROM a, b WHERE a.id = b.a_id;");
	result->Print();

	// fully manual with custom scan op

	/*
	 ORDER_BY
	    PROJECTION[%(+(some_int, 42), 2) count()]
	        HASH_GROUP_BY
	            FILTER[some_int<=7 some_int>=3]
	                TABLE_FUNCTION
	*/

	// select * from a, b, c where a.id = b.a_id and b.c_id = c.id;

	vector<TypeId> types{TypeId::INT32, TypeId::INT32};
	vector<idx_t> column_ids_a{0};
	vector<idx_t> column_ids_b{0, 1};

	string table_vertex_a = "a";
	string table_edge_b = "b";
	string table_vertex_c = "c";
	idx_t table_index_a = 6;
	idx_t table_index_b = 7;
	idx_t table_index_c = 8;

	// build table scan of table a
	vector<unique_ptr<PhysicalOperator>> children; // empty
	unordered_map<idx_t, vector<TableFilter>> table_filters_a;

	auto table_or_view_a =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_vertex_a);

	auto table_a = (TableCatalogEntry *)table_or_view_a;
	auto logical_get_a = make_unique<LogicalGet>(table_a, table_index_a, column_ids_a);
	vector<TypeId> get_a_types{TypeId::INT32};
	logical_get_a->types = get_a_types;

	auto scan_function_a =
	    make_unique<PhysicalTableScan>(*logical_get_a.get(), *logical_get_a.get()->table, logical_get_a.get()->table_index,
	                                   *logical_get_a.get()->table->storage, logical_get_a.get()->column_ids,
	                                   move(logical_get_a.get()->expressions), move(table_filters_a));

	// build table scan of table b
	unordered_map<idx_t, vector<TableFilter>> table_filters_b;

	auto table_or_view_b =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_edge_b);

	auto table_b = (TableCatalogEntry *)table_or_view_b;
	auto logical_get_b = make_unique<LogicalGet>(table_b, table_index_b, column_ids_b);
	vector<TypeId> get_b_types{TypeId::INT32, TypeId::INT32};
	logical_get_b->types = get_b_types;

	auto scan_function_b =
	    make_unique<PhysicalTableScan>(*logical_get_b.get(), *logical_get_b.get()->table, logical_get_b.get()->table_index,
	                                   *logical_get_b.get()->table->storage, logical_get_b.get()->column_ids,
	                                   move(logical_get_b.get()->expressions), move(table_filters_b));

	// join conditions
	vector<JoinCondition> cond;
	JoinCondition joinCondition;
	// the column number of a_id and id
	joinCondition.left = make_unique<BoundReferenceExpression>("a_id", TypeId::INT32, 0);
	joinCondition.right = make_unique<BoundReferenceExpression>("id", TypeId::INT32, 0);
	joinCondition.comparison = ExpressionType::COMPARE_EQUAL;
	cond.push_back(move(joinCondition));

	vector<idx_t> left_projection_map, right_projection_map;
	LogicalComparisonJoin joinOp(JoinType::INNER);
	vector<TypeId> outputTypes{TypeId::INT32, TypeId::INT32, TypeId::INT32};
	joinOp.types = outputTypes;
	auto hashJoin = make_unique<PhysicalHashJoin>(context, joinOp, move(scan_function_b),
	                                              move(scan_function_a),move(cond), joinOp.join_type);

	idx_t table_index = 0;
	vector<TypeId> result_types{TypeId::INT32, TypeId::INT32, TypeId::INT32};
	vector<unique_ptr<Expression>> select_list;
	auto result_col0 = make_unique<BoundReferenceExpression>("id", TypeId::INT32, 2);
	auto result_col1 = make_unique<BoundReferenceExpression>("a_id", TypeId::INT32, 0);
	auto result_col2 = make_unique<BoundReferenceExpression>("c_id", TypeId::INT32, 1);
	select_list.push_back(move(result_col0));
	select_list.push_back(move(result_col1));
	select_list.push_back(move(result_col2));
	auto projection = make_unique<PhysicalProjection>(result_types, move(select_list));
	projection->children.push_back(move(hashJoin));

	// execute!
	DataChunk result_chunk;
	result_chunk.Initialize(projection->types);
	auto state = projection->GetOperatorState();

	do {
		projection->GetChunk(*con.context, result_chunk, state.get());
		result_chunk.Print();
	} while (result_chunk.size() > 0);
}

void test_trivial_abc() {
	DuckDB db(nullptr);
	Connection con(db);
	con.DisableProfiling();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	auto &context = *con.context;

	CreateGraph(con);

	// use sql for everything
	auto result = con.Query("SELECT * FROM a, b, c WHERE a.id = b.a_id AND b.c_id = c.id;");
	result->Print();

	// fully manual with custom scan op

	/*
	 ORDER_BY
	    PROJECTION[%(+(some_int, 42), 2) count()]
	        HASH_GROUP_BY
	            FILTER[some_int<=7 some_int>=3]
	                TABLE_FUNCTION
	*/

	// select * from a, b, c where a.id = b.a_id and b.c_id = c.id;

	vector<idx_t> column_ids_a{0};
	vector<idx_t> column_ids_b{0, 1};
	vector<idx_t> column_ids_c{0};

	string table_vertex_a = "a";
	string table_edge_b = "b";
	string table_vertex_c = "c";
	idx_t table_index_a = 6;
	idx_t table_index_b = 7;
	idx_t table_index_c = 8;

	// build table scan of table a
	vector<unique_ptr<PhysicalOperator>> children; // empty
	unordered_map<idx_t, vector<TableFilter>> table_filters_a;

	auto table_or_view_a =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_vertex_a);

	auto table_a = (TableCatalogEntry *)table_or_view_a;
	auto logical_get_a = make_unique<LogicalGet>(table_a, table_index_a, column_ids_a);
	vector<TypeId> get_a_types{TypeId::INT32};
	logical_get_a->types = get_a_types;

	auto scan_function_a =
	    make_unique<PhysicalTableScan>(*logical_get_a.get(), *logical_get_a.get()->table, logical_get_a.get()->table_index,
	                                   *logical_get_a.get()->table->storage, logical_get_a.get()->column_ids,
	                                   move(logical_get_a.get()->expressions), move(table_filters_a));

	// build table scan of table b
	unordered_map<idx_t, vector<TableFilter>> table_filters_b;

	auto table_or_view_b =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_edge_b);

	auto table_b = (TableCatalogEntry *)table_or_view_b;
	auto logical_get_b = make_unique<LogicalGet>(table_b, table_index_b, column_ids_b);
	vector<TypeId> get_b_types{TypeId::INT32, TypeId::INT32};
	logical_get_b->types = get_b_types;

	auto scan_function_b =
	    make_unique<PhysicalTableScan>(*logical_get_b.get(), *logical_get_b.get()->table, logical_get_b.get()->table_index,
	                                   *logical_get_b.get()->table->storage, logical_get_b.get()->column_ids,
	                                   move(logical_get_b.get()->expressions), move(table_filters_b));

	// build table scan of table c
	unordered_map<idx_t, vector<TableFilter>> table_filters_c;

	auto table_or_view_c =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_vertex_c);

	auto table_c = (TableCatalogEntry *)table_or_view_c;
	auto logical_get_c = make_unique<LogicalGet>(table_c, table_index_c, column_ids_c);
	vector<TypeId> get_c_types{TypeId::INT32};
	logical_get_c->types = get_c_types;

	auto scan_function_c =
	    make_unique<PhysicalTableScan>(*logical_get_c.get(), *logical_get_c.get()->table, logical_get_c.get()->table_index,
	                                   *logical_get_c.get()->table->storage, logical_get_c.get()->column_ids,
	                                   move(logical_get_c.get()->expressions), move(table_filters_c));

	// join condition of b and c
	vector<JoinCondition> condBC;
	JoinCondition joinConditionBC;
	joinConditionBC.left = make_unique<BoundReferenceExpression>("c_id", TypeId::INT32, 1);
	joinConditionBC.right = make_unique<BoundReferenceExpression>("id", TypeId::INT32, 0);
	joinConditionBC.comparison = ExpressionType::COMPARE_EQUAL;
	condBC.push_back(move(joinConditionBC));

	LogicalComparisonJoin joinOpBC(JoinType::INNER);
	vector<TypeId> outputTypesBC{TypeId::INT32, TypeId::INT32, TypeId::INT32};
	joinOpBC.types = outputTypesBC;
	auto hasJoinBC = make_unique<PhysicalHashJoin>(context, joinOpBC, move(scan_function_b),
	                                               move(scan_function_c), move(condBC), joinOpBC.join_type);

	// join conditions of a and b
	vector<JoinCondition> cond;
	JoinCondition joinCondition;
	// the column number of a_id and id
	joinCondition.left = make_unique<BoundReferenceExpression>("a_id", TypeId::INT32, 0);
	joinCondition.right = make_unique<BoundReferenceExpression>("id", TypeId::INT32, 0);
	joinCondition.comparison = ExpressionType::COMPARE_EQUAL;
	cond.push_back(move(joinCondition));

	vector<idx_t> left_projection_map, right_projection_map;
	LogicalComparisonJoin joinOp(JoinType::INNER);
	vector<TypeId> outputTypes{TypeId::INT32, TypeId::INT32, TypeId::INT32, TypeId::INT32};
	joinOp.types = outputTypes;
	auto hashJoinAB = make_unique<PhysicalHashJoin>(context, joinOp, move(hasJoinBC),
	                                              move(scan_function_a),move(cond), joinOp.join_type);

	vector<TypeId> result_types{TypeId::INT32, TypeId::INT32, TypeId::INT32, TypeId::INT32};
	vector<unique_ptr<Expression>> select_list;
	auto result_col0 = make_unique<BoundReferenceExpression>("id", TypeId::INT32, 2);
	auto result_col1 = make_unique<BoundReferenceExpression>("a_id", TypeId::INT32, 0);
	auto result_col2 = make_unique<BoundReferenceExpression>("c_id", TypeId::INT32, 1);
	auto result_col3 = make_unique<BoundReferenceExpression>("id", TypeId::INT32, 3);
	select_list.push_back(move(result_col0));
	select_list.push_back(move(result_col1));
	select_list.push_back(move(result_col2));
	select_list.push_back(move(result_col3));
	auto projection = make_unique<PhysicalProjection>(result_types, move(select_list));
	projection->children.push_back(move(hashJoinAB));

	// execute!
	DataChunk result_chunk;
	result_chunk.Initialize(projection->types);
	auto state = projection->GetOperatorState();

	do {
		projection->GetChunk(*con.context, result_chunk, state.get());
		result_chunk.Print();
	} while (result_chunk.size() > 0);
}

void test_sjoin_merge_abc() {
	DuckDB db(nullptr);
	Connection con(db);
	con.DisableProfiling();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	auto &context = *con.context;

	CreateGraph(con);

	// use sql for everything
	auto result = con.Query("SELECT * FROM a, b, c WHERE a.id = b.a_id AND b.c_id = c.id;");
	result->Print();

	// fully manual with custom scan op

	/*
	 ORDER_BY
	    PROJECTION[%(+(some_int, 42), 2) count()]
	        HASH_GROUP_BY
	            FILTER[some_int<=7 some_int>=3]
	                TABLE_FUNCTION
	*/

	con.context->transaction.Commit();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	con.Query("CREATE RAI b_rai ON b (FROM a_id REFERENCES a.id, TO c_id REFERENCES c.id)");

	// select * from a, b, c where a.id = b.a_id and b.c_id = c.id;

	vector<idx_t> column_ids_a{0, COLUMN_IDENTIFIER_ROW_ID};
	vector<idx_t> column_ids_b{0, 1, 2, 3};
	vector<idx_t> column_ids_c{0, COLUMN_IDENTIFIER_ROW_ID};

	vector<idx_t> left_projection_map, right_projection_map;

	string table_vertex_a = "a";
	string table_edge_b = "b";
	string table_vertex_c = "c";
	idx_t table_index_a = 6;
	idx_t table_index_b = 7;
	idx_t table_index_c = 8;

	// build table scan of table a
	vector<unique_ptr<PhysicalOperator>> children; // empty
	unordered_map<idx_t, vector<TableFilter>> table_filters_a;

	auto table_or_view_a =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_vertex_a);

	auto table_a = (TableCatalogEntry *)table_or_view_a;
	auto logical_get_a = make_unique<LogicalGet>(table_a, table_index_a, column_ids_a);
	vector<TypeId> get_a_types{TypeId::INT32, TypeId::INT64};
	logical_get_a->types = get_a_types;

	auto scan_function_a =
	    make_unique<PhysicalTableScan>(*logical_get_a.get(), *logical_get_a.get()->table, logical_get_a.get()->table_index,
	                                   *logical_get_a.get()->table->storage, logical_get_a.get()->column_ids,
	                                   move(logical_get_a.get()->expressions), move(table_filters_a));

	// build table scan of table b
	unordered_map<idx_t, vector<TableFilter>> table_filters_b;

	auto table_or_view_b =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_edge_b);

	auto table_b = (TableCatalogEntry *)table_or_view_b;
	auto logical_get_b = make_unique<LogicalGet>(table_b, table_index_b, column_ids_b);
	vector<TypeId> get_b_types{TypeId::INT32, TypeId::INT32, TypeId::INT64, TypeId::INT64};
	logical_get_b->types = get_b_types;

	auto scan_function_b =
	    make_unique<PhysicalTableScan>(*logical_get_b.get(), *logical_get_b.get()->table, logical_get_b.get()->table_index,
	                                   *logical_get_b.get()->table->storage, logical_get_b.get()->column_ids,
	                                   move(logical_get_b.get()->expressions), move(table_filters_b));

	// build table scan of table c
	unordered_map<idx_t, vector<TableFilter>> table_filters_c;

	auto table_or_view_c =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_vertex_c);

	auto table_c = (TableCatalogEntry *)table_or_view_c;
	auto logical_get_c = make_unique<LogicalGet>(table_c, table_index_c, column_ids_c);
	vector<TypeId> get_c_types{TypeId::INT32, TypeId::INT64};
	logical_get_c->types = get_c_types;

	auto scan_function_c =
	    make_unique<PhysicalTableScan>(*logical_get_c.get(), *logical_get_c.get()->table, logical_get_c.get()->table_index,
	                                   *logical_get_c.get()->table->storage, logical_get_c.get()->column_ids,
	                                   move(logical_get_c.get()->expressions), move(table_filters_c));

	// join condition of b and c
	vector<JoinCondition> condBC;
	JoinCondition joinConditionBC;
	joinConditionBC.left = make_unique<BoundReferenceExpression>("c_id_rowid", TypeId::INT64, 3);
	joinConditionBC.right = make_unique<BoundReferenceExpression>("c_rowid", TypeId::INT64, 1);
	joinConditionBC.comparison = ExpressionType::COMPARE_EQUAL;

	auto raiInfoBC = make_unique<RAIInfo>();
	raiInfoBC->rai = table_b->storage->info->rais[0].get();
	raiInfoBC->rai_type = RAIType::EDGE_TARGET;
	raiInfoBC->forward = true;
	raiInfoBC->vertex = table_c;
	raiInfoBC->vertex_id = table_index_c;
	raiInfoBC->left_cardinalities[0] = table_b->storage->info->cardinality;

	joinConditionBC.rais.push_back(move(raiInfoBC));
	condBC.push_back(move(joinConditionBC));

	LogicalComparisonJoin joinOpBC(JoinType::INNER);
	vector<TypeId> outputTypesBC{TypeId::INT32, TypeId::INT32, TypeId::INT64, TypeId::INT64, TypeId::INT32, TypeId::INT64};
	joinOpBC.types = outputTypesBC;
	auto sipJoinBC = make_unique<PhysicalSIPJoin>(context, joinOpBC, move(scan_function_b),
	                                               move(scan_function_c), move(condBC), joinOpBC.join_type,
	                                              left_projection_map, right_projection_map);

	// join conditions of a and b
	vector<JoinCondition> condAB;
	JoinCondition joinConditionAB;
	joinConditionAB.left = make_unique<BoundReferenceExpression>("a_id_rowid", TypeId::INT64, 2);
	joinConditionAB.right = make_unique<BoundReferenceExpression>("a_rowid", TypeId::INT64, 1);
	joinConditionAB.comparison = ExpressionType::COMPARE_EQUAL;

	auto raiInfoAB = make_unique<RAIInfo>();
	raiInfoAB->rai = table_b->storage->info->rais[0].get();
	raiInfoAB->rai_type = RAIType::EDGE_SOURCE;
	raiInfoAB->forward = true;
	raiInfoAB->vertex = table_a;
	raiInfoAB->vertex_id = table_index_a;
	raiInfoAB->passing_tables[0] = table_index_b;
	raiInfoAB->left_cardinalities[0] = table_b->storage->info->cardinality;
	raiInfoAB->passing_tables[1] = table_index_c;
	raiInfoAB->left_cardinalities[1] = table_c->storage->info->cardinality;
	raiInfoAB->compact_list = &raiInfoAB->rai->alist->compact_forward_list;

	joinConditionAB.rais.push_back(move(raiInfoAB));
	condAB.push_back(move(joinConditionAB));

	LogicalComparisonJoin joinOp(JoinType::INNER);
	vector<TypeId> outputTypes{TypeId::INT32, TypeId::INT32, TypeId::INT64, TypeId::INT64, TypeId::INT32,
	                           TypeId::INT64, TypeId::INT32, TypeId::INT64};
	joinOp.types = outputTypes;
	auto sipJoinAB = make_unique<PhysicalSIPJoin>(context, joinOp, move(sipJoinBC),
	                                              move(scan_function_a), move(condAB), joinOp.join_type,
	                                              left_projection_map, right_projection_map);

	vector<TypeId> result_types{TypeId::INT32, TypeId::INT32, TypeId::INT32, TypeId::INT64, TypeId::INT64, TypeId::INT32};
	vector<unique_ptr<Expression>> select_list;
	auto result_col0 = make_unique<BoundReferenceExpression>("id", TypeId::INT32, 6);
	auto result_col1 = make_unique<BoundReferenceExpression>("a_id", TypeId::INT32, 0);
	auto result_col2 = make_unique<BoundReferenceExpression>("c_id", TypeId::INT32, 1);
	auto result_col3 = make_unique<BoundReferenceExpression>("a_id_rowid", TypeId::INT64, 2);
	auto result_col4 = make_unique<BoundReferenceExpression>("c_id_rowid", TypeId::INT64, 3);
	auto result_col5 = make_unique<BoundReferenceExpression>("id", TypeId::INT32, 4);
	select_list.push_back(move(result_col0));
	select_list.push_back(move(result_col1));
	select_list.push_back(move(result_col2));
	select_list.push_back(move(result_col3));
	select_list.push_back(move(result_col4));
	select_list.push_back(move(result_col5));
	auto projection = make_unique<PhysicalProjection>(result_types, move(select_list));
	projection->children.push_back(move(sipJoinAB));

	// execute!
	DataChunk result_chunk;
	result_chunk.Initialize(projection->types);
	auto state = projection->GetOperatorState();

	std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> start =
	    std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

	do {
		projection->GetChunk(*con.context, result_chunk, state.get());
		result_chunk.Print();
	} while (result_chunk.size() > 0);

	std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> end =
	    std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

	std::cout << "Execution Time Cost: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
}

int main() {
	// (a) -[b]
	// test_trivial_ab();

	// (a)-[b]-(c)
	// test_trivial_abc();

	// (a)-[b]-(c) with sjoin and merge join
	test_sjoin_merge_abc();
}