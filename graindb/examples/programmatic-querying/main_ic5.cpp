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
#include "duckdb/execution/operator/join/physical_merge_sip_join.hpp"

#include <unordered_set>
#include <string>
#include <chrono>
#include <iostream>
#include <fstream>

/**
This file contains an example on how a query tree can be programmatically constructed. This is essentially hand-rolling
the binding+planning phase for one specific query.

Note that this API is currently very unstable, and is subject to change at any moment. In general, this API should not
be used currently outside of internal use cases.
**/

using namespace duckdb;
using namespace std;


void getStringListFromFile(string filename, int index, int count, vector<string>& slist) {
	fstream infile(filename, std::ios::in);
	string schema, data;
	std::getline(infile, schema);
	std::cout << schema << std::endl;
	char delimiter = '|';

	unordered_set<string> record;

	string result = "";
	while (std::getline(infile, data)) {
		int pos = 0;
		int last = 0;
		int indexer = 0;

		while ((pos = data.find(delimiter, last)) != std::string::npos) {
			if (index == indexer) {
				string token = data.substr(last, pos - last);
				if (record.find(token) == record.end()) {
					slist.push_back(token);
					record.insert(token);
				}
				break;
			}
			indexer += 1;
			last = pos + 1;
		}
		if (index == indexer) {
			string token = data.substr(last, pos - last);
			if (record.find(token) == record.end()) {
				slist.push_back(token);
				record.insert(token);
			}
		}

		if (slist.size() >= count)
			break;
	}
}

void extractInfo(string& inputstr, vector<bool>& filter, char delimiter, string& result) {
	int pos = 0;
	int last = 0;
	int indexer = 0;
	std::string token;

	while ((pos = inputstr.find(delimiter, last)) != std::string::npos) {
		if (filter[indexer]) {
			token = inputstr.substr(last, pos - last);
			result += "\'" + token + "\'" + ',';
		}
		indexer += 1;
		last = pos + 1;
	}
	if (filter[indexer]) {
		result += "\'" + inputstr.substr(last) + "\'";
	}

	int lastindex = result.size() - 1;
	if (result[lastindex] == ',') {
		result = result.substr(0, lastindex);
	}
}

void extractInfoFile(Connection& con, string filename, string tablename, vector<bool>& filter) {
	fstream infile(filename, std::ios::in);
	string schema, data;
	std::getline(infile, schema);
	std::cout << schema << std::endl;

	string result = "";
	while (std::getline(infile, data)) {
		result.clear();
		extractInfo(data, filter, '|', result);
		con.Query("INSERT INTO " + tablename + " VALUES (" + result + ")");
		data.clear();
		// cout << "INSERT INTO " + tablename + " VALUES (" + result + ");" << endl;
	}

	infile.close();
}

void CreateGraphFromFile(Connection & con) {
	vector<string> table_names{"Person", "Forum", "Post", "Knows", "HasMember", "ContainerOf", "HasCreator"};
	for (int i = 0; i < table_names.size(); ++i) {
		con.Query("DROP TABLE " + table_names[i]);
	}

	con.Query("CREATE TABLE Person(id STRING)");
	con.Query("CREATE TABLE Forum(id STRING, title STRING)");
	con.Query("CREATE TABLE Post(id STRING)");
	con.Query("CREATE TABLE Knows(id1 STRING, id2 STRING)");
	con.Query("CREATE TABLE HasMember(forumId STRING, personId STRING)");
	con.Query("CREATE TABLE ContainerOf(forumId STRING, postId STRING)");
	con.Query("CREATE TABLE HasCreator(postId STRING, personId STRING)");

	vector<bool> filter_person{true, false, false, false, false, false, false, false};
	extractInfoFile(con, "../../../resource/sample/person_0_0.csv", "Person", filter_person);

	vector<bool> filter_forum{true, true, false};
	extractInfoFile(con, "../../../resource/sample/forum_0_0.csv", "Forum", filter_forum);

	vector<bool> filter_post{true, false, false, false, false, false, false, false};
	extractInfoFile(con, "../../../resource/sample/post_0_0.csv", "Post", filter_post);

	vector<bool> filter_knows{true, true, false};
	extractInfoFile(con, "../../../resource/sample/person_knows_person_0_0.csv", "Knows", filter_knows);

	vector<bool> filter_hasmember{true, true, false};
	extractInfoFile(con, "../../../resource/sample/forum_hasMember_person_0_0.csv", "HasMember", filter_hasmember);

	vector<bool> filter_containerof{true, true};
	extractInfoFile(con, "../../../resource/sample/forum_containerOf_post_0_0.csv", "ContainerOf", filter_containerof);

	vector<bool> filter_hascreator{true, true};
	extractInfoFile(con, "../../../resource/sample/post_hasCreator_person_0_0.csv", "HasCreator", filter_hascreator);
}

void CreateGraph(Connection& con) {
	vector<string> table_names{"Person", "Forum", "Post", "Knows", "HasMember", "ContainerOf", "HasCreator"};
	for (int i = 0; i < table_names.size(); ++i) {
		con.Query("DROP TABLE " + table_names[i]);
	}

	con.Query("CREATE TABLE Person(id STRING)");
	con.Query("CREATE TABLE Forum(id STRING, title STRING)");
	con.Query("CREATE TABLE Post(id STRING)");
	con.Query("CREATE TABLE Knows(id1 STRING, id2 STRING)");
	con.Query("CREATE TABLE HasMember(forumId STRING, personId STRING)");
	con.Query("CREATE TABLE ContainerOf(forumId STRING, postId STRING)");
	con.Query("CREATE TABLE HasCreator(postId STRING, personId STRING)");

	int S_NUM = 3;
	for (int i = 0; i < S_NUM; ++i) {
		con.Query("INSERT INTO Person VALUES (\'" + to_string(i) +"\')");
		con.Query("INSERT INTO Forum VALUES (\'" + to_string(i) + "\',\'" + to_string(i) + "\')");
		con.Query("INSERT INTO Post VALUES (\'" + to_string(i) + "\')");
		con.Query("INSERT INTO HasMember VALUES (\'" + to_string(i) + "\',\'" + to_string(i) + "\')");
		con.Query("INSERT INTO ContainerOf VALUES (\'" + to_string(i) + "\',\'" + to_string(i) + "\')");
		con.Query("INSERT INTO HasCreator VALUES (\'" + to_string(i) + "\',\'" + to_string(i) + "\')");
	}

	for (int i = 0; i < S_NUM; ++i) {
		for (int j = 0; j < S_NUM; ++j) {
			if (i == j)
				continue;
			con.Query("INSERT INTO Knows VALUES (\'" + to_string(i) + "\',\'" + to_string(j) + "\')");
		}
	}
}

void buildIndex(Connection& con) {
	con.Query("CREATE UNDIRECTED RAI knows_rai ON Knows (FROM id1 REFERENCES Person.id, TO id2 REFERENCES Person.id)");
	con.Query("CREATE UNDIRECTED RAI hasMember_rai ON HasMember (FROM forumId REFERENCES Forum.id, TO personId REFERENCES Person.id)");
	// con.Query("CREATE RAI hasMember_rai_back ON HasMember (FROM personId REFERENCES Person.id, TO forumId REFERENCES Forum.id)");
	con.Query("CREATE UNDIRECTED RAI containerOf_rai ON ContainerOf (FROM forumId REFERENCES Forum.id, TO postId REFERENCES Post.id)");
	// con.Query("CREATE RAI containerOf_rai_back ON ContainerOf (FROM postId REFERENCES Post.id, TO forumId REFERENCES Forum.id)");
	con.Query("CREATE UNDIRECTED RAI hasCreator_rai ON HasCreator (FROM postId REFERENCES Post.id, TO personId REFERENCES Person.id)");
	// con.Query("CREATE RAI hasCreator_rai_back ON HasCreator (FROM personId REFERENCES Person.id, TO postId REFERENCES Post.id)");
}

unique_ptr<PhysicalTableScan> get_scan_function(ClientContext& context, string& table_name, idx_t table_index,
                                                vector<idx_t>& columns_ids, vector<TypeId>& getTypes,
                                                unordered_map<idx_t, vector<TableFilter>> table_filters = unordered_map<idx_t, vector<TableFilter>>()) {

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

void test_graindb_hashjoin() {
	DuckDB db(nullptr);
	Connection con(db);
	con.DisableProfiling();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	auto &context = *con.context;

	CreateGraph(con);

	auto result = con.Query("SELECT * FROM ");

	con.context->transaction.Commit();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	con.Query("CREATE RAI b_rai ON b (FROM a_id REFERENCES a.id, TO c_id REFERENCES c.id)");

	// select * from a, b, c where a.id = b.a_id and b.c_id = c.id;

	con.Query("CREATE TABLE Person(id INTEGER)");
	con.Query("CREATE TABLE Forum(id INTEGER, title STRING)");
	con.Query("CREATE TABLE Post(id INTEGER)");
	con.Query("CREATE TABLE Knows(id1 INTEGER, id2 INTEGER)");
	con.Query("CREATE TABLE HasMember(forumId INTEGER, personId INTEGER)");
	con.Query("CREATE TABLE ContainerOf(forumId INTEGER, postId INTEGER)");
	con.Query("CREATE TABLE HasCreator(postId INTEGER, personId INTEGER)");

	vector<idx_t> column_ids_a{0, COLUMN_IDENTIFIER_ROW_ID};
	vector<idx_t> column_ids_b{0, 1, 2, 3};
	vector<idx_t> column_ids_c{0, COLUMN_IDENTIFIER_ROW_ID};

	vector<idx_t> left_projection_map, right_projection_map;

	string table_vertex_person = "person";
	string table_vertex_forum = "forum";
	string table_vertex_post = "post";
	string table_edge_knows = "knows";
	string table_edge_hasmember = "hasmember";
	string table_edge_containerof = "containerof";
	string table_edge_hascreator = "hascreator";
	idx_t table_index_person1 = 6;
	idx_t table_index_forum = 7;
	idx_t table_index_person2 = 8;
	idx_t table_index_post = 9;
	idx_t table_index_knows = 10;
	idx_t table_index_hasmember = 11;
	idx_t table_index_containerof = 12;
	idx_t table_index_hascreator = 13;

	auto table_or_view_person =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_vertex_person);
	auto table_person = (TableCatalogEntry *)table_or_view_person;

	auto table_or_view_forum =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_vertex_forum);
	auto table_forum = (TableCatalogEntry *)table_or_view_forum;

	auto table_or_view_post=
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_vertex_post);
	auto table_post = (TableCatalogEntry *)table_or_view_post;

	auto table_or_view_knows =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_edge_knows);
	auto table_knows = (TableCatalogEntry *)table_or_view_knows;

	auto table_or_view_hasmember =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_edge_hasmember);
	auto table_hasmember = (TableCatalogEntry *)table_or_view_hasmember;

	auto table_or_view_containerof =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_edge_containerof);
	auto table_containerof = (TableCatalogEntry *)table_or_view_containerof;

	auto table_or_view_hascreator =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_edge_hascreator);
	auto table_hascreator = (TableCatalogEntry *)table_or_view_hascreator;


	// join post with hascreator
	vector<idx_t> post_ids{0};
	vector<TypeId> get_post_types{TypeId::VARCHAR};
	auto scan_function_post = move(get_scan_function(context, table_vertex_post,
	                                                table_index_post, post_ids, get_post_types));

	vector<idx_t> hascreator_ids{0, 1};
	vector<TypeId> get_hascreator_types{TypeId::VARCHAR, TypeId::VARCHAR};
	auto scan_function_hascreator = move(get_scan_function(context, table_edge_hascreator,
	                                                 table_index_hascreator, post_ids, get_hascreator_types));

	vector<JoinCondition> cond_post;
	JoinCondition join_condition_post;
	join_condition_post.left = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 0);
	join_condition_post.right = make_unique<BoundReferenceExpression>("postid", TypeId::VARCHAR, 0);
	join_condition_post.comparison = ExpressionType::COMPARE_EQUAL;

	cond_post.push_back(move(join_condition_post));

	LogicalComparisonJoin join_post_op(JoinType::INNER);
	vector<TypeId> output_post_types{TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR};
	join_post_op.types = output_post_types;
	auto join_post = make_unique<PhysicalHashJoin>(context, join_post_op, move(scan_function_post),
	                                              move(scan_function_hascreator), move(cond_post), join_post_op.join_type,
	                                              left_projection_map, right_projection_map);


	// join containerof with existing
	vector<idx_t> cof_ids{0, 1};
	vector<TypeId> get_cof_types{TypeId::VARCHAR, TypeId::VARCHAR};
	auto scan_function_cof = move(get_scan_function(context, table_edge_containerof,
	                                                  table_index_containerof, cof_ids, get_cof_types));

	vector<JoinCondition> cond_cof;
	JoinCondition join_condition_cof1, join_condition_cof2;
	join_condition_cof1.left = make_unique<BoundReferenceExpression>("postid", TypeId::VARCHAR, 1);
	join_condition_cof1.right = make_unique<BoundReferenceExpression>("postid", TypeId::VARCHAR, 1);
	join_condition_cof1.comparison = ExpressionType::COMPARE_EQUAL;

	join_condition_cof2.left = make_unique<BoundReferenceExpression>("postid", TypeId::VARCHAR, 1);
	join_condition_cof2.right = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 0);
	join_condition_cof2.comparison = ExpressionType::COMPARE_EQUAL;

	cond_cof.push_back(move(join_condition_cof1));
	cond_cof.push_back(move(join_condition_cof2));

	LogicalComparisonJoin join_cof_op(JoinType::INNER);
	vector<TypeId> output_cof_types{TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR};
	join_cof_op.types = output_cof_types;
	auto join_cof = make_unique<PhysicalHashJoin>(context, join_cof_op, move(scan_function_cof),
	                                                move(join_post), move(cond_cof), join_cof_op.join_type,
	                                                left_projection_map, right_projection_map);


	// join forum with existing
	vector<idx_t> forum_ids{0, 1};
	vector<TypeId> get_forum_types{TypeId::VARCHAR, TypeId::VARCHAR};
	auto scan_function_forum = move(get_scan_function(context, table_vertex_forum,
	                                                      table_index_forum, forum_ids, get_forum_types));

	vector<JoinCondition> cond_forum;
	JoinCondition join_condition_forum;
	join_condition_forum.left = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 0);
	join_condition_forum.right = make_unique<BoundReferenceExpression>("forumid", TypeId::VARCHAR, 3);
	join_condition_forum.comparison = ExpressionType::COMPARE_EQUAL;

	cond_forum.push_back(move(join_condition_forum));

	LogicalComparisonJoin join_forum_op(JoinType::INNER);
	vector<TypeId> output_forum_types{TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR};
	vector<idx_t> right_projection_map_forum{0, 4};
	join_forum_op.types = output_forum_types;
	auto join_forum = make_unique<PhysicalHashJoin>(context, join_forum_op, move(scan_function_forum),
	                                                    move(join_cof), move(cond_forum), join_forum_op.join_type,
	                                                    left_projection_map, right_projection_map_forum);


	// join hasmember with existing
	vector<idx_t> hasmember_ids{1, 0};
	vector<TypeId> get_hasmember_types{TypeId::VARCHAR, TypeId::VARCHAR};
	auto scan_function_hasmember = move(get_scan_function(context, table_edge_hasmember,
	                                                    table_index_hasmember, hasmember_ids, get_hasmember_types));

	vector<JoinCondition> cond_hasmember;
	JoinCondition join_condition_hasmember1, join_condition_hasmember2, join_condition_hasmember3;
	join_condition_hasmember1.left = make_unique<BoundReferenceExpression>("personid", TypeId::VARCHAR, 0);
	join_condition_hasmember1.right = make_unique<BoundReferenceExpression>("personid", TypeId::VARCHAR, 3);
	join_condition_hasmember1.comparison = ExpressionType::COMPARE_EQUAL;

	join_condition_hasmember2.left = make_unique<BoundReferenceExpression>("forumid", TypeId::VARCHAR, 1);
	join_condition_hasmember2.right = make_unique<BoundReferenceExpression>("forumid", TypeId::VARCHAR, 2);
	join_condition_hasmember2.comparison = ExpressionType::COMPARE_EQUAL;

	join_condition_hasmember3.left = make_unique<BoundReferenceExpression>("forumid", TypeId::VARCHAR, 1);
	join_condition_hasmember3.right = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 0);
	join_condition_hasmember3.comparison = ExpressionType::COMPARE_EQUAL;

	cond_hasmember.push_back(move(join_condition_hasmember1));
	cond_hasmember.push_back(move(join_condition_hasmember2));
	cond_hasmember.push_back(move(join_condition_hasmember3));

	LogicalComparisonJoin join_hasmember_op(JoinType::INNER);
	vector<TypeId> output_hasmember_types{TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR};
	join_hasmember_op.types = output_hasmember_types;
	auto join_hasmember = make_unique<PhysicalHashJoin>(context, join_hasmember_op, move(scan_function_hasmember),
	                                                  move(join_forum), move(cond_hasmember), join_hasmember_op.join_type,
	                                                  left_projection_map, right_projection_map);

	// join person2 with exisitng
	vector<idx_t> person2_ids{0, COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_person2_types{TypeId::VARCHAR, TypeId::INT64};
	auto scan_function_person2 = move(get_scan_function(context, table_vertex_person,
	                                                    table_index_person2, person2_ids, get_person2_types));

	vector<JoinCondition> cond_person2;
	JoinCondition join_condition_person21, join_condition_person22;
	join_condition_person21.left = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 0);
	join_condition_person21.right = make_unique<BoundReferenceExpression>("personid", TypeId::VARCHAR, 5);
	join_condition_person21.comparison = ExpressionType::COMPARE_EQUAL;

	join_condition_person22.left = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 0);
	join_condition_person22.right = make_unique<BoundReferenceExpression>("personid", TypeId::VARCHAR, 0);
	join_condition_person22.comparison = ExpressionType::COMPARE_EQUAL;

	cond_person2.push_back(move(join_condition_person21));
	cond_person2.push_back(move(join_condition_person22));

	LogicalComparisonJoin join_person2_op(JoinType::INNER);
	vector<TypeId> output_person2_types{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64, TypeId::INT64, TypeId::VARCHAR,
	                                  TypeId::VARCHAR, TypeId::VARCHAR};
	vector<idx_t> right_projection_map_person2{0, 3, 5};
	join_person2_op.types = output_person2_types;
	auto join_person2 = make_unique<PhysicalHashJoin>(context, join_person2_op, move(scan_function_person2),
	                                               move(join_hasmember), move(cond_person2), join_person2_op.join_type,
	                                               left_projection_map, right_projection_map_person2);

	// knows join the previous results, including person2, hasMember and hasCreator
	vector<idx_t> knows_ids{1, 3, 2};
	vector<TypeId> get_knows_types{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64};
	auto scan_function_knows = move(get_scan_function(context, table_vertex_person,
	                                                   table_index_knows, knows_ids, get_knows_types));

	vector<JoinCondition> cond_knows;
	JoinCondition join_condition_knows1, join_condition_knows2, join_condition_knows3;
	join_condition_knows1.left = make_unique<BoundReferenceExpression>("id2_rowid", TypeId::INT64, 1);
	join_condition_knows1.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 1);
	join_condition_knows1.comparison = ExpressionType::COMPARE_EQUAL;

	join_condition_knows2.left = make_unique<BoundReferenceExpression>("id2", TypeId::VARCHAR, 0);
	join_condition_knows2.right = make_unique<BoundReferenceExpression>("personid", TypeId::VARCHAR, 4);
	join_condition_knows2.comparison = ExpressionType::COMPARE_EQUAL;

	join_condition_knows3.left = make_unique<BoundReferenceExpression>("id2", TypeId::VARCHAR, 0);
	join_condition_knows3.right = make_unique<BoundReferenceExpression>("personid", TypeId::INT64, 2);
	join_condition_knows3.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_knows = make_unique<RAIInfo>();
	rai_info_knows->rai = table_knows->storage->info->rais[0].get();
	rai_info_knows->rai_type = RAIType::EDGE_TARGET;
	rai_info_knows->forward = true;
	rai_info_knows->vertex = table_person;
	rai_info_knows->vertex_id = table_index_person2;
	rai_info_knows->left_cardinalities[0] = table_knows->storage->info->cardinality;

	join_condition_knows1.rais.push_back(move(rai_info_knows));
	cond_knows.push_back(move(join_condition_knows1));
	cond_knows.push_back(move(join_condition_knows2));
	cond_knows.push_back(move(join_condition_knows3));

	LogicalComparisonJoin join_knows_op(JoinType::INNER);
	vector<TypeId> output_knows_types{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64, TypeId::INT64, TypeId::VARCHAR,
	                           TypeId::VARCHAR, TypeId::VARCHAR};
	vector<idx_t> right_projection_map_knows{1, 2, 3, 4};
	join_knows_op.types = output_knows_types;
	auto join_knows = make_unique<PhysicalSIPJoin>(context, join_knows_op, move(scan_function_knows),
	                                                      move(join_person2), move(cond_knows), join_knows_op.join_type,
	                                                      left_projection_map, right_projection_map_knows);

	// person join knows
	vector<JoinCondition> cond_person_knows;
	JoinCondition join_condition_person_knows;
	join_condition_person_knows.left = make_unique<BoundReferenceExpression>("id1_rowid", TypeId::INT64, 2);
	join_condition_person_knows.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 0);
	join_condition_person_knows.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_person_knows = make_unique<RAIInfo>();
	rai_info_person_knows->rai = table_knows->storage->info->rais[0].get();
	rai_info_person_knows->rai_type = RAIType::EDGE_SOURCE;
	rai_info_person_knows->forward = true;
	rai_info_person_knows->vertex = table_person;
	rai_info_person_knows->vertex_id = table_index_person1;
	rai_info_person_knows->passing_tables[0] = table_index_knows;
	rai_info_person_knows->left_cardinalities[0] = table_knows->storage->info->cardinality;
	rai_info_person_knows->passing_tables[1] = table_index_person2;
	rai_info_person_knows->left_cardinalities[1] = table_person->storage->info->cardinality;
	rai_info_person_knows->compact_list = &rai_info_person_knows->rai->alist->compact_forward_list;

	join_condition_person_knows.rais.push_back(move(rai_info_person_knows));
	cond_person_knows.push_back(move(join_condition_person_knows));

	vector<idx_t> person_ids{COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_person_types{TypeId::INT64};
	auto scan_function_person = move(get_scan_function(context, table_vertex_person,
	                                                   table_index_person1, person_ids, get_person_types));
	LogicalComparisonJoin joinOp(JoinType::INNER);
	vector<TypeId> outputTypes{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64, TypeId::INT64, TypeId::VARCHAR,
	                           TypeId::VARCHAR, TypeId::VARCHAR, TypeId::INT64};
	joinOp.types = outputTypes;
	auto join_person_knows = make_unique<PhysicalSIPJoin>(context, joinOp, move(join_knows),
	                                              move(scan_function_person), move(cond_person_knows), joinOp.join_type,
	                                              left_projection_map, right_projection_map);

	// project
	vector<TypeId> result_types{TypeId::VARCHAR};
	vector<unique_ptr<Expression>> select_list;
	auto result_col0 = make_unique<BoundReferenceExpression>("title", TypeId::VARCHAR, 5);
	select_list.push_back(move(result_col0));
	auto projection = make_unique<PhysicalProjection>(result_types, move(select_list));
	projection->children.push_back(move(join_person_knows));

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

void test_graindb_smergejoin() {
	DuckDB db(nullptr);
	Connection con(db);
	con.DisableProfiling();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	auto &context = *con.context;

	// CreateGraph(con);
	CreateGraphFromFile(con);

	con.context->transaction.Commit();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	// select * from a, b, c where a.id = b.a_id and b.c_id = c.id;

	vector<idx_t> left_projection_map, right_projection_map;

	string table_vertex_person = "person";
	string table_vertex_forum = "forum";
	string table_vertex_post = "post";
	string table_edge_knows = "knows";
	string table_edge_hasmember = "hasmember";
	string table_edge_containerof = "containerof";
	string table_edge_hascreator = "hascreator";
	idx_t table_index_person1 = 6;
	idx_t table_index_forum = 10;
	idx_t table_index_person2 = 8;
	idx_t table_index_post = 12;
	idx_t table_index_knows = 7;
	idx_t table_index_hasmember = 9;
	idx_t table_index_containerof = 11;
	idx_t table_index_hascreator = 13;

	buildIndex(con);

	auto table_or_view_person =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_vertex_person);
	auto table_person = (TableCatalogEntry *)table_or_view_person;

	auto table_or_view_forum =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_vertex_forum);
	auto table_forum = (TableCatalogEntry *)table_or_view_forum;

	auto table_or_view_post=
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_vertex_post);
	auto table_post = (TableCatalogEntry *)table_or_view_post;

	auto table_or_view_knows =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_knows);
	auto table_knows = (TableCatalogEntry *)table_or_view_knows;

	auto table_or_view_hasmember =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_hasmember);
	auto table_hasmember = (TableCatalogEntry *)table_or_view_hasmember;

	auto table_or_view_containerof =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_containerof);
	auto table_containerof = (TableCatalogEntry *)table_or_view_containerof;

	auto table_or_view_hascreator =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_hascreator);
	auto table_hascreator = (TableCatalogEntry *)table_or_view_hascreator;


	// join post with hascreator
	vector<idx_t> post_ids{COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_post_types{TypeId::INT64};
	auto scan_function_post = move(get_scan_function(context, table_vertex_post,
	                                                 table_index_post, post_ids, get_post_types));

	vector<idx_t> hascreator_ids{2, 3};
	vector<TypeId> get_hascreator_types{TypeId::INT64, TypeId::INT64};
	auto scan_function_hascreator = move(get_scan_function(context, table_edge_hascreator,
	                                                       table_index_hascreator, hascreator_ids, get_hascreator_types));

	vector<JoinCondition> cond_post;
	JoinCondition join_condition_post;
	join_condition_post.left = make_unique<BoundReferenceExpression>("post_rowid", TypeId::INT64, 0);
	join_condition_post.right = make_unique<BoundReferenceExpression>("postid_rowid", TypeId::INT64, 0);
	join_condition_post.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_post = make_unique<RAIInfo>();
	rai_info_post->rai = table_hascreator->storage->info->rais[0].get(); // 2d40
	rai_info_post->rai_type = RAIType::SOURCE_EDGE;
	rai_info_post->forward = false;
	rai_info_post->vertex = table_post;
	rai_info_post->vertex_id = table_index_post;
	rai_info_post->passing_tables[0] = table_index_post;
	rai_info_post->left_cardinalities[0] = table_post->storage->info->cardinality;

	join_condition_post.rais.push_back(move(rai_info_post));
	cond_post.push_back(move(join_condition_post));

	LogicalComparisonJoin join_post_op(JoinType::INNER);
	vector<TypeId> output_post_types{TypeId::INT64, TypeId::INT64, TypeId::INT64};
	join_post_op.types = output_post_types;
	auto join_post = make_unique<PhysicalSIPJoin>(context, join_post_op, move(scan_function_post),
	                                               move(scan_function_hascreator), move(cond_post), join_post_op.join_type,
	                                               left_projection_map, right_projection_map);


	// join forum with existing
	vector<idx_t> forum_ids{1, COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_forum_types{TypeId::VARCHAR, TypeId::INT64};
	auto scan_function_forum = move(get_scan_function(context, table_vertex_forum,
	                                                  table_index_forum, forum_ids, get_forum_types));

	vector<JoinCondition> cond_forum;
	JoinCondition join_condition_forum;
	join_condition_forum.left = make_unique<BoundReferenceExpression>("forum_rowid", TypeId::INT64, 1);
	join_condition_forum.right = make_unique<BoundReferenceExpression>("post_rowid", TypeId::INT64, 0);
	join_condition_forum.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_forum = make_unique<RAIInfo>();
	rai_info_forum->rai = table_containerof->storage->info->rais[0].get(); // 33040
	rai_info_forum->rai_type = RAIType::SOURCE_EDGE;
	rai_info_forum->forward = false;
	rai_info_forum->vertex = table_forum;
	rai_info_forum->vertex_id = table_index_forum;
	rai_info_forum->passing_tables[0] = table_index_forum;
	rai_info_forum->left_cardinalities[0] = table_forum->storage->info->cardinality;
	rai_info_forum->compact_list = &rai_info_forum->rai->alist->compact_backward_list; // 20b8

	join_condition_forum.rais.push_back(move(rai_info_forum));

	cond_forum.push_back(move(join_condition_forum));

	LogicalComparisonJoin join_forum_op(JoinType::INNER);
	vector<TypeId> output_forum_types{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64, TypeId::INT64};
	vector<idx_t> right_projection_map_forum{0, 2}; // reserve column 0 and 2 in the right table, and drop the other columns to join
	vector<idx_t> merge_projection_map;
	join_forum_op.types = output_forum_types;
	auto join_forum = make_unique<PhysicalMergeSIPJoin>(context, join_forum_op, move(scan_function_forum),
	                                                move(join_post), move(cond_forum), join_forum_op.join_type,
	                                                left_projection_map, right_projection_map_forum, merge_projection_map);


	// join hasmember with existing
	vector<idx_t> hasmember_ids{1, 2};
	vector<TypeId> get_hasmember_types{TypeId::VARCHAR, TypeId::INT64};
	auto scan_function_hasmember = move(get_scan_function(context, table_edge_hasmember,
	                                                      table_index_hasmember, hasmember_ids, get_hasmember_types));

	vector<JoinCondition> cond_hasmember;
	JoinCondition join_condition_hasmember;
	join_condition_hasmember.left = make_unique<BoundReferenceExpression>("forumid_rowid", TypeId::INT64, 1);
	join_condition_hasmember.right = make_unique<BoundReferenceExpression>("forum_rowid", TypeId::INT64, 1);
	join_condition_hasmember.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_hasmember = make_unique<RAIInfo>();
	rai_info_hasmember->rai = table_hasmember->storage->info->rais[0].get(); // 50c0
	rai_info_hasmember->rai_type = RAIType::EDGE_SOURCE;
	rai_info_hasmember->forward = true;
	rai_info_hasmember->vertex = table_forum;
	rai_info_hasmember->vertex_id = table_index_forum;
	rai_info_hasmember->passing_tables[0] = table_index_hasmember;
	rai_info_hasmember->left_cardinalities[0] = table_hasmember->storage->info->cardinality;
	rai_info_hasmember->compact_list = &rai_info_hasmember->rai->alist->compact_forward_list; // c2d8

	join_condition_hasmember.rais.push_back(move(rai_info_hasmember));

	cond_hasmember.push_back(move(join_condition_hasmember));

	LogicalComparisonJoin join_hasmember_op(JoinType::INNER);
	vector<TypeId> output_hasmember_types{TypeId::VARCHAR, TypeId::INT64, TypeId::VARCHAR, TypeId::INT64, TypeId::INT64};
	vector<idx_t> right_projection_map_hasmember{0, 1, 3};
	join_hasmember_op.types = output_hasmember_types;
	auto join_hasmember = make_unique<PhysicalSIPJoin>(context, join_hasmember_op, move(scan_function_hasmember),
	                                                    move(join_forum), move(cond_hasmember), join_hasmember_op.join_type,
	                                                    left_projection_map, right_projection_map_hasmember);

	// join person2 with exisitng
	vector<idx_t> person2_ids{0, COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_person2_types{TypeId::VARCHAR, TypeId::INT64};
	auto scan_function_person2 = move(get_scan_function(context, table_vertex_person,
	                                                    table_index_person2, person2_ids, get_person2_types));

	vector<JoinCondition> cond_person2;
	JoinCondition join_condition_person21, join_condition_person22;
	join_condition_person21.left = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 1);
	join_condition_person21.right = make_unique<BoundReferenceExpression>("personid_rowid", TypeId::INT64, 4);
	join_condition_person21.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_person21 = make_unique<RAIInfo>();
	rai_info_person21->rai = table_hascreator->storage->info->rais[0].get(); // 2d40
	rai_info_person21->rai_type = RAIType::TARGET_EDGE;
	rai_info_person21->forward = true;
	rai_info_person21->vertex = table_person;
	rai_info_person21->vertex_id = table_index_person2;
	rai_info_person21->passing_tables[0] = table_index_person2;
	rai_info_person21->left_cardinalities[0] = table_person->storage->info->cardinality;

	join_condition_person21.rais.push_back(move(rai_info_person21));

	join_condition_person22.left = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 0);
	join_condition_person22.right = make_unique<BoundReferenceExpression>("personid", TypeId::VARCHAR, 0); // flag
	join_condition_person22.comparison = ExpressionType::COMPARE_EQUAL;

	cond_person2.push_back(move(join_condition_person21));
	cond_person2.push_back(move(join_condition_person22));

	LogicalComparisonJoin join_person2_op(JoinType::INNER);
	vector<TypeId> output_person2_types{TypeId::VARCHAR, TypeId::INT64, TypeId::VARCHAR, TypeId::VARCHAR, TypeId::INT64};
	vector<idx_t> right_projection_map_person2{0, 2, 4};
	join_person2_op.types = output_person2_types;
	auto join_person2 = make_unique<PhysicalSIPJoin>(context, join_person2_op, move(scan_function_person2),
	                                                  move(join_hasmember), move(cond_person2), join_person2_op.join_type,
	                                                  left_projection_map, right_projection_map_person2);

	// knows join the previous results, including person2, hasMember and hasCreator
	vector<idx_t> knows_ids{3, 2};
	vector<TypeId> get_knows_types{TypeId::INT64, TypeId::INT64};
	auto scan_function_knows = move(get_scan_function(context, table_edge_knows,
	                                                  table_index_knows, knows_ids, get_knows_types));

	vector<JoinCondition> cond_knows;
	JoinCondition join_condition_knows;
	join_condition_knows.left = make_unique<BoundReferenceExpression>("id2_rowid", TypeId::INT64, 0);
	join_condition_knows.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 1);
	join_condition_knows.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_knows = make_unique<RAIInfo>();
	rai_info_knows->rai = table_knows->storage->info->rais[0].get(); // 3640
	rai_info_knows->rai_type = RAIType::EDGE_TARGET;
	rai_info_knows->forward = false;
	rai_info_knows->vertex = table_person;
	rai_info_knows->vertex_id = table_index_person2;
	rai_info_knows->passing_tables[0] = table_index_knows;
	rai_info_knows->left_cardinalities[0] = table_knows->storage->info->cardinality;
	rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list; // e4b8

	join_condition_knows.rais.push_back(move(rai_info_knows));
	cond_knows.push_back(move(join_condition_knows));

	LogicalComparisonJoin join_knows_op(JoinType::INNER);
	vector<TypeId> output_knows_types{TypeId::INT64, TypeId::INT64, TypeId::INT64, TypeId::VARCHAR};
	vector<idx_t> right_projection_map_knows{1, 3};
	join_knows_op.types = output_knows_types;
	auto join_knows = make_unique<PhysicalSIPJoin>(context, join_knows_op, move(scan_function_knows),
	                                               move(join_person2), move(cond_knows), join_knows_op.join_type,
	                                               left_projection_map, right_projection_map_knows);

	// person join knows
	vector<idx_t> person_ids{COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_person_types{TypeId::INT64};
	auto scan_function_person = move(get_scan_function(context, table_vertex_person,
	                                                   table_index_person1, person_ids, get_person_types));

	vector<JoinCondition> cond_person_knows;
	JoinCondition join_condition_person_knows;
	join_condition_person_knows.left = make_unique<BoundReferenceExpression>("id1_rowid", TypeId::INT64, 1);
	join_condition_person_knows.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 0);
	join_condition_person_knows.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_person_knows = make_unique<RAIInfo>();
	rai_info_person_knows->rai = table_knows->storage->info->rais[0].get();  // 3640
	rai_info_person_knows->rai_type = RAIType::EDGE_SOURCE;
	rai_info_person_knows->forward = true;
	rai_info_person_knows->vertex = table_person;
	rai_info_person_knows->vertex_id = table_index_person1;
	rai_info_person_knows->passing_tables[0] = table_index_knows;
	rai_info_person_knows->left_cardinalities[0] = table_knows->storage->info->cardinality;
	rai_info_person_knows->compact_list = &rai_info_person_knows->rai->alist->compact_forward_list; //e498

	join_condition_person_knows.rais.push_back(move(rai_info_person_knows));
	cond_person_knows.push_back(move(join_condition_person_knows));

	LogicalComparisonJoin join_person1(JoinType::INNER);
	vector<TypeId> outputTypes{TypeId::INT64, TypeId::INT64, TypeId::INT64, TypeId::VARCHAR, TypeId::INT64};
	join_person1.types = outputTypes;
	auto join_person_knows = make_unique<PhysicalSIPJoin>(context, join_person1, move(join_knows),
	                                                      move(scan_function_person), move(cond_person_knows), join_person1.join_type,
	                                                      left_projection_map, right_projection_map);

	// project
	vector<TypeId> result_types{TypeId::VARCHAR};
	vector<unique_ptr<Expression>> select_list;
	auto result_col0 = make_unique<BoundReferenceExpression>("title", TypeId::VARCHAR, 3);
	select_list.push_back(move(result_col0));
	auto projection = make_unique<PhysicalProjection>(result_types, move(select_list));
	projection->children.push_back(move(join_person_knows));

	// execute!
	DataChunk result_chunk;
	result_chunk.Initialize(projection->types);
	auto state = projection->GetOperatorState();

	std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> start =
	    std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

	long long int result_num = 0;
	do {
		projection->GetChunk(*con.context, result_chunk, state.get());
		result_num += result_chunk.size();
		// result_chunk.Print();
	} while (result_chunk.size() > 0);

	std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> end =
	    std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

	std::cout << "Execution Time Cost: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
	std::cout << "Result Num: " << result_num << std::endl;
}

int test_graindb_smergejoin_person_constrained(string constantval, int& rnum) {
	DuckDB db(nullptr);
	Connection con(db);
	con.DisableProfiling();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	auto &context = *con.context;

	// CreateGraph(con);
	CreateGraphFromFile(con);

	con.context->transaction.Commit();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	// select * from a, b, c where a.id = b.a_id and b.c_id = c.id;

	vector<idx_t> left_projection_map, right_projection_map;

	string table_vertex_person = "person";
	string table_vertex_forum = "forum";
	string table_vertex_post = "post";
	string table_edge_knows = "knows";
	string table_edge_hasmember = "hasmember";
	string table_edge_containerof = "containerof";
	string table_edge_hascreator = "hascreator";
	idx_t table_index_person1 = 6;
	idx_t table_index_forum = 10;
	idx_t table_index_person2 = 8;
	idx_t table_index_post = 12;
	idx_t table_index_knows = 7;
	idx_t table_index_hasmember = 9;
	idx_t table_index_containerof = 11;
	idx_t table_index_hascreator = 13;

	buildIndex(con);

	auto table_or_view_person =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_vertex_person);
	auto table_person = (TableCatalogEntry *)table_or_view_person;

	auto table_or_view_forum =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_vertex_forum);
	auto table_forum = (TableCatalogEntry *)table_or_view_forum;

	auto table_or_view_post=
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_vertex_post);
	auto table_post = (TableCatalogEntry *)table_or_view_post;

	auto table_or_view_knows =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_knows);
	auto table_knows = (TableCatalogEntry *)table_or_view_knows;

	auto table_or_view_hasmember =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_hasmember);
	auto table_hasmember = (TableCatalogEntry *)table_or_view_hasmember;

	auto table_or_view_containerof =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_containerof);
	auto table_containerof = (TableCatalogEntry *)table_or_view_containerof;

	auto table_or_view_hascreator =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_hascreator);
	auto table_hascreator = (TableCatalogEntry *)table_or_view_hascreator;

	// string constantval = "4398046511628";
	// string constantval = "893";
	// string constantval = "0";

	// join post with hascreator
	vector<idx_t> post_ids{COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_post_types{TypeId::INT64};
	auto scan_function_post = move(get_scan_function(context, table_vertex_post,
	                                                 table_index_post, post_ids, get_post_types));

	vector<idx_t> hascreator_ids{2, 3};
	vector<TypeId> get_hascreator_types{TypeId::INT64, TypeId::INT64};
	auto scan_function_hascreator = move(get_scan_function(context, table_edge_hascreator,
	                                                       table_index_hascreator, hascreator_ids, get_hascreator_types));

	vector<JoinCondition> cond_post;
	JoinCondition join_condition_post;
	join_condition_post.left = make_unique<BoundReferenceExpression>("post_rowid", TypeId::INT64, 0);
	join_condition_post.right = make_unique<BoundReferenceExpression>("postid_rowid", TypeId::INT64, 0);
	join_condition_post.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_post = make_unique<RAIInfo>();
	rai_info_post->rai = table_hascreator->storage->info->rais[0].get(); // 2d40
	rai_info_post->rai_type = RAIType::SOURCE_EDGE;
	rai_info_post->forward = false;
	rai_info_post->vertex = table_post;
	rai_info_post->vertex_id = table_index_post;
	rai_info_post->passing_tables[0] = table_index_post;
	rai_info_post->left_cardinalities[0] = table_post->storage->info->cardinality;

	join_condition_post.rais.push_back(move(rai_info_post));
	cond_post.push_back(move(join_condition_post));

	LogicalComparisonJoin join_post_op(JoinType::INNER);
	vector<TypeId> output_post_types{TypeId::INT64, TypeId::INT64, TypeId::INT64};
	join_post_op.types = output_post_types;
	auto join_post = make_unique<PhysicalSIPJoin>(context, join_post_op, move(scan_function_post),
	                                              move(scan_function_hascreator), move(cond_post), join_post_op.join_type,
	                                              left_projection_map, right_projection_map);

	// join containerof with existing
	vector<idx_t> cof_ids{0, 3};
	vector<TypeId> get_cof_types{TypeId::VARCHAR, TypeId::INT64};
	auto scan_function_cof = move(get_scan_function(context, table_edge_containerof,
	                                                  table_index_containerof, cof_ids, get_cof_types));

	vector<JoinCondition> cond_cof;
	JoinCondition join_condition_cof;
	join_condition_cof.left = make_unique<BoundReferenceExpression>("postid_rowid", TypeId::INT64, 1);
	join_condition_cof.right = make_unique<BoundReferenceExpression>("post_rowid", TypeId::INT64, 0);
	join_condition_cof.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_cof = make_unique<RAIInfo>();
	rai_info_cof->rai = table_containerof->storage->info->rais[0].get(); // 33040
	rai_info_cof->rai_type = RAIType::EDGE_TARGET;
	rai_info_cof->forward = false;
	rai_info_cof->vertex = table_post;
	rai_info_cof->vertex_id = table_index_post;
	rai_info_cof->passing_tables[0] = table_index_containerof;
	rai_info_cof->left_cardinalities[0] = table_containerof->storage->info->cardinality;
	rai_info_cof->compact_list = &rai_info_cof->rai->alist->compact_backward_list; // 20b8

	join_condition_cof.rais.push_back(move(rai_info_cof));

	cond_cof.push_back(move(join_condition_cof));

	LogicalComparisonJoin join_cof_op(JoinType::INNER);
	vector<TypeId> output_cof_types{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64, TypeId::INT64};
	vector<idx_t> right_projection_map_cof{0, 2}; // reserve column 0 and 2 in the right table, and drop the other columns to join
	// vector<idx_t> merge_projection_map;
	join_cof_op.types = output_cof_types;
	auto join_cof = make_unique<PhysicalSIPJoin>(context, join_cof_op, move(scan_function_cof),
	                                               move(join_post), move(cond_cof), join_cof_op.join_type,
	                                               left_projection_map, right_projection_map_cof);

	// hasmember join forum
	vector<idx_t> hasmember_ids{2, 3};
	vector<TypeId> get_hasmember_types{TypeId::INT64, TypeId::INT64};
	auto scan_function_hasmember = move(get_scan_function(context, table_edge_hasmember,
	                                                   table_index_hasmember, hasmember_ids,
	                                                   get_hasmember_types));

	vector<idx_t> forum_ids{0, 1, COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_forum_types{TypeId::VARCHAR, TypeId::VARCHAR, TypeId::INT64};
	auto scan_function_forum = move(get_scan_function(context, table_vertex_forum,
	                                                      table_index_forum, forum_ids,
	                                                      get_forum_types));

	vector<JoinCondition> cond_hasmember_forum;
	JoinCondition join_condition_hasmember_forum;
	join_condition_hasmember_forum.left = make_unique<BoundReferenceExpression>("forumid_rowid", TypeId::INT64, 0);
	join_condition_hasmember_forum.right = make_unique<BoundReferenceExpression>("forum_rowid", TypeId::INT64, 2);
	join_condition_hasmember_forum.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_hasmember_forum = make_unique<RAIInfo>();
	rai_info_hasmember_forum->rai = table_hasmember->storage->info->rais[0].get();  // 3640
	rai_info_hasmember_forum->rai_type = RAIType::EDGE_SOURCE;
	rai_info_hasmember_forum->forward = true;
	rai_info_hasmember_forum->vertex = table_forum;
	rai_info_hasmember_forum->vertex_id = table_index_forum;
	rai_info_hasmember_forum->passing_tables[0] = table_index_hasmember;
	rai_info_hasmember_forum->left_cardinalities[0] = table_hasmember->storage->info->cardinality;
	rai_info_hasmember_forum->compact_list = &rai_info_hasmember_forum->rai->alist->compact_forward_list; //e498

	join_condition_hasmember_forum.rais.push_back(move(rai_info_hasmember_forum));
	cond_hasmember_forum.push_back(move(join_condition_hasmember_forum));

	LogicalComparisonJoin join_hasmember_forum_op(JoinType::INNER);
	vector<TypeId> output_types_hasmember_forum{TypeId::INT64, TypeId::INT64, TypeId::VARCHAR, TypeId::VARCHAR, TypeId::INT64};
	join_hasmember_forum_op.types = output_types_hasmember_forum;
	auto join_hasmember_forum = make_unique<PhysicalSIPJoin>(context, join_hasmember_forum_op, move(scan_function_hasmember),
	                                                      move(scan_function_forum), move(cond_hasmember_forum), join_hasmember_forum_op.join_type,
	                                                      left_projection_map, right_projection_map);

	// join person and knows
	/*vector<idx_t> person_ids{0, COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_person_types{TypeId::VARCHAR, TypeId::INT64};
	unordered_map<idx_t, vector<TableFilter>> person_filters;
	Value pv(constantval);
	TableFilter person_filter(pv, ExpressionType::COMPARE_EQUAL, 0);
	person_filters[0].push_back(person_filter);
	auto scan_function_person = move(get_scan_function(context, table_vertex_person,
	                                                   table_index_person1, person_ids,
	                                                   get_person_types, person_filters));

	vector<idx_t> knows_ids{0, 2, 3};
	vector<TypeId> get_knows_types{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64};
	unordered_map<idx_t, vector<TableFilter>> knows_filters;
	Value pknows(constantval);
	TableFilter knows_filter(pknows, ExpressionType::COMPARE_EQUAL, 0);
	knows_filters[0].push_back(knows_filter);
	auto scan_function_knows = move(get_scan_function(context, table_edge_knows,
	                                                  table_index_knows, knows_ids,
	                                                  get_knows_types, knows_filters));

	vector<JoinCondition> cond_person_knows;
	JoinCondition join_condition_person_knows;
	join_condition_person_knows.left = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 1);
	join_condition_person_knows.right = make_unique<BoundReferenceExpression>("id1_rowid", TypeId::INT64, 1);
	join_condition_person_knows.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_person_knows = make_unique<RAIInfo>();
	rai_info_person_knows->rai = table_knows->storage->info->rais[0].get();  // 3640
	rai_info_person_knows->rai_type = RAIType::SOURCE_EDGE;
	rai_info_person_knows->forward = false;
	rai_info_person_knows->vertex = table_person;
	rai_info_person_knows->vertex_id = table_index_person1;
	rai_info_person_knows->passing_tables[0] = table_index_person1;
	rai_info_person_knows->left_cardinalities[0] = table_person->storage->info->cardinality;
	// rai_info_person_knows->compact_list = &rai_info_person_knows->rai->alist->compact_backward_list; //e498

	join_condition_person_knows.rais.push_back(move(rai_info_person_knows));
	cond_person_knows.push_back(move(join_condition_person_knows));

	LogicalComparisonJoin join_person1(JoinType::INNER);
	vector<TypeId> outputTypes{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64, TypeId::INT64};
	join_person1.types = outputTypes;
	vector<idx_t> right_projection_map_person_knows{1, 2};
	auto join_person_knows = make_unique<PhysicalSIPJoin>(context, join_person1, move(scan_function_person),
	                                                      move(scan_function_knows), move(cond_person_knows), join_person1.join_type,
	                                                      left_projection_map, right_projection_map_person_knows);
*/

	// join person2 with exisitng
	vector<idx_t> knows_ids{0, 3};
	vector<TypeId> get_knows_types{TypeId::VARCHAR, TypeId::INT64};
	unordered_map<idx_t, vector<TableFilter>> knows_filters;
	Value pknows(constantval);
	TableFilter knows_filter(pknows, ExpressionType::COMPARE_EQUAL, 0);
	knows_filters[0].push_back(knows_filter);
	auto scan_function_knows = move(get_scan_function(context, table_edge_knows,
	                                                  table_index_knows, knows_ids,
	                                                  get_knows_types, knows_filters));
	                                                  //get_knows_types, knows_filters));

	vector<idx_t> person2_ids{COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_person2_types{TypeId::INT64};
	auto scan_function_person2 = move(get_scan_function(context, table_vertex_person,
	                                                    table_index_person2, person2_ids, get_person2_types));

	vector<JoinCondition> cond_person2;
	JoinCondition join_condition_person2;
	join_condition_person2.left = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 0);
	join_condition_person2.right = make_unique<BoundReferenceExpression>("id2_rowid", TypeId::INT64, 1);
	join_condition_person2.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_person2 = make_unique<RAIInfo>();
	rai_info_person2->rai = table_knows->storage->info->rais[0].get(); // 2d40
	rai_info_person2->rai_type = RAIType::TARGET_EDGE;
	rai_info_person2->forward = true;
	rai_info_person2->vertex = table_person;
	rai_info_person2->vertex_id = table_index_person2;
	rai_info_person2->passing_tables[0] = table_index_person2;
	rai_info_person2->left_cardinalities[0] = table_person->storage->info->cardinality;
	// rai_info_person2->compact_list = &rai_info_person2->rai->alist->compact_forward_list;

	join_condition_person2.rais.push_back(move(rai_info_person2));

	cond_person2.push_back(move(join_condition_person2));

	LogicalComparisonJoin join_person2_op(JoinType::INNER);
	vector<TypeId> output_person2_types{TypeId::INT64, TypeId::INT64};
	join_person2_op.types = output_person2_types;
	vector<idx_t> right_projection_map_person2{1};
	auto join_person2 = make_unique<PhysicalSIPJoin>(context, join_person2_op, move(scan_function_person2),
	                                                 move(scan_function_knows), move(cond_person2), join_person2_op.join_type,
	                                                 left_projection_map, right_projection_map_person2);

	// hasmember join person2
	vector<JoinCondition> cond_hasmember_person2;
	JoinCondition join_condition_hasmember_person2;
	join_condition_hasmember_person2.left = make_unique<BoundReferenceExpression>("personid_rowid", TypeId::INT64, 1);
	join_condition_hasmember_person2.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 0);
	join_condition_hasmember_person2.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_hasmember_person2 = make_unique<RAIInfo>();
	rai_info_hasmember_person2->rai = table_hasmember->storage->info->rais[0].get(); // 2d40
	rai_info_hasmember_person2->rai_type = RAIType::EDGE_TARGET;
	rai_info_hasmember_person2->forward = false;
	rai_info_hasmember_person2->vertex = table_person;
	rai_info_hasmember_person2->vertex_id = table_index_person2;
	rai_info_hasmember_person2->passing_tables[0] = table_index_hasmember;
	rai_info_hasmember_person2->left_cardinalities[0] = table_hasmember->storage->info->cardinality;
	rai_info_hasmember_person2->compact_list = &rai_info_hasmember_person2->rai->alist->compact_backward_list;

	join_condition_hasmember_person2.rais.push_back(move(rai_info_hasmember_person2));

	cond_hasmember_person2.push_back(move(join_condition_hasmember_person2));

	LogicalComparisonJoin join_hasmember_person2_op(JoinType::INNER);
	vector<TypeId> output_hasmember_person2_types{TypeId::INT64, TypeId::INT64, TypeId::VARCHAR, TypeId::VARCHAR,
	                                              TypeId::INT64, TypeId::INT64};
	join_hasmember_person2_op.types = output_hasmember_person2_types;
	vector<idx_t> right_projection_map_hasmember_person2{0};
	auto join_hasmember_person2 = make_unique<PhysicalSIPJoin>(context, join_hasmember_person2_op, move(join_hasmember_forum),
	                                                 move(join_person2), move(cond_hasmember_person2), join_hasmember_person2_op.join_type,
	                                                 left_projection_map, right_projection_map_hasmember_person2);

	// join all

	vector<JoinCondition> cond_path_2;
	JoinCondition join_condition_path_21, join_condition_path_22;
	join_condition_path_21.left = make_unique<BoundReferenceExpression>("personid_rowid", TypeId::INT64, 3);
	join_condition_path_21.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 5);
	join_condition_path_21.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_path21 = make_unique<RAIInfo>();
	rai_info_path21->rai = table_hascreator->storage->info->rais[0].get(); // 2d40
	rai_info_path21->rai_type = RAIType::EDGE_TARGET;
	rai_info_path21->forward = false;
	rai_info_path21->vertex = table_person;
	rai_info_path21->vertex_id = table_index_person2;
	rai_info_path21->passing_tables[0] = table_index_hascreator;
	rai_info_path21->left_cardinalities[0] = table_hascreator->storage->info->cardinality;
	rai_info_path21->compact_list = &rai_info_path21->rai->alist->compact_backward_list;

	join_condition_path_21.rais.push_back(move(rai_info_path21));

	join_condition_path_22.left = make_unique<BoundReferenceExpression>("forumid", TypeId::VARCHAR, 0);
	join_condition_path_22.right = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 2);
	join_condition_path_22.comparison = ExpressionType::COMPARE_EQUAL;

	cond_path_2.push_back(move(join_condition_path_21));
	cond_path_2.push_back(move(join_condition_path_22));

	LogicalComparisonJoin join_path_2_op(JoinType::INNER);
	vector<TypeId> output_path_types_2{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64, TypeId::INT64,
	                                   TypeId::VARCHAR, TypeId::VARCHAR, TypeId::INT64};
	join_path_2_op.types = output_path_types_2;
	vector<idx_t> right_projection_map_path_2{2, 3, 5};
	auto join_path_2 = make_unique<PhysicalSIPJoin>(context, join_path_2_op, move(join_cof),
	                                               move(join_hasmember_person2), move(cond_path_2), join_path_2_op.join_type,
	                                               left_projection_map, right_projection_map_path_2);

	// project
	vector<TypeId> result_types{TypeId::VARCHAR};
	vector<unique_ptr<Expression>> select_list;
	auto result_col0 = make_unique<BoundReferenceExpression>("title", TypeId::VARCHAR, 5);
	select_list.push_back(move(result_col0));
	auto projection = make_unique<PhysicalProjection>(result_types, move(select_list));
	projection->children.push_back(move(join_path_2));

	// execute!
	DataChunk result_chunk;
	result_chunk.Initialize(projection->types);
	auto state = projection->GetOperatorState();

	std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> start =
	    std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

	long long int result_num = 0;
	do {
		projection->GetChunk(*con.context, result_chunk, state.get());
		result_num += result_chunk.size();
		// result_chunk.Print();
	} while (result_chunk.size() > 0);

	std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> end =
	    std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

	std::cout << "Execution Time Cost: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
	std::cout << "Result Num: " << result_num << std::endl;

	rnum = result_num;
	return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

int test_graindb_smergejoin_ldbc(string constantval, int& rnum) {
	DuckDB db(nullptr);
	Connection con(db);
	con.DisableProfiling();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	auto &context = *con.context;

	// CreateGraph(con);
	CreateGraphFromFile(con);

	con.context->transaction.Commit();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	// select * from a, b, c where a.id = b.a_id and b.c_id = c.id;

	vector<idx_t> left_projection_map, right_projection_map;

	string table_vertex_person = "person";
	string table_vertex_forum = "forum";
	string table_vertex_post = "post";
	string table_edge_knows = "knows";
	string table_edge_hasmember = "hasmember";
	string table_edge_containerof = "containerof";
	string table_edge_hascreator = "hascreator";
	idx_t table_index_person1 = 6;
	idx_t table_index_forum = 12;
	idx_t table_index_person2 = 8;
	idx_t table_index_post = 22;
	idx_t table_index_knows = 20;
	idx_t table_index_hasmember = 13;
	idx_t table_index_containerof = 23;
	idx_t table_index_hascreator = 21;

	buildIndex(con);

	auto table_or_view_person =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_vertex_person);
	auto table_person = (TableCatalogEntry *)table_or_view_person;

	auto table_or_view_forum =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_vertex_forum);
	auto table_forum = (TableCatalogEntry *)table_or_view_forum;

	auto table_or_view_post=
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_vertex_post);
	auto table_post = (TableCatalogEntry *)table_or_view_post;

	auto table_or_view_knows =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_knows);
	auto table_knows = (TableCatalogEntry *)table_or_view_knows;

	auto table_or_view_hasmember =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_hasmember);
	auto table_hasmember = (TableCatalogEntry *)table_or_view_hasmember;

	auto table_or_view_containerof =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_containerof);
	auto table_containerof = (TableCatalogEntry *)table_or_view_containerof;

	auto table_or_view_hascreator =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_hascreator);
	auto table_hascreator = (TableCatalogEntry *)table_or_view_hascreator;

	// string constantval = "4398046511628";
	// string constantval = "893";
	// string constantval = "0";


	// join post and containerof
	vector<idx_t> post_ids{COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_post_types{TypeId::INT64};
	auto scan_function_post = move(get_scan_function(context, table_vertex_post,
	                                                 table_index_post, post_ids, get_post_types));
	vector<idx_t> cof_ids{0, 3};
	vector<TypeId> get_cof_types{TypeId::VARCHAR, TypeId::INT64};
	auto scan_function_cof = move(get_scan_function(context, table_edge_containerof,
	                                                    table_index_containerof, cof_ids, get_cof_types));

	vector<JoinCondition> cond_post_cof;
	JoinCondition join_condition_post_cof;
	join_condition_post_cof.left = make_unique<BoundReferenceExpression>("post_rowid", TypeId::INT64, 0);
	join_condition_post_cof.right = make_unique<BoundReferenceExpression>("postid_rowid", TypeId::INT64, 1);
	join_condition_post_cof.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_post_cof = make_unique<RAIInfo>();
	rai_info_post_cof->rai = table_containerof->storage->info->rais[0].get(); // 2d40
	rai_info_post_cof->rai_type = RAIType::TARGET_EDGE;
	rai_info_post_cof->forward = true;
	rai_info_post_cof->vertex = table_post;
	rai_info_post_cof->vertex_id = table_index_post;
	rai_info_post_cof->passing_tables[0] = table_index_post;
	rai_info_post_cof->left_cardinalities[0] = table_post->storage->info->cardinality;

	join_condition_post_cof.rais.push_back(move(rai_info_post_cof));

	cond_post_cof.push_back(move(join_condition_post_cof));

	LogicalComparisonJoin join_post_cof_op(JoinType::INNER);
	vector<TypeId> output_post_cof_types{TypeId::INT64, TypeId::VARCHAR, TypeId::INT64};
	join_post_cof_op.types = output_post_cof_types;
	auto join_post_cof = make_unique<PhysicalSIPJoin>(context, join_post_cof_op, move(scan_function_post),
	                                                           move(scan_function_cof), move(cond_post_cof), join_post_cof_op.join_type,
	                                                           left_projection_map, right_projection_map);

	// join hascreator

	vector<idx_t> hascreator_ids{1, 2};
	vector<TypeId> get_hascreator_types{TypeId::VARCHAR, TypeId::INT64};
	auto scan_function_hascreator = move(get_scan_function(context, table_edge_hascreator,
	                                                 table_index_hascreator, hascreator_ids, get_hascreator_types));

	vector<JoinCondition> cond_hascreator;
	JoinCondition join_condition_hascreator;
	join_condition_hascreator.left = make_unique<BoundReferenceExpression>("postid_rowid", TypeId::INT64, 1);
	join_condition_hascreator.right = make_unique<BoundReferenceExpression>("post_rowid", TypeId::INT64, 0);
	join_condition_hascreator.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_hascreator = make_unique<RAIInfo>();
	rai_info_hascreator->rai = table_hascreator->storage->info->rais[0].get(); // 2d40
	rai_info_hascreator->rai_type = RAIType::EDGE_SOURCE;
	rai_info_hascreator->forward = true;
	rai_info_hascreator->vertex = table_post;
	rai_info_hascreator->vertex_id = table_index_post;
	rai_info_hascreator->passing_tables[0] = table_index_hascreator;
	rai_info_hascreator->left_cardinalities[0] = table_hascreator->storage->info->cardinality;
	rai_info_hascreator->compact_list = &rai_info_hascreator->rai->alist->compact_forward_list;

	join_condition_hascreator.rais.push_back(move(rai_info_hascreator));

	cond_hascreator.push_back(move(join_condition_hascreator));

	LogicalComparisonJoin join_hascreator_op(JoinType::INNER);
	vector<TypeId> output_hascreator_types{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64, TypeId::VARCHAR};
	join_hascreator_op.types = output_hascreator_types;
	vector<idx_t> right_projection_map_hascreator{0, 1};
	auto join_hascreator = make_unique<PhysicalSIPJoin>(context, join_hascreator_op, move(scan_function_hascreator),
	                                                         move(join_post_cof), move(cond_hascreator), join_hascreator_op.join_type,
	                                                         left_projection_map, right_projection_map_hascreator);

	// project for hasmember

	vector<idx_t> knows_ids{0, 1};
	vector<TypeId> get_knows_types{TypeId::VARCHAR, TypeId::VARCHAR};
	unordered_map<idx_t, vector<TableFilter>> knows_filters;
	Value pknows(constantval);
	TableFilter knows_filter(pknows, ExpressionType::COMPARE_EQUAL, 0);
	knows_filters[0].push_back(knows_filter);
	auto scan_function_knows = move(get_scan_function(context, table_edge_knows,
	                                                  table_index_knows, knows_ids,
	                                                  get_knows_types, knows_filters));
	                                                  //get_knows_types, knows_filters));

	vector<TypeId> project_for_hasmember_result_types{TypeId::VARCHAR};
	vector<unique_ptr<Expression>> project_for_hasmember_select_list;
	auto project_for_hasmember_result_col0 = make_unique<BoundReferenceExpression>("id2", TypeId::VARCHAR, 1);
	project_for_hasmember_select_list.push_back(move(project_for_hasmember_result_col0));
	auto hasmember_projection = make_unique<PhysicalProjection>(project_for_hasmember_result_types, move(project_for_hasmember_select_list));
	hasmember_projection->children.push_back(move(scan_function_knows));

	// join hasmember

	vector<idx_t> hasmember_ids{1, 2};
	vector<TypeId> get_hasmember_types{TypeId::VARCHAR, TypeId::INT64};
	auto scan_function_hasmember = move(get_scan_function(context, table_edge_hasmember,
	                                                       table_index_hasmember, hasmember_ids, get_hasmember_types));

	vector<JoinCondition> cond_hasmember;
	JoinCondition join_condition_hasmember;
	join_condition_hasmember.left = make_unique<BoundReferenceExpression>("personid", TypeId::VARCHAR, 0);
	join_condition_hasmember.right = make_unique<BoundReferenceExpression>("id2", TypeId::VARCHAR, 0);
	join_condition_hasmember.comparison = ExpressionType::COMPARE_EQUAL;

	cond_hasmember.push_back(move(join_condition_hasmember));

	LogicalComparisonJoin join_hasmember_op(JoinType::INNER);
	vector<TypeId> output_hasmember_types{TypeId::VARCHAR, TypeId::INT64, TypeId::VARCHAR};
	join_hasmember_op.types = output_hasmember_types;
	auto join_hasmember = make_unique<PhysicalHashJoin>(context, join_hasmember_op, move(scan_function_hasmember),
	                                                 move(hasmember_projection), move(cond_hasmember), join_hasmember_op.join_type,
	                                                 left_projection_map, right_projection_map);

	// join forum

	vector<idx_t> forum_ids{0, 1, COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_forum_types{TypeId::VARCHAR, TypeId::VARCHAR, TypeId::INT64};
	auto scan_function_forum = move(get_scan_function(context, table_vertex_forum,
	                                                       table_index_forum, forum_ids, get_forum_types));

	vector<JoinCondition> cond_forum;
	JoinCondition join_condition_forum;
	join_condition_forum.left = make_unique<BoundReferenceExpression>("forumid_rowid", TypeId::INT64, 1);
	join_condition_forum.right = make_unique<BoundReferenceExpression>("forum_rowid", TypeId::INT64, 2);
	join_condition_forum.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_forum = make_unique<RAIInfo>();
	rai_info_forum->rai = table_hasmember->storage->info->rais[0].get(); // 2d40
	rai_info_forum->rai_type = RAIType::EDGE_SOURCE;
	rai_info_forum->forward = true;
	rai_info_forum->vertex = table_forum;
	rai_info_forum->vertex_id = table_index_forum;
	rai_info_forum->passing_tables[0] = table_index_hasmember;
	rai_info_forum->left_cardinalities[0] = table_hasmember->storage->info->cardinality;
	rai_info_forum->compact_list = &rai_info_forum->rai->alist->compact_forward_list;

	join_condition_forum.rais.push_back(move(rai_info_forum));

	cond_forum.push_back(move(join_condition_forum));

	LogicalComparisonJoin join_forum_op(JoinType::INNER);
	vector<TypeId> output_forum_types{TypeId::VARCHAR, TypeId::INT64, TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR, TypeId::INT64};
	join_forum_op.types = output_forum_types;
	auto join_forum = make_unique<PhysicalSIPJoin>(context, join_forum_op, move(join_hasmember),
	                                                    move(scan_function_forum), move(cond_forum), join_forum_op.join_type,
	                                                    left_projection_map, right_projection_map);

	// project 2

	vector<TypeId> inner_result_types{TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR};
	vector<unique_ptr<Expression>> inner_select_list;
	auto inner_result_col0 = make_unique<BoundReferenceExpression>("title", TypeId::VARCHAR, 4);
	auto inner_result_col1 = make_unique<BoundReferenceExpression>("f_forumid", TypeId::VARCHAR, 3);
	auto inner_result_col2 = make_unique<BoundReferenceExpression>("k_person2id", TypeId::VARCHAR, 2);
	inner_select_list.push_back(move(inner_result_col0));
	inner_select_list.push_back(move(inner_result_col1));
	inner_select_list.push_back(move(inner_result_col2));
	auto inner_projection = make_unique<PhysicalProjection>(inner_result_types, move(inner_select_list));
	inner_projection->children.push_back(move(join_forum));

	// join all

	vector<JoinCondition> cond_path_2;
	JoinCondition join_condition_path_21, join_condition_path_22;
	join_condition_path_21.left = make_unique<BoundReferenceExpression>("personid", TypeId::VARCHAR, 0);
	join_condition_path_21.right = make_unique<BoundReferenceExpression>("k_person2id", TypeId::VARCHAR, 2);
	join_condition_path_21.comparison = ExpressionType::COMPARE_EQUAL;

	join_condition_path_22.left = make_unique<BoundReferenceExpression>("forumid", TypeId::VARCHAR, 3);
	join_condition_path_22.right = make_unique<BoundReferenceExpression>("f_forumid", TypeId::VARCHAR, 1);
	join_condition_path_22.comparison = ExpressionType::COMPARE_EQUAL;

	cond_path_2.push_back(move(join_condition_path_21));
	cond_path_2.push_back(move(join_condition_path_22));

	LogicalComparisonJoin join_path_2_op(JoinType::INNER);
	vector<TypeId> output_path_types_2{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64, TypeId::VARCHAR,
	                                   TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR};
	join_path_2_op.types = output_path_types_2;
	auto join_path_2 = make_unique<PhysicalHashJoin>(context, join_path_2_op, move(join_hascreator),
	                                                move(inner_projection), move(cond_path_2), join_path_2_op.join_type,
	                                                left_projection_map, right_projection_map);

	// project
	vector<TypeId> result_types{TypeId::VARCHAR};
	vector<unique_ptr<Expression>> select_list;
	auto result_col0 = make_unique<BoundReferenceExpression>("title", TypeId::VARCHAR, 4);
	select_list.push_back(move(result_col0));
	auto projection = make_unique<PhysicalProjection>(result_types, move(select_list));
	projection->children.push_back(move(join_path_2));

	// execute!
	DataChunk result_chunk;
	result_chunk.Initialize(projection->types);
	auto state = projection->GetOperatorState();

	std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> start =
	    std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

	long long int result_num = 0;
	do {
		projection->GetChunk(*con.context, result_chunk, state.get());
		result_num += result_chunk.size();
		// result_chunk.Print();
	} while (result_chunk.size() > 0);

	std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> end =
	    std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

	std::cout << "Execution Time Cost: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
	std::cout << "Result Num: " << result_num << std::endl;

	rnum = result_num;
	return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

int test_duckdb_ldbc(string constantval, int& rnum) {
	DuckDB db(nullptr);
	Connection con(db);
	con.DisableProfiling();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	auto &context = *con.context;

	// CreateGraph(con);
	CreateGraphFromFile(con);

	con.context->transaction.Commit();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	// select * from a, b, c where a.id = b.a_id and b.c_id = c.id;

	vector<idx_t> left_projection_map, right_projection_map;

	string table_vertex_person = "person";
	string table_vertex_forum = "forum";
	string table_vertex_post = "post";
	string table_edge_knows = "knows";
	string table_edge_hasmember = "hasmember";
	string table_edge_containerof = "containerof";
	string table_edge_hascreator = "hascreator";
	idx_t table_index_person1 = 6;
	idx_t table_index_forum = 12;
	idx_t table_index_person2 = 8;
	idx_t table_index_post = 22;
	idx_t table_index_knows = 20;
	idx_t table_index_hasmember = 13;
	idx_t table_index_containerof = 23;
	idx_t table_index_hascreator = 21;

	auto table_or_view_person =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_vertex_person);
	auto table_person = (TableCatalogEntry *)table_or_view_person;

	auto table_or_view_forum =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_vertex_forum);
	auto table_forum = (TableCatalogEntry *)table_or_view_forum;

	auto table_or_view_post=
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_vertex_post);
	auto table_post = (TableCatalogEntry *)table_or_view_post;

	auto table_or_view_knows =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_knows);
	auto table_knows = (TableCatalogEntry *)table_or_view_knows;

	auto table_or_view_hasmember =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_hasmember);
	auto table_hasmember = (TableCatalogEntry *)table_or_view_hasmember;

	auto table_or_view_containerof =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_containerof);
	auto table_containerof = (TableCatalogEntry *)table_or_view_containerof;

	auto table_or_view_hascreator =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "main", table_edge_hascreator);
	auto table_hascreator = (TableCatalogEntry *)table_or_view_hascreator;

	// string constantval = "4398046511628";
	// string constantval = "893";
	// string constantval = "0";

	// join person and knows
	vector<idx_t> person2_ids{0};
	vector<TypeId> get_person2_types{TypeId::VARCHAR};
	auto scan_function_person2 = move(get_scan_function(context, table_vertex_person,
	                                                      table_index_person2, person2_ids, get_person2_types));

	vector<idx_t> knows_ids{1, 0};
	vector<TypeId> get_knows_types{TypeId::VARCHAR, TypeId::VARCHAR};
	unordered_map<idx_t, vector<TableFilter>> knows_filters;
	Value pknows(constantval);
	TableFilter knows_filter(pknows, ExpressionType::COMPARE_EQUAL, 1);
	knows_filters[1].push_back(knows_filter);
	auto scan_function_knows = move(get_scan_function(context, table_edge_knows,
	                                                  table_index_knows, knows_ids,
	                                                  get_knows_types, knows_filters));
	                                                  //get_knows_types, knows_filters));

	vector<JoinCondition> cond_person2_knows;
	JoinCondition join_condition_person2_knows;
	join_condition_person2_knows.left = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 0);
	join_condition_person2_knows.right = make_unique<BoundReferenceExpression>("id2", TypeId::VARCHAR, 0);
	join_condition_person2_knows.comparison = ExpressionType::COMPARE_EQUAL;

	cond_person2_knows.push_back(move(join_condition_person2_knows));

	LogicalComparisonJoin join_person2_knows_op(JoinType::INNER);
	vector<TypeId> output_person2_knows{TypeId::VARCHAR, TypeId::VARCHAR};
	join_person2_knows_op.types = output_person2_knows;
	vector<idx_t> right_projection_map_person2_knows_map{0};
	auto join_person2_knows = make_unique<PhysicalHashJoin>(context, join_person2_knows_op, move(scan_function_person2),
	                                                 move(scan_function_knows), move(cond_person2_knows), join_person2_knows_op.join_type,
	                                                 left_projection_map, right_projection_map_person2_knows_map);


	// join hascreator

	vector<idx_t> hascreator_ids{0, 1};
	vector<TypeId> get_hascreator_types{TypeId::VARCHAR, TypeId::VARCHAR};
	auto scan_function_hascreator = move(get_scan_function(context, table_edge_hascreator,
	                                                    table_index_hascreator, hascreator_ids, get_hascreator_types));

	vector<JoinCondition> cond_hascreator;
	JoinCondition join_condition_hascreator1, join_condition_hascreator2;
	join_condition_hascreator1.left = make_unique<BoundReferenceExpression>("personid", TypeId::VARCHAR, 1);
	join_condition_hascreator1.right = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 0);
	join_condition_hascreator1.comparison = ExpressionType::COMPARE_EQUAL;

	join_condition_hascreator2.left = make_unique<BoundReferenceExpression>("personid", TypeId::VARCHAR, 1);
	join_condition_hascreator2.right = make_unique<BoundReferenceExpression>("id2", TypeId::VARCHAR, 0);
	join_condition_hascreator2.comparison = ExpressionType::COMPARE_EQUAL;

	cond_hascreator.push_back(move(join_condition_hascreator1));
	cond_hascreator.push_back(move(join_condition_hascreator2));

	LogicalComparisonJoin join_hascreator_op(JoinType::INNER);
	vector<TypeId> output_hascreator{TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR};
	join_hascreator_op.types = output_hascreator;
	auto join_hascreator = make_unique<PhysicalHashJoin>(context, join_hascreator_op, move(scan_function_hascreator),
	                                                        move(join_person2_knows), move(cond_hascreator), join_hascreator_op.join_type,
	                                                        left_projection_map, right_projection_map);

	// join containerof

	vector<idx_t> cof_ids{0, 1};
	vector<TypeId> get_cof_types{TypeId::VARCHAR, TypeId::VARCHAR};
	auto scan_function_cof = move(get_scan_function(context, table_edge_containerof,
	                                                       table_index_containerof, cof_ids, get_cof_types));

	vector<JoinCondition> cond_cof;
	JoinCondition join_condition_cof;
	join_condition_cof.left = make_unique<BoundReferenceExpression>("postid", TypeId::VARCHAR, 1);
	join_condition_cof.right = make_unique<BoundReferenceExpression>("postid", TypeId::VARCHAR, 0);
	join_condition_cof.comparison = ExpressionType::COMPARE_EQUAL;

	cond_cof.push_back(move(join_condition_cof));

	LogicalComparisonJoin join_cof_op(JoinType::INNER);
	vector<TypeId> output_cof{TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR};
	join_cof_op.types = output_cof;
	vector<idx_t> right_projection_map_cof{1};
	auto join_cof = make_unique<PhysicalHashJoin>(context, join_cof_op, move(scan_function_cof),
	                                                     move(join_hascreator), move(cond_cof), join_cof_op.join_type,
	                                                     left_projection_map, right_projection_map_cof);


	// join forum

	vector<idx_t> forum_ids{0, 1};
	vector<TypeId> get_forum_types{TypeId::VARCHAR, TypeId::VARCHAR};
	auto scan_function_forum = move(get_scan_function(context, table_vertex_forum,
	                                                table_index_forum, forum_ids, get_forum_types));

	vector<JoinCondition> cond_forum;
	JoinCondition join_condition_forum;
	join_condition_forum.left = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 0);
	join_condition_forum.right = make_unique<BoundReferenceExpression>("forumid", TypeId::VARCHAR, 0);
	join_condition_forum.comparison = ExpressionType::COMPARE_EQUAL;

	cond_forum.push_back(move(join_condition_forum));

	LogicalComparisonJoin join_forum_op(JoinType::INNER);
	vector<TypeId> output_forum{TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR};
	join_forum_op.types = output_forum;
	vector<idx_t> right_projection_map_forum{1, 2};
	auto join_forum = make_unique<PhysicalHashJoin>(context, join_forum_op, move(scan_function_forum),
	                                              move(join_cof), move(cond_forum), join_forum_op.join_type,
	                                              left_projection_map, right_projection_map_forum);

	// join post

	vector<idx_t> post_ids{0};
	vector<TypeId> get_post_types{TypeId::VARCHAR};
	auto scan_function_post = move(get_scan_function(context, table_vertex_post,
	                                                  table_index_post, post_ids, get_post_types));

	vector<JoinCondition> cond_post;
	JoinCondition join_condition_post1, join_condition_post2;
	join_condition_post1.left = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 0);
	join_condition_post1.right = make_unique<BoundReferenceExpression>("postid", TypeId::VARCHAR, 2);
	join_condition_post1.comparison = ExpressionType::COMPARE_EQUAL;

	join_condition_post2.left = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 0);
	join_condition_post2.right = make_unique<BoundReferenceExpression>("postid", TypeId::VARCHAR, 2);
	join_condition_post2.comparison = ExpressionType::COMPARE_EQUAL;

	cond_post.push_back(move(join_condition_post1));
	cond_post.push_back(move(join_condition_post2));

	LogicalComparisonJoin join_post_op(JoinType::INNER);
	vector<TypeId> output_post{TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR};
	join_post_op.types = output_post;
	vector<idx_t> right_projection_map_post{0, 1, 3};
	auto join_post = make_unique<PhysicalHashJoin>(context, join_post_op, move(scan_function_post),
	                                                move(join_forum), move(cond_post), join_post_op.join_type,
	                                                left_projection_map, right_projection_map_post);

	// join hasmember

	vector<idx_t> hasmember_ids{1, 0};
	vector<TypeId> get_hasmember_types{TypeId::VARCHAR, TypeId::VARCHAR};
	auto scan_function_hasmember = move(get_scan_function(context, table_edge_hasmember,
	                                                  table_index_hasmember, hasmember_ids, get_hasmember_types));

	vector<JoinCondition> cond_hasmember;
	JoinCondition join_condition_hasmember1, join_condition_hasmember2, join_condition_hasmember3, join_condition_hasmember4, join_condition_hasmember5;
	join_condition_hasmember1.left = make_unique<BoundReferenceExpression>("personid", TypeId::VARCHAR, 0);
	join_condition_hasmember1.right = make_unique<BoundReferenceExpression>("personid", TypeId::VARCHAR, 3);
	join_condition_hasmember1.comparison = ExpressionType::COMPARE_EQUAL;

	join_condition_hasmember2.left = make_unique<BoundReferenceExpression>("forumid", TypeId::VARCHAR, 1);
	join_condition_hasmember2.right = make_unique<BoundReferenceExpression>("forumid", TypeId::VARCHAR, 1);
	join_condition_hasmember2.comparison = ExpressionType::COMPARE_EQUAL;

	join_condition_hasmember3.left = make_unique<BoundReferenceExpression>("forumid", TypeId::VARCHAR, 1);
	join_condition_hasmember3.right = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 1);
	join_condition_hasmember3.comparison = ExpressionType::COMPARE_EQUAL;

	join_condition_hasmember4.left = make_unique<BoundReferenceExpression>("personid", TypeId::VARCHAR, 0);
	join_condition_hasmember4.right = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 3);
	join_condition_hasmember4.comparison = ExpressionType::COMPARE_EQUAL;

	join_condition_hasmember5.left = make_unique<BoundReferenceExpression>("personid", TypeId::VARCHAR, 0);
	join_condition_hasmember5.right = make_unique<BoundReferenceExpression>("id2", TypeId::VARCHAR, 3);
	join_condition_hasmember5.comparison = ExpressionType::COMPARE_EQUAL;

	cond_hasmember.push_back(move(join_condition_hasmember1));
	cond_hasmember.push_back(move(join_condition_hasmember2));
	cond_hasmember.push_back(move(join_condition_hasmember3));
	cond_hasmember.push_back(move(join_condition_hasmember4));
	cond_hasmember.push_back(move(join_condition_hasmember5));

	LogicalComparisonJoin join_hasmember_op(JoinType::INNER);
	vector<TypeId> output_hasmember{TypeId::VARCHAR, TypeId::VARCHAR, TypeId::VARCHAR};
	join_hasmember_op.types = output_hasmember;
	vector<idx_t> right_projection_map_hasmember{2};
	auto join_hasmember = make_unique<PhysicalHashJoin>(context, join_hasmember_op, move(scan_function_hasmember),
	                                                 move(join_post), move(cond_hasmember), join_hasmember_op.join_type,
	                                                 left_projection_map, right_projection_map_hasmember);

	// project
	vector<TypeId> result_types{TypeId::VARCHAR};
	vector<unique_ptr<Expression>> select_list;
	auto result_col0 = make_unique<BoundReferenceExpression>("title", TypeId::VARCHAR, 2);
	select_list.push_back(move(result_col0));
	auto projection = make_unique<PhysicalProjection>(result_types, move(select_list));
	projection->children.push_back(move(join_hasmember));

	// execute!
	DataChunk result_chunk;
	result_chunk.Initialize(projection->types);
	auto state = projection->GetOperatorState();

	std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> start =
	    std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

	long long int result_num = 0;
	do {
		projection->GetChunk(*con.context, result_chunk, state.get());
		result_num += result_chunk.size();
		// result_chunk.Print();
	} while (result_chunk.size() > 0);

	std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> end =
	    std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

	std::cout << "Execution Time Cost: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
	std::cout << "Result Num: " << result_num << std::endl;

	rnum = result_num;
	return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

int test_gopt_join(string constantval, int& rnum) {
	DuckDB db(nullptr);
	Connection con(db);
	con.DisableProfiling();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	auto &context = *con.context;

	CreateGraphFromFile(con);
	// CreateGraph(con);

	con.context->transaction.Commit();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	// select * from a, b, c where a.id = b.a_id and b.c_id = c.id;

	vector<idx_t> left_projection_map, right_projection_map;

	string table_vertex_person = "person";
	string table_vertex_forum = "forum";
	string table_vertex_post = "post";
	string table_edge_knows = "knows";
	string table_edge_hasmember = "hasmember";
	string table_edge_containerof = "containerof";
	string table_edge_hascreator = "hascreator";
	idx_t table_index_person1 = 6;
	idx_t table_index_forum = 10;
	idx_t table_index_person2 = 8;
	idx_t table_index_post = 12;
	idx_t table_index_knows = 7;
	idx_t table_index_hasmember = 9;
	idx_t table_index_containerof = 11;
	idx_t table_index_hascreator = 13;

	buildIndex(con);

	auto table_or_view_person =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_vertex_person);
	auto table_person = (TableCatalogEntry *)table_or_view_person;

	auto table_or_view_forum =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_vertex_forum);
	auto table_forum = (TableCatalogEntry *)table_or_view_forum;

	auto table_or_view_post=
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_vertex_post);
	auto table_post = (TableCatalogEntry *)table_or_view_post;

	auto table_or_view_knows =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_edge_knows);
	auto table_knows = (TableCatalogEntry *)table_or_view_knows;

	auto table_or_view_hasmember =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_edge_hasmember);
	auto table_hasmember = (TableCatalogEntry *)table_or_view_hasmember;

	auto table_or_view_containerof =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_edge_containerof);
	auto table_containerof = (TableCatalogEntry *)table_or_view_containerof;

	auto table_or_view_hascreator =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_edge_hascreator);
	auto table_hascreator = (TableCatalogEntry *)table_or_view_hascreator;

	// string constantval = "4398046511628";
	// string constantval = "893";

	// person join knows
	/*vector<idx_t> person_ids{0, COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_person_types{TypeId::VARCHAR, TypeId::INT64};
	unordered_map<idx_t, vector<TableFilter>> person_filters;
	Value pv(constantval);
	TableFilter person_filter(pv, ExpressionType::COMPARE_EQUAL, 0); // column 0 after projection to {0, COLUMN_IDENTIFIER_ROW_ID}
	person_filters[0].push_back(person_filter);
	auto scan_function_person = move(get_scan_function(context, table_vertex_person,
	                                                   table_index_person1, person_ids,
	                                                   get_person_types, person_filters));

	vector<idx_t> knows_ids{0, 3, 2};
	vector<TypeId> get_knows_types{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64};
	unordered_map<idx_t, vector<TableFilter>> knows_filters;
	Value pknows(constantval);
	TableFilter knows_filter(pknows, ExpressionType::COMPARE_EQUAL, 0);
	knows_filters[0].push_back(knows_filter);
	auto scan_function_knows = move(get_scan_function(context, table_edge_knows,
	                                                  table_index_knows, knows_ids,
	                                                  get_knows_types, knows_filters));

	vector<JoinCondition> cond_person_knows;
	JoinCondition join_condition_person_knows;
	join_condition_person_knows.left = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 1);
	join_condition_person_knows.right = make_unique<BoundReferenceExpression>("id1_rowid", TypeId::INT64, 2);
	join_condition_person_knows.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_person_knows = make_unique<RAIInfo>();
	rai_info_person_knows->rai = table_knows->storage->info->rais[0].get();  // 3640
	rai_info_person_knows->rai_type = RAIType::SOURCE_EDGE;
	rai_info_person_knows->forward = true;
	rai_info_person_knows->vertex = table_person;
	rai_info_person_knows->vertex_id = table_index_person1;
	rai_info_person_knows->passing_tables[0] = table_index_knows;
	rai_info_person_knows->left_cardinalities[0] = table_knows->storage->info->cardinality;
	rai_info_person_knows->compact_list = &rai_info_person_knows->rai->alist->compact_forward_list; //e498

	join_condition_person_knows.rais.push_back(move(rai_info_person_knows));
	cond_person_knows.push_back(move(join_condition_person_knows));

	LogicalComparisonJoin join_person1(JoinType::INNER);
	vector<TypeId> outputTypes{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64, TypeId::INT64};
	join_person1.types = outputTypes;
	vector<idx_t> right_projection_map_knows{1, 2};
	auto join_person_knows = make_unique<PhysicalSIPJoin>(context, join_person1, move(scan_function_person),
	                                                      move(scan_function_knows), move(cond_person_knows), join_person1.join_type,
	                                                      left_projection_map, right_projection_map_knows);*/

	// join person2 with exisitng knows
	vector<idx_t> knows_ids{0, 3};
	vector<TypeId> get_knows_types{TypeId::VARCHAR, TypeId::INT64};
	unordered_map<idx_t, vector<TableFilter>> knows_filters;
	Value pknows(constantval);
	TableFilter knows_filter(pknows, ExpressionType::COMPARE_EQUAL, 0);
	knows_filters[0].push_back(knows_filter);
	auto scan_function_knows = move(get_scan_function(context, table_edge_knows,
	                                                  table_index_knows, knows_ids,
	                                                  get_knows_types, knows_filters));
	                                                  //get_knows_types, knows_filters));

	vector<idx_t> person2_ids{COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_person2_types{TypeId::INT64};
	auto scan_function_person2 = move(get_scan_function(context, table_vertex_person,
	                                                    table_index_person2, person2_ids, get_person2_types));

	vector<JoinCondition> cond_person2;
	JoinCondition join_condition_person2;
	join_condition_person2.left = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 0);
	join_condition_person2.right = make_unique<BoundReferenceExpression>("id2_rowid", TypeId::INT64, 1);
	join_condition_person2.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_person2 = make_unique<RAIInfo>();
	rai_info_person2->rai = table_hascreator->storage->info->rais[0].get(); // 2d40
	rai_info_person2->rai_type = RAIType::TARGET_EDGE;
	rai_info_person2->forward = true;
	rai_info_person2->vertex = table_person;
	rai_info_person2->vertex_id = table_index_person2;
	rai_info_person2->passing_tables[0] = table_index_person2;
	rai_info_person2->left_cardinalities[0] = table_person->storage->info->cardinality;

	join_condition_person2.rais.push_back(move(rai_info_person2));

	cond_person2.push_back(move(join_condition_person2));

	LogicalComparisonJoin join_person2_op(JoinType::INNER);
	vector<TypeId> output_person2_types{TypeId::INT64, TypeId::INT64};
	vector<idx_t> right_projection_map_person2{1};
	join_person2_op.types = output_person2_types;
	auto join_person2 = make_unique<PhysicalSIPJoin>(context, join_person2_op, move(scan_function_person2),
	                                                 move(scan_function_knows), move(cond_person2), join_person2_op.join_type,
	                                                 left_projection_map, right_projection_map_person2);

	// join forum with existing
	vector<idx_t> forum_ids{1, COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_forum_types{TypeId::VARCHAR, TypeId::INT64};
	auto scan_function_forum = move(get_scan_function(context, table_vertex_forum,
	                                                  table_index_forum, forum_ids, get_forum_types));

	vector<JoinCondition> cond_forum;
	JoinCondition join_condition_forum;
	join_condition_forum.left = make_unique<BoundReferenceExpression>("forum_rowid", TypeId::INT64, 1);
	join_condition_forum.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 0);
	join_condition_forum.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_forum = make_unique<RAIInfo>();
	rai_info_forum->rai = table_hasmember->storage->info->rais[0].get(); // 33040
	rai_info_forum->rai_type = RAIType::SOURCE_EDGE;
	rai_info_forum->forward = false;
	rai_info_forum->vertex = table_forum;
	rai_info_forum->vertex_id = table_index_forum;
	rai_info_forum->passing_tables[0] = table_index_forum;
	rai_info_forum->left_cardinalities[0] = table_forum->storage->info->cardinality;
	//rai_info_forum->compact_list = &rai_info_forum->rai->alist->compact_backward_list; // 20b8

	join_condition_forum.rais.push_back(move(rai_info_forum));

	cond_forum.push_back(move(join_condition_forum));

	LogicalComparisonJoin join_forum_op(JoinType::INNER);
	vector<TypeId> output_forum_types{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64};
	vector<idx_t> right_projection_map_forum{0};
	vector<idx_t> merge_projection_map;
	join_forum_op.types = output_forum_types;
	auto join_forum = make_unique<PhysicalMergeSIPJoin>(context, join_forum_op, move(scan_function_forum),
	                                                    move(join_person2), move(cond_forum), join_forum_op.join_type,
	                                                    left_projection_map, right_projection_map_forum, merge_projection_map);

	// join post with forum
	vector<idx_t> post_ids1{0, COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_post_types1{TypeId::VARCHAR, TypeId::INT64};
	auto scan_function_post = move(get_scan_function(context, table_vertex_post,
	                                                  table_index_post, post_ids1, get_post_types1));

	vector<JoinCondition> cond_post1;
	JoinCondition join_condition_post1;
	join_condition_post1.left = make_unique<BoundReferenceExpression>("post_rowid", TypeId::INT64, 1);
	join_condition_post1.right = make_unique<BoundReferenceExpression>("forum_rowid", TypeId::INT64, 1);
	join_condition_post1.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_post1 = make_unique<RAIInfo>();
	rai_info_post1->rai = table_containerof->storage->info->rais[0].get(); // 33040
	rai_info_post1->rai_type = RAIType::TARGET_EDGE;
	rai_info_post1->forward = true;
	rai_info_post1->vertex = table_post;
	rai_info_post1->vertex_id = table_index_post;
	rai_info_post1->passing_tables[0] = table_index_post;
	rai_info_post1->left_cardinalities[0] = table_post->storage->info->cardinality;
	// rai_info_post1->compact_list = &rai_info_post1->rai->alist->compact_forward_list; // 20b8

	join_condition_post1.rais.push_back(move(rai_info_post1));

	cond_post1.push_back(move(join_condition_post1));

	LogicalComparisonJoin join_post_op(JoinType::INNER);
	vector<TypeId> output_post_types{TypeId::VARCHAR, TypeId::INT64, TypeId::VARCHAR, TypeId::INT64};
	vector<idx_t> right_projection_map_post{0, 2}; // reserve column 0 and 2 in the right table, and drop the other columns to join
	join_post_op.types = output_post_types;
	auto join_post1 = make_unique<PhysicalMergeSIPJoin>(context, join_post_op, move(scan_function_post),
	                                                    move(join_forum), move(cond_post1), join_post_op.join_type,
	                                                    left_projection_map, right_projection_map_post, merge_projection_map);

	// join hascreator with existing tables
	vector<idx_t> hascreator_ids{2, 3, 0};
	vector<TypeId> get_hascreator_types{TypeId::INT64, TypeId::INT64, TypeId::VARCHAR};
	auto scan_function_hascreator = move(get_scan_function(context, table_edge_hascreator,
	                                                 table_index_hascreator, hascreator_ids, get_hascreator_types));

	vector<JoinCondition> cond_hascreator;
	JoinCondition join_condition_hascreator1, join_condition_hascreator2;
	join_condition_hascreator1.left = make_unique<BoundReferenceExpression>("personid_rowid", TypeId::INT64, 1);
	join_condition_hascreator1.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 3);
	join_condition_hascreator1.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_hascreator1 = make_unique<RAIInfo>();
	rai_info_hascreator1->rai = table_hascreator->storage->info->rais[0].get(); // 33040
	rai_info_hascreator1->rai_type = RAIType::EDGE_TARGET;
	rai_info_hascreator1->forward = false;
	rai_info_hascreator1->vertex = table_person;
	rai_info_hascreator1->vertex_id = table_index_person2;
	rai_info_hascreator1->passing_tables[0] = table_index_hascreator;
	rai_info_hascreator1->left_cardinalities[0] = table_hascreator->storage->info->cardinality;
	rai_info_hascreator1->compact_list = &rai_info_hascreator1->rai->alist->compact_backward_list; // 20b8

	join_condition_hascreator1.rais.push_back(move(rai_info_hascreator1));

	join_condition_hascreator2.left = make_unique<BoundReferenceExpression>("postid", TypeId::VARCHAR, 2);
	join_condition_hascreator2.right = make_unique<BoundReferenceExpression>("id", TypeId::VARCHAR, 0);
	join_condition_hascreator2.comparison = ExpressionType::COMPARE_EQUAL;

	cond_hascreator.push_back(move(join_condition_hascreator1));
	cond_hascreator.push_back(move(join_condition_hascreator2));

	LogicalComparisonJoin join_hascreator_op(JoinType::INNER);
	vector<TypeId> output_hascreator_types{TypeId::INT64, TypeId::INT64, TypeId::VARCHAR, TypeId::VARCHAR};
	join_hascreator_op.types = output_hascreator_types;
	vector<idx_t> right_projection_map_hascreator{2};
	auto join_hascreator = make_unique<PhysicalSIPJoin>(context, join_hascreator_op, move(scan_function_hascreator),
	                                                    move(join_post1), move(cond_hascreator), join_hascreator_op.join_type,
	                                                    left_projection_map, right_projection_map_hascreator);

	// project
	vector<TypeId> result_types{TypeId::VARCHAR};
	vector<unique_ptr<Expression>> select_list;
	auto result_col0 = make_unique<BoundReferenceExpression>("title", TypeId::VARCHAR, 3);
	select_list.push_back(move(result_col0));
	auto projection = make_unique<PhysicalProjection>(result_types, move(select_list));
	projection->children.push_back(move(join_hascreator));

	// execute!
	DataChunk result_chunk;
	result_chunk.Initialize(projection->types);
	auto state = projection->GetOperatorState();

	std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> start =
	    std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

	long long int result_num = 0;
	do {
		projection->GetChunk(*con.context, result_chunk, state.get());
		result_num += result_chunk.size();
		// result_chunk.Print();
	} while (result_chunk.size() > 0);

	std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> end =
	    std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

	std::cout << "Execution Time Cost: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
	std::cout << "Result Num: " << result_num << std::endl;

	rnum = result_num;
	return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

int main() {
	// (a) -[b]
	// test_trivial_ab();

	// (a)-[b]-(c)
	// test_trivial_abc();

	// (a)-[b]-(c) with sjoin and merge join
	// test_sjoin_merge_abc();

	//string constantval = "4398046511628";
	// string constantval = "893";

	int count_num = 50;
	int counter = 0;
	vector<string> constantval_list;
	getStringListFromFile("../../../resource/sample/person_0_0.csv", 0, count_num, constantval_list);
	// constantval_list.push_back("0");

	int duck_time = 0;
	int grain_time = 0;
	int grain_ldbc_time = 0;
	int gopt_time = 0;

	for (const auto& constantval : constantval_list) {
		std::cout << "Testing " << constantval << " " << counter++ << "/" << count_num << std::endl;
		std::cout << "DUCK PLAN WITH CONSTRAINT ON PERSON1: " << std::endl;
		int r0num = 0;
		duck_time += test_duckdb_ldbc(constantval, r0num);

		std::cout << "GRAIN PLAN WITH CONSTRAINT ON PERSON1: " << std::endl;
		int r1num = 0;
		grain_time += test_graindb_smergejoin_person_constrained(constantval, r1num);

		std::cout << "GRAIN PLAN WITH LDBC QUERY: " << std::endl;
		int r2num = 0;
		grain_ldbc_time += test_graindb_smergejoin_ldbc(constantval, r2num);

		std::cout << "GOPT PLAN: " << std::endl;
		int r3num = 0;
		gopt_time += test_gopt_join(constantval, r3num);

		assert(r0num == r3num);
		assert(r1num == r3num);
		assert(r2num == r3num);
	}

	std::cout << "Duck Avg Time Cost: " << (double) duck_time / constantval_list.size() << std::endl;
	std::cout << "GRain Avg Time Cost: " << (double) grain_time / constantval_list.size() << std::endl;
	std::cout << "GRain LDBC Avg Time Cost: " << (double) grain_ldbc_time / constantval_list.size() << std::endl;
	std::cout << "GOpt Avg Time Cost: " << (double) gopt_time / constantval_list.size() << std::endl;

	return 0;
}