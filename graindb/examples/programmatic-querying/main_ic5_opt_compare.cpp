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

	infile.close();
}

void extractInfo(string& inputstr, vector<bool>& filter, char delimiter, string& result) {
	int pos = 0;
	int last = 0;
	int indexer = 0;
	std::string token;

	while ((pos = inputstr.find(delimiter, last)) != std::string::npos) {
		if (indexer < filter.size() && filter[indexer]) {
			token = inputstr.substr(last, pos - last);
			result += "\'" + token + "\'" + ',';
		}
		indexer += 1;
		last = pos + 1;
	}
	if (indexer < filter.size() && filter[indexer]) {
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

	string prepath = "../../../resource/sf0.1/";

	vector<bool> filter_person{true, false, false, false, false, false, false, false, false, false};
	extractInfoFile(con, prepath + "person_0_0.csv", "Person", filter_person);

	vector<bool> filter_forum{true, true, false};
	extractInfoFile(con, prepath + "forum_0_0.csv", "Forum", filter_forum);

	vector<bool> filter_post{true, false, false, false, false, false, false, false};
	extractInfoFile(con, prepath + "post_0_0.csv", "Post", filter_post);

	vector<bool> filter_knows{true, true, false};
	extractInfoFile(con, prepath + "person_knows_person_0_0.csv", "Knows", filter_knows);

	vector<bool> filter_hasmember{true, true, false};
	extractInfoFile(con, prepath + "forum_hasMember_person_0_0.csv", "HasMember", filter_hasmember);

	vector<bool> filter_containerof{true, true};
	extractInfoFile(con, prepath + "forum_containerOf_post_0_0.csv", "ContainerOf", filter_containerof);

	vector<bool> filter_hascreator{true, true};
	extractInfoFile(con, prepath + "post_hasCreator_person_0_0.csv", "HasCreator", filter_hascreator);
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


int test_gopt_join_k_p_f_po_p(string constantval, int& rnum, Connection& con) {
	auto &context = *con.context;

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

	// buildIndex(con);

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
	rai_info_forum->compact_list = &rai_info_forum->rai->alist->compact_backward_list; // 20b8

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
	vector<idx_t> post_ids1{COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_post_types1{TypeId::INT64};
	auto scan_function_post = move(get_scan_function(context, table_vertex_post,
	                                                 table_index_post, post_ids1, get_post_types1));

	vector<JoinCondition> cond_post1;
	JoinCondition join_condition_post1;
	join_condition_post1.left = make_unique<BoundReferenceExpression>("post_rowid", TypeId::INT64, 0);
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
	rai_info_post1->compact_list = &rai_info_post1->rai->alist->compact_forward_list; // 20b8

	join_condition_post1.rais.push_back(move(rai_info_post1));

	cond_post1.push_back(move(join_condition_post1));

	LogicalComparisonJoin join_post_op(JoinType::INNER);
	vector<TypeId> output_post_types{TypeId::INT64, TypeId::VARCHAR, TypeId::INT64};
	vector<idx_t> right_projection_map_post{0, 2}; // reserve column 0 and 2 in the right table, and drop the other columns to join
	join_post_op.types = output_post_types;
	auto join_post1 = make_unique<PhysicalMergeSIPJoin>(context, join_post_op, move(scan_function_post),
	                                                    move(join_forum), move(cond_post1), join_post_op.join_type,
	                                                    left_projection_map, right_projection_map_post, merge_projection_map);

	// join hascreator with existing tables
	vector<idx_t> hascreator_ids{2, 3};
	vector<TypeId> get_hascreator_types{TypeId::INT64, TypeId::INT64};
	auto scan_function_hascreator = move(get_scan_function(context, table_edge_hascreator,
	                                                       table_index_hascreator, hascreator_ids, get_hascreator_types));

	vector<JoinCondition> cond_hascreator;
	JoinCondition join_condition_hascreator1, join_condition_hascreator2;
	join_condition_hascreator1.left = make_unique<BoundReferenceExpression>("personid_rowid", TypeId::INT64, 1);
	join_condition_hascreator1.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 2);
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

	join_condition_hascreator2.left = make_unique<BoundReferenceExpression>("postid_rowid", TypeId::INT64, 0);
	join_condition_hascreator2.right = make_unique<BoundReferenceExpression>("post_rowid", TypeId::VARCHAR, 0);
	join_condition_hascreator2.comparison = ExpressionType::COMPARE_EQUAL;

	cond_hascreator.push_back(move(join_condition_hascreator1));
	cond_hascreator.push_back(move(join_condition_hascreator2));

	LogicalComparisonJoin join_hascreator_op(JoinType::INNER);
	vector<TypeId> output_hascreator_types{TypeId::INT64, TypeId::INT64, TypeId::VARCHAR};
	join_hascreator_op.types = output_hascreator_types;
	vector<idx_t> right_projection_map_hascreator{1};
	auto join_hascreator = make_unique<PhysicalSIPJoin>(context, join_hascreator_op, move(scan_function_hascreator),
	                                                    move(join_post1), move(cond_hascreator), join_hascreator_op.join_type,
	                                                    left_projection_map, right_projection_map_hascreator);

	// project
	vector<TypeId> result_types{TypeId::VARCHAR};
	vector<unique_ptr<Expression>> select_list;
	auto result_col0 = make_unique<BoundReferenceExpression>("title", TypeId::VARCHAR, 2);
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

	con.context->transaction.Commit();

	rnum = result_num;
	return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

int test_gopt_join_k_p_f_po_po(string constantval, int& rnum, Connection& con) {
	auto &context = *con.context;

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

	// buildIndex(con);

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
	rai_info_forum->compact_list = &rai_info_forum->rai->alist->compact_backward_list; // 20b8

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
	rai_info_post1->compact_list = &rai_info_post1->rai->alist->compact_forward_list; // 20b8

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
	vector<idx_t> hascreator_ids{2, 3};
	vector<TypeId> get_hascreator_types{TypeId::INT64, TypeId::INT64};
	auto scan_function_hascreator = move(get_scan_function(context, table_edge_hascreator,
	                                                       table_index_hascreator, hascreator_ids, get_hascreator_types));

	vector<JoinCondition> cond_hascreator;
	JoinCondition join_condition_hascreator1, join_condition_hascreator2;
	join_condition_hascreator1.left = make_unique<BoundReferenceExpression>("postid_rowid", TypeId::INT64, 0);
	join_condition_hascreator1.right = make_unique<BoundReferenceExpression>("post_rowid", TypeId::INT64, 1);
	join_condition_hascreator1.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_hascreator1 = make_unique<RAIInfo>();
	rai_info_hascreator1->rai = table_hascreator->storage->info->rais[0].get(); // 33040
	rai_info_hascreator1->rai_type = RAIType::EDGE_SOURCE;
	rai_info_hascreator1->forward = true;
	rai_info_hascreator1->vertex = table_post;
	rai_info_hascreator1->vertex_id = table_index_post;
	rai_info_hascreator1->passing_tables[0] = table_index_hascreator;
	rai_info_hascreator1->left_cardinalities[0] = table_hascreator->storage->info->cardinality;
	rai_info_hascreator1->compact_list = &rai_info_hascreator1->rai->alist->compact_forward_list; // 20b8

	join_condition_hascreator1.rais.push_back(move(rai_info_hascreator1));

	join_condition_hascreator2.left = make_unique<BoundReferenceExpression>("personid_rowid", TypeId::INT64, 1);
	join_condition_hascreator2.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 3);
	join_condition_hascreator2.comparison = ExpressionType::COMPARE_EQUAL;

	cond_hascreator.push_back(move(join_condition_hascreator1));
	cond_hascreator.push_back(move(join_condition_hascreator2));

	LogicalComparisonJoin join_hascreator_op(JoinType::INNER);
	vector<TypeId> output_hascreator_types{TypeId::INT64, TypeId::INT64, TypeId::VARCHAR};
	join_hascreator_op.types = output_hascreator_types;
	vector<idx_t> right_projection_map_hascreator{2};
	auto join_hascreator = make_unique<PhysicalSIPJoin>(context, join_hascreator_op, move(scan_function_hascreator),
	                                                    move(join_post1), move(cond_hascreator), join_hascreator_op.join_type,
	                                                    left_projection_map, right_projection_map_hascreator);

	// project
	vector<TypeId> result_types{TypeId::VARCHAR};
	vector<unique_ptr<Expression>> select_list;
	auto result_col0 = make_unique<BoundReferenceExpression>("title", TypeId::VARCHAR, 2);
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

	con.context->transaction.Commit();

	rnum = result_num;
	return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

int test_gopt_join_k_p_f_ppo_fpo(string constantval, int& rnum, Connection& con) {
	auto &context = *con.context;

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

	// buildIndex(con);

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
	rai_info_forum->compact_list = &rai_info_forum->rai->alist->compact_backward_list; // 20b8

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

	// join post with person2
	vector<idx_t> post_ids1{COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_post_types1{TypeId::INT64};
	auto scan_function_post = move(get_scan_function(context, table_vertex_post,
	                                                 table_index_post, post_ids1, get_post_types1));

	vector<JoinCondition> cond_post1;
	JoinCondition join_condition_post1;
	join_condition_post1.left = make_unique<BoundReferenceExpression>("post_rowid", TypeId::INT64, 0);
	join_condition_post1.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 2);
	join_condition_post1.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_post1 = make_unique<RAIInfo>();
	rai_info_post1->rai = table_hascreator->storage->info->rais[0].get(); // 33040
	rai_info_post1->rai_type = RAIType::SOURCE_EDGE;
	rai_info_post1->forward = false;
	rai_info_post1->vertex = table_post;
	rai_info_post1->vertex_id = table_index_post;
	rai_info_post1->passing_tables[0] = table_index_post;
	rai_info_post1->left_cardinalities[0] = table_post->storage->info->cardinality;
	rai_info_post1->compact_list = &rai_info_post1->rai->alist->compact_backward_list; // 20b8

	join_condition_post1.rais.push_back(move(rai_info_post1));

	cond_post1.push_back(move(join_condition_post1));

	LogicalComparisonJoin join_post_op(JoinType::INNER);
	vector<TypeId> output_post_types{TypeId::INT64, TypeId::VARCHAR, TypeId::INT64};
	vector<idx_t> right_projection_map_post{0, 1}; // reserve column 0 and 2 in the right table, and drop the other columns to join
	join_post_op.types = output_post_types;
	auto join_post1 = make_unique<PhysicalMergeSIPJoin>(context, join_post_op, move(scan_function_post),
	                                                    move(join_forum), move(cond_post1), join_post_op.join_type,
	                                                    left_projection_map, right_projection_map_post, merge_projection_map);

	// join containerof with existing tables
	vector<idx_t> cof_ids{2, 3};
	vector<TypeId> get_cof_types{TypeId::INT64, TypeId::INT64};
	auto scan_function_cof = move(get_scan_function(context, table_edge_containerof,
	                                                       table_index_containerof, cof_ids, get_cof_types));

	vector<JoinCondition> cond_cof;
	JoinCondition join_condition_cof1, join_condition_cof2;
	join_condition_cof1.left = make_unique<BoundReferenceExpression>("forumid_rowid", TypeId::INT64, 0);
	join_condition_cof1.right = make_unique<BoundReferenceExpression>("forum_rowid", TypeId::INT64, 2);
	join_condition_cof1.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_cof1 = make_unique<RAIInfo>();
	rai_info_cof1->rai = table_containerof->storage->info->rais[0].get(); // 33040
	rai_info_cof1->rai_type = RAIType::EDGE_SOURCE;
	rai_info_cof1->forward = true;
	rai_info_cof1->vertex = table_forum;
	rai_info_cof1->vertex_id = table_index_forum;
	rai_info_cof1->passing_tables[0] = table_index_containerof;
	rai_info_cof1->left_cardinalities[0] = table_containerof->storage->info->cardinality;
	rai_info_cof1->compact_list = &rai_info_cof1->rai->alist->compact_forward_list; // 20b8

	join_condition_cof1.rais.push_back(move(rai_info_cof1));

	join_condition_cof2.left = make_unique<BoundReferenceExpression>("postid_rowid", TypeId::INT64, 1);
	join_condition_cof2.right = make_unique<BoundReferenceExpression>("post_rowid", TypeId::VARCHAR, 0);
	join_condition_cof2.comparison = ExpressionType::COMPARE_EQUAL;

	cond_cof.push_back(move(join_condition_cof1));
	cond_cof.push_back(move(join_condition_cof2));

	LogicalComparisonJoin join_cof_op(JoinType::INNER);
	vector<TypeId> output_cof_types{TypeId::INT64, TypeId::INT64, TypeId::VARCHAR};
	join_cof_op.types = output_cof_types;
	vector<idx_t> right_projection_map_cof{1};
	auto join_cof = make_unique<PhysicalSIPJoin>(context, join_cof_op, move(scan_function_cof),
	                                                    move(join_post1), move(cond_cof), join_cof_op.join_type,
	                                                    left_projection_map, right_projection_map_cof);

	// project
	vector<TypeId> result_types{TypeId::VARCHAR};
	vector<unique_ptr<Expression>> select_list;
	auto result_col0 = make_unique<BoundReferenceExpression>("title", TypeId::VARCHAR, 2);
	select_list.push_back(move(result_col0));
	auto projection = make_unique<PhysicalProjection>(result_types, move(select_list));
	projection->children.push_back(move(join_cof));

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

	con.context->transaction.Commit();

	rnum = result_num;
	return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

int test_gopt_join_k_p_po_f_f(string constantval, int& rnum, Connection& con) {
	auto &context = *con.context;

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	// select * from a, b, c where a.id = b.a_id and b.c_id = c.id;

	vector<idx_t> left_projection_map, right_projection_map, merge_projection_map;;

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

	// buildIndex(con);

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

	// join post with existing
	vector<idx_t> post_ids1{COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_post_types1{TypeId::INT64};
	auto scan_function_post = move(get_scan_function(context, table_vertex_post,
	                                                 table_index_post, post_ids1, get_post_types1));

	vector<JoinCondition> cond_post1;
	JoinCondition join_condition_post1;
	join_condition_post1.left = make_unique<BoundReferenceExpression>("post_rowid", TypeId::INT64, 0);
	join_condition_post1.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 0);
	join_condition_post1.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_post1 = make_unique<RAIInfo>();
	rai_info_post1->rai = table_hascreator->storage->info->rais[0].get(); // 33040
	rai_info_post1->rai_type = RAIType::SOURCE_EDGE;
	rai_info_post1->forward = false;
	rai_info_post1->vertex = table_post;
	rai_info_post1->vertex_id = table_index_post;
	rai_info_post1->passing_tables[0] = table_index_post;
	rai_info_post1->left_cardinalities[0] = table_post->storage->info->cardinality;
	rai_info_post1->compact_list = &rai_info_post1->rai->alist->compact_backward_list; // 20b8

	join_condition_post1.rais.push_back(move(rai_info_post1));

	cond_post1.push_back(move(join_condition_post1));

	LogicalComparisonJoin join_post_op(JoinType::INNER);
	vector<TypeId> output_post_types{TypeId::INT64, TypeId::INT64};
	vector<idx_t> right_projection_map_post{0}; // reserve column 0 and 2 in the right table, and drop the other columns to join
	join_post_op.types = output_post_types;
	auto join_post1 = make_unique<PhysicalMergeSIPJoin>(context, join_post_op, move(scan_function_post),
	                                                    move(join_person2), move(cond_post1), join_post_op.join_type,
	                                                    left_projection_map, right_projection_map_post, merge_projection_map);

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
	vector<TypeId> output_forum_types{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64};
	vector<idx_t> right_projection_map_forum{1};
	join_forum_op.types = output_forum_types;
	auto join_forum = make_unique<PhysicalMergeSIPJoin>(context, join_forum_op, move(scan_function_forum),
	                                                    move(join_post1), move(cond_forum), join_forum_op.join_type,
	                                                    left_projection_map, right_projection_map_forum, merge_projection_map);


	// join hasmember with existing tables
	vector<idx_t> hasmember_ids{2, 3};
	vector<TypeId> get_hasmember_types{TypeId::INT64, TypeId::INT64};
	auto scan_function_hasmember = move(get_scan_function(context, table_edge_hasmember,
	                                                       table_index_hasmember, hasmember_ids, get_hasmember_types));

	vector<JoinCondition> cond_hasmember;
	JoinCondition join_condition_hasmember1, join_condition_hasmember2;
	join_condition_hasmember1.left = make_unique<BoundReferenceExpression>("forumid_rowid", TypeId::INT64, 0);
	join_condition_hasmember1.right = make_unique<BoundReferenceExpression>("forum_rowid", TypeId::INT64, 1);
	join_condition_hasmember1.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_hasmember1 = make_unique<RAIInfo>();
	rai_info_hasmember1->rai = table_hasmember->storage->info->rais[0].get(); // 33040
	rai_info_hasmember1->rai_type = RAIType::EDGE_SOURCE;
	rai_info_hasmember1->forward = true;
	rai_info_hasmember1->vertex = table_forum;
	rai_info_hasmember1->vertex_id = table_index_forum;
	rai_info_hasmember1->passing_tables[0] = table_index_hasmember;
	rai_info_hasmember1->left_cardinalities[0] = table_hasmember->storage->info->cardinality;
	rai_info_hasmember1->compact_list = &rai_info_hasmember1->rai->alist->compact_forward_list; // 20b8

	join_condition_hasmember1.rais.push_back(move(rai_info_hasmember1));

	join_condition_hasmember2.left = make_unique<BoundReferenceExpression>("personid_rowid", TypeId::INT64, 1);
	join_condition_hasmember2.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 2);
	join_condition_hasmember2.comparison = ExpressionType::COMPARE_EQUAL;

	cond_hasmember.push_back(move(join_condition_hasmember1));
	cond_hasmember.push_back(move(join_condition_hasmember2));

	LogicalComparisonJoin join_hasmember_op(JoinType::INNER);
	vector<TypeId> output_hasmember_types{TypeId::INT64, TypeId::INT64, TypeId::VARCHAR};
	join_hasmember_op.types = output_hasmember_types;
	vector<idx_t> right_projection_map_hasmember{0};
	auto join_hasmember = make_unique<PhysicalSIPJoin>(context, join_hasmember_op, move(scan_function_hasmember),
	                                                    move(join_forum), move(cond_hasmember), join_hasmember_op.join_type,
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

	con.context->transaction.Commit();

	rnum = result_num;
	return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

int test_gopt_join_k_p_po_f_p(string constantval, int& rnum, Connection& con) {
	auto &context = *con.context;

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	// select * from a, b, c where a.id = b.a_id and b.c_id = c.id;

	vector<idx_t> left_projection_map, right_projection_map, merge_projection_map;;

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

	// buildIndex(con);

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

	// join post with existing
	vector<idx_t> post_ids1{COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_post_types1{TypeId::INT64};
	auto scan_function_post = move(get_scan_function(context, table_vertex_post,
	                                                 table_index_post, post_ids1, get_post_types1));

	vector<JoinCondition> cond_post1;
	JoinCondition join_condition_post1;
	join_condition_post1.left = make_unique<BoundReferenceExpression>("post_rowid", TypeId::INT64, 0);
	join_condition_post1.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 0);
	join_condition_post1.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_post1 = make_unique<RAIInfo>();
	rai_info_post1->rai = table_hascreator->storage->info->rais[0].get(); // 33040
	rai_info_post1->rai_type = RAIType::SOURCE_EDGE;
	rai_info_post1->forward = false;
	rai_info_post1->vertex = table_post;
	rai_info_post1->vertex_id = table_index_post;
	rai_info_post1->passing_tables[0] = table_index_post;
	rai_info_post1->left_cardinalities[0] = table_post->storage->info->cardinality;
	rai_info_post1->compact_list = &rai_info_post1->rai->alist->compact_backward_list; // 20b8

	join_condition_post1.rais.push_back(move(rai_info_post1));

	cond_post1.push_back(move(join_condition_post1));

	LogicalComparisonJoin join_post_op(JoinType::INNER);
	vector<TypeId> output_post_types{TypeId::INT64, TypeId::INT64};
	vector<idx_t> right_projection_map_post{0}; // reserve column 0 and 2 in the right table, and drop the other columns to join
	join_post_op.types = output_post_types;
	auto join_post1 = make_unique<PhysicalMergeSIPJoin>(context, join_post_op, move(scan_function_post),
	                                                    move(join_person2), move(cond_post1), join_post_op.join_type,
	                                                    left_projection_map, right_projection_map_post, merge_projection_map);

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
	vector<TypeId> output_forum_types{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64};
	vector<idx_t> right_projection_map_forum{1};
	join_forum_op.types = output_forum_types;
	auto join_forum = make_unique<PhysicalMergeSIPJoin>(context, join_forum_op, move(scan_function_forum),
	                                                    move(join_post1), move(cond_forum), join_forum_op.join_type,
	                                                    left_projection_map, right_projection_map_forum, merge_projection_map);


	// join hasmember with existing tables
	vector<idx_t> hasmember_ids{2, 3};
	vector<TypeId> get_hasmember_types{TypeId::INT64, TypeId::INT64};
	auto scan_function_hasmember = move(get_scan_function(context, table_edge_hasmember,
	                                                      table_index_hasmember, hasmember_ids, get_hasmember_types));

	vector<JoinCondition> cond_hasmember;
	JoinCondition join_condition_hasmember1, join_condition_hasmember2;
	join_condition_hasmember1.left = make_unique<BoundReferenceExpression>("personid_rowid", TypeId::INT64, 1);
	join_condition_hasmember1.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 2);
	join_condition_hasmember1.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_hasmember1 = make_unique<RAIInfo>();
	rai_info_hasmember1->rai = table_hasmember->storage->info->rais[0].get(); // 33040
	rai_info_hasmember1->rai_type = RAIType::EDGE_TARGET;
	rai_info_hasmember1->forward = false;
	rai_info_hasmember1->vertex = table_person;
	rai_info_hasmember1->vertex_id = table_index_person2;
	rai_info_hasmember1->passing_tables[0] = table_index_hasmember;
	rai_info_hasmember1->left_cardinalities[0] = table_hasmember->storage->info->cardinality;
	rai_info_hasmember1->compact_list = &rai_info_hasmember1->rai->alist->compact_backward_list; // 20b8

	join_condition_hasmember1.rais.push_back(move(rai_info_hasmember1));

	join_condition_hasmember2.left = make_unique<BoundReferenceExpression>("forumid_rowid", TypeId::INT64, 0);
	join_condition_hasmember2.right = make_unique<BoundReferenceExpression>("forum_rowid", TypeId::INT64, 1);
	join_condition_hasmember2.comparison = ExpressionType::COMPARE_EQUAL;

	cond_hasmember.push_back(move(join_condition_hasmember1));
	cond_hasmember.push_back(move(join_condition_hasmember2));

	LogicalComparisonJoin join_hasmember_op(JoinType::INNER);
	vector<TypeId> output_hasmember_types{TypeId::INT64, TypeId::INT64, TypeId::VARCHAR};
	join_hasmember_op.types = output_hasmember_types;
	vector<idx_t> right_projection_map_hasmember{0};
	auto join_hasmember = make_unique<PhysicalSIPJoin>(context, join_hasmember_op, move(scan_function_hasmember),
	                                                   move(join_forum), move(cond_hasmember), join_hasmember_op.join_type,
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

	con.context->transaction.Commit();

	rnum = result_num;
	return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

int test_gopt_join_k_p_po_pf_pof(string constantval, int& rnum, Connection& con) {
	auto &context = *con.context;

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	// select * from a, b, c where a.id = b.a_id and b.c_id = c.id;

	vector<idx_t> left_projection_map, right_projection_map, merge_projection_map;;

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

	// buildIndex(con);

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

	// join post with existing
	vector<idx_t> post_ids1{COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_post_types1{TypeId::INT64};
	auto scan_function_post = move(get_scan_function(context, table_vertex_post,
	                                                 table_index_post, post_ids1, get_post_types1));

	vector<JoinCondition> cond_post1;
	JoinCondition join_condition_post1;
	join_condition_post1.left = make_unique<BoundReferenceExpression>("post_rowid", TypeId::INT64, 0);
	join_condition_post1.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 0);
	join_condition_post1.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_post1 = make_unique<RAIInfo>();
	rai_info_post1->rai = table_hascreator->storage->info->rais[0].get(); // 33040
	rai_info_post1->rai_type = RAIType::SOURCE_EDGE;
	rai_info_post1->forward = false;
	rai_info_post1->vertex = table_post;
	rai_info_post1->vertex_id = table_index_post;
	rai_info_post1->passing_tables[0] = table_index_post;
	rai_info_post1->left_cardinalities[0] = table_post->storage->info->cardinality;
	rai_info_post1->compact_list = &rai_info_post1->rai->alist->compact_backward_list; // 20b8

	join_condition_post1.rais.push_back(move(rai_info_post1));

	cond_post1.push_back(move(join_condition_post1));

	LogicalComparisonJoin join_post_op(JoinType::INNER);
	vector<TypeId> output_post_types{TypeId::INT64, TypeId::INT64};
	vector<idx_t> right_projection_map_post{0}; // reserve column 0 and 2 in the right table, and drop the other columns to join
	join_post_op.types = output_post_types;
	auto join_post1 = make_unique<PhysicalMergeSIPJoin>(context, join_post_op, move(scan_function_post),
	                                                    move(join_person2), move(cond_post1), join_post_op.join_type,
	                                                    left_projection_map, right_projection_map_post, merge_projection_map);

	// join forum with existing (person2)
	vector<idx_t> forum_ids{1, COLUMN_IDENTIFIER_ROW_ID};
	vector<TypeId> get_forum_types{TypeId::VARCHAR, TypeId::INT64};
	auto scan_function_forum = move(get_scan_function(context, table_vertex_forum,
	                                                  table_index_forum, forum_ids, get_forum_types));

	vector<JoinCondition> cond_forum;
	JoinCondition join_condition_forum;
	join_condition_forum.left = make_unique<BoundReferenceExpression>("forum_rowid", TypeId::INT64, 1);
	join_condition_forum.right = make_unique<BoundReferenceExpression>("person_rowid", TypeId::INT64, 1);
	join_condition_forum.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_forum = make_unique<RAIInfo>();
	rai_info_forum->rai = table_hasmember->storage->info->rais[0].get(); // 33040
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
	vector<TypeId> output_forum_types{TypeId::VARCHAR, TypeId::INT64, TypeId::INT64};
	vector<idx_t> right_projection_map_forum{0};
	join_forum_op.types = output_forum_types;
	auto join_forum = make_unique<PhysicalMergeSIPJoin>(context, join_forum_op, move(scan_function_forum),
	                                                    move(join_post1), move(cond_forum), join_forum_op.join_type,
	                                                    left_projection_map, right_projection_map_forum, merge_projection_map);


	// join cof with existing tables
	vector<idx_t> cof_ids{2, 3};
	vector<TypeId> get_cof_types{TypeId::INT64, TypeId::INT64};
	auto scan_function_cof = move(get_scan_function(context, table_edge_containerof,
	                                                      table_index_containerof, cof_ids, get_cof_types));

	vector<JoinCondition> cond_cof;
	JoinCondition join_condition_cof1, join_condition_cof2;
	join_condition_cof1.left = make_unique<BoundReferenceExpression>("postid_rowid", TypeId::INT64, 1);
	join_condition_cof1.right = make_unique<BoundReferenceExpression>("post_rowid", TypeId::INT64, 2);
	join_condition_cof1.comparison = ExpressionType::COMPARE_EQUAL;

	auto rai_info_cof1 = make_unique<RAIInfo>();
	rai_info_cof1->rai = table_containerof->storage->info->rais[0].get(); // 33040
	rai_info_cof1->rai_type = RAIType::EDGE_TARGET;
	rai_info_cof1->forward = false;
	rai_info_cof1->vertex = table_post;
	rai_info_cof1->vertex_id = table_index_post;
	rai_info_cof1->passing_tables[0] = table_index_containerof;
	rai_info_cof1->left_cardinalities[0] = table_containerof->storage->info->cardinality;
	rai_info_cof1->compact_list = &rai_info_cof1->rai->alist->compact_backward_list; // 20b8

	join_condition_cof1.rais.push_back(move(rai_info_cof1));

	join_condition_cof2.left = make_unique<BoundReferenceExpression>("forumid_rowid", TypeId::INT64, 0);
	join_condition_cof2.right = make_unique<BoundReferenceExpression>("forum_rowid", TypeId::INT64, 1);
	join_condition_cof2.comparison = ExpressionType::COMPARE_EQUAL;

	cond_cof.push_back(move(join_condition_cof1));
	cond_cof.push_back(move(join_condition_cof2));

	LogicalComparisonJoin join_cof_op(JoinType::INNER);
	vector<TypeId> output_cof_types{TypeId::INT64, TypeId::INT64, TypeId::VARCHAR};
	join_cof_op.types = output_cof_types;
	vector<idx_t> right_projection_map_cof{0};
	auto join_cof = make_unique<PhysicalSIPJoin>(context, join_cof_op, move(scan_function_cof),
	                                                   move(join_forum), move(cond_cof), join_cof_op.join_type,
	                                                   left_projection_map, right_projection_map_cof);

	// project
	vector<TypeId> result_types{TypeId::VARCHAR};
	vector<unique_ptr<Expression>> select_list;
	auto result_col0 = make_unique<BoundReferenceExpression>("title", TypeId::VARCHAR, 2);
	select_list.push_back(move(result_col0));
	auto projection = make_unique<PhysicalProjection>(result_types, move(select_list));
	projection->children.push_back(move(join_cof));

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

	con.context->transaction.Commit();

	rnum = result_num;
	return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

void create_db_conn(DuckDB& db, Connection& con) {
	con.DisableProfiling();
	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	// CreateGraph(con);
	CreateGraphFromFile(con);

	con.context->transaction.Commit();

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	buildIndex(con);

	con.context->transaction.Commit();
}

int main() {
	//string constantval = "4398046511628";
	// string constantval = "893";

	int count_num = 50;
	int counter = 0;
	vector<string> constantval_list;
	getStringListFromFile("../../../resource/sf0.1/person_0_0.csv", 0, count_num, constantval_list);
	// constantval_list.push_back("0");

	int duck_time = 0;
	int grain_time = 0;
	int grain_ldbc_time = 0;
	int gopt_time = 0;
	int person_post_time = 0;
	int person_forum_time = 0;

	vector<vector<int>> results(6, vector<int>());

	{
		DuckDB db(nullptr);
		Connection con(db);
		create_db_conn(db, con);

		counter = 0;
		for (const auto &constantval : constantval_list) {
			std::cout << "Testing " << constantval << " " << counter++ << "/" << count_num << std::endl;
			std::cout << "test_gopt_join_k_p_f_po_p: " << std::endl;
			int r0num = 0;
			duck_time += test_gopt_join_k_p_f_po_p(constantval, r0num, con);
			results[0].push_back(r0num);
		}
	}

	{
		DuckDB db(nullptr);
		Connection con(db);
		create_db_conn(db, con);

		counter = 0;
		for (const auto &constantval : constantval_list) {
			std::cout << "Testing " << constantval << " " << counter++ << "/" << count_num << std::endl;
			std::cout << "test_gopt_join_k_p_f_po_po: " << std::endl;
			int r1num = 0;
			grain_time += test_gopt_join_k_p_f_po_po(constantval, r1num, con);
			results[1].push_back(r1num);
		}
	}

	{
		DuckDB db(nullptr);
		Connection con(db);
		create_db_conn(db, con);

		counter = 0;
		for (const auto &constantval : constantval_list) {
			std::cout << "Testing " << constantval << " " << counter++ << "/" << count_num << std::endl;
			std::cout << "test_gopt_join_k_p_po_f_f: " << std::endl;
			int r2num = 0;
			grain_ldbc_time += test_gopt_join_k_p_po_f_f(constantval, r2num, con);
			results[2].push_back(r2num);
		}
	}

	{
		DuckDB db(nullptr);
		Connection con(db);
		create_db_conn(db, con);

		counter = 0;
		for (const auto &constantval : constantval_list) {
			std::cout << "Testing " << constantval << " " << counter++ << "/" << count_num << std::endl;
			std::cout << "test_gopt_join_k_p_po_f_p: " << std::endl;
			int r3num = 0;
			gopt_time += test_gopt_join_k_p_po_f_p(constantval, r3num, con);
			results[3].push_back(r3num);
		}
	}

	{
		DuckDB db(nullptr);
		Connection con(db);
		create_db_conn(db, con);

		counter = 0;
		for (const auto &constantval : constantval_list) {
			std::cout << "Testing " << constantval << " " << counter++ << "/" << count_num << std::endl;
			std::cout << "test_gopt_join_k_p_po_pf_pof: " << std::endl;
			int r4num = 0;
			person_post_time += test_gopt_join_k_p_po_pf_pof(constantval, r4num, con);
			results[4].push_back(r4num);
		}
	}

	{
		DuckDB db(nullptr);
		Connection con(db);
		create_db_conn(db, con);

		counter = 0;
		for (const auto &constantval : constantval_list) {
			std::cout << "Testing " << constantval << " " << counter++ << "/" << count_num << std::endl;
			std::cout << "test_gopt_join_k_p_f_ppo_fpo: " << std::endl;
			int r5num = 0;
			person_forum_time += test_gopt_join_k_p_f_ppo_fpo(constantval, r5num, con);
			results[5].push_back(r5num);
		}
	}


	for (int i = 0; i < constantval_list.size(); ++i) {
		for (int j = 1; j < results.size(); ++j) {
			if (results[0][i] != results[j][i]) {
				std::cout << "error" << std::endl;
				assert(1 == 0);
			}
		}
	}

	std::cout << "test_gopt_join_k_p_f_po_p: " << (double) duck_time / constantval_list.size() << std::endl;
	std::cout << "test_gopt_join_k_p_f_po_po: " << (double) grain_time / constantval_list.size() << std::endl;
	std::cout << "test_gopt_join_k_p_f_ppo_fpo: " << (double) person_forum_time / constantval_list.size() << std::endl;
	std::cout << "test_gopt_join_k_p_po_f_f: " << (double) grain_ldbc_time / constantval_list.size() << std::endl;
	std::cout << "test_gopt_join_k_p_po_f_p: " << (double) gopt_time / constantval_list.size() << std::endl;
	std::cout << "test_gopt_join_k_p_po_pf_pof: " << (double) person_post_time / constantval_list.size() << std::endl;

	return 0;
}