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

	con.Query("CREATE TABLE Person(id INT64)");
	con.Query("CREATE TABLE Forum(id STRING, title STRING)");
	con.Query("CREATE TABLE Post(id STRING)");
	con.Query("CREATE TABLE Knows(id1 STRING, id2 STRING)");
	con.Query("CREATE TABLE HasMember(forumId STRING, personId STRING)");
	con.Query("CREATE TABLE ContainerOf(forumId STRING, postId STRING)");
	con.Query("CREATE TABLE HasCreator(postId STRING, personId STRING)");

	string prepath = "../../../resource/sample/";

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

std::time_t convertDateTimeToTimestamp(const std::string& dateTimeStr) {
    std::tm timeStruct = {};
    std::istringstream iss(dateTimeStr);

    iss >> std::get_time(&timeStruct, "%Y-%m-%dT%H:%M:%S");
    if (!iss.good()) {
        // 处理日期时间字符串格式不正确的情况
        throw std::runtime_error("Invalid date time format");
    }

    auto timestamp = std::chrono::system_clock::from_time_t(std::mktime(&timeStruct));
    return std::chrono::duration_cast<std::chrono::milliseconds>(timestamp.time_since_epoch()).count();
}


int main() {
	//string constantval = "4398046511628";
	// string constantval = "893";

	int count_num = 50;
	vector<string> constantval_list;
	// getStringListFromFile("../../../resource/sample/person_0_0.csv", 0, count_num, constantval_list);
	// constantval_list.push_back("0");

    std::string dateTimeStr = "2012-03-24T16:37:35.817+0000";
    std::time_t timestamp = convertDateTimeToTimestamp(dateTimeStr);
    std::cout << "Timestamp: " << timestamp << std::endl;

	vector<vector<int>> results(6, vector<int>());
	DuckDB db(nullptr);
	Connection con(db);
	create_db_conn(db, con);

	for (int i = 0; i < constantval_list.size(); ++i) {
		con.context->transaction.SetAutoCommit(false);
		con.context->transaction.BeginTransaction();

		con.context->SetPbParameters(0, "output/query" + to_string(i) + ".log");
		auto result =
		    con.Query("SELECT f.title FROM "
		              "Knows k1, Person p2, HasMember hm, Forum f, ContainerOf cof, Post po, HasCreator hc "
		              "WHERE p2.id = k1.id2 AND p2.id = hm.personId AND f.id = hm.forumId AND f.id = cof.forumId AND "
		              "po.id = cof.postId AND po.id = hc.postId AND p2.id = hc.personId AND k1.id1 = \'" +
		              constantval_list[i] + "\'");

		con.context->transaction.Commit();

		result->Print();
	}

	return 0;
}