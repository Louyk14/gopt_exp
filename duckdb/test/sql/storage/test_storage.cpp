#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/appender.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

TEST_CASE("Test interleaving of insertions/updates/deletes on multiple tables", "[storage][.]") {
	auto config = GetTestConfig();
	duckdb::unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (a INTEGER, b INTEGER);"));
		idx_t test_insert = 0, test_insert2 = 0;
		for (idx_t i = 0; i < 1000; i++) {
			idx_t stage = i % 7;
			switch (stage) {
			case 0:
				for (; test_insert < i; test_insert++) {
					REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (" + to_string(test_insert) + ")"));
				}
				break;
			case 1:
				for (; test_insert2 < i; test_insert2++) {
					REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (" + to_string(test_insert) + ", " +
					                          to_string(test_insert) + " + 2)"));
				}
				break;
			case 2:
				REQUIRE_NO_FAIL(con.Query("UPDATE test SET a = a + 1 WHERE a % 2 = 0"));
				break;
			case 3:
				REQUIRE_NO_FAIL(con.Query("UPDATE test2 SET a = a + 1 WHERE a % 2 = 0"));
				break;
			case 4:
				REQUIRE_NO_FAIL(con.Query("UPDATE test2 SET b = b + 1 WHERE b % 2 = 0"));
				break;
			case 5:
				REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a % 5 = 0"));
				break;
			default:
				REQUIRE_NO_FAIL(con.Query("DELETE FROM test2 WHERE a % 5 = 0"));
				break;
			}
		}
		REQUIRE_NO_FAIL(con.Query("COMMIT"));

		result = con.Query("SELECT SUM(a) FROM test ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {396008}));

		result = con.Query("SELECT SUM(a), SUM(b) FROM test2 ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {403915}));
		REQUIRE(CHECK_COLUMN(result, 1, {405513}));
	}
	// reload the database from disk
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT SUM(a) FROM test ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {396008}));

		result = con.Query("SELECT SUM(a), SUM(b) FROM test2 ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {403915}));
		REQUIRE(CHECK_COLUMN(result, 1, {405513}));
	}
	DeleteDatabase(storage_database);
}

void extractInfo(string& inputstr, std::vector<bool>& filter, char delimiter, string& result) {
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

void extractInfoFile(Connection& con, string filename, string tablename, std::vector<bool>& filter) {
	fstream infile(filename, std::ios::in);
	string schema, data;
	std::getline(infile, schema);
	// std::cout << schema << std::endl;

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
	std::vector<string> table_names{"Person", "Forum", "Post", "Knows", "HasMember", "ContainerOf", "HasCreator"};
	for (int i = 0; i < table_names.size(); ++i) {
		con.Query("DROP TABLE " + table_names[i]);
	}

	con.Query("CREATE TABLE person(id STRING)");
	con.Query("CREATE TABLE Forum(id STRING, title STRING)");
	con.Query("CREATE TABLE Post(id STRING)");
	con.Query("CREATE TABLE Knows(id1 STRING, id2 STRING)");
	con.Query("CREATE TABLE HasMember(forumId STRING, personId STRING)");
	con.Query("CREATE TABLE ContainerOf(forumId STRING, postId STRING)");
	con.Query("CREATE TABLE HasCreator(postId STRING, personId STRING)");

	string prepath = "/Users/louyk/Desktop/dbs/duckdb/resource/sf0.1/";

	std::vector<bool> filter_person{true, false, false, false, false, false, false, false, false, false};
	extractInfoFile(con, prepath + "person_0_0.csv", "Person", filter_person);

	std::vector<bool> filter_forum{true, true, false};
	extractInfoFile(con, prepath + "forum_0_0.csv", "Forum", filter_forum);

	std::vector<bool> filter_post{true, false, false, false, false, false, false, false};
	extractInfoFile(con, prepath + "post_0_0.csv", "Post", filter_post);

	std::vector<bool> filter_knows{true, true, false};
	extractInfoFile(con, prepath + "person_knows_person_0_0.csv", "Knows", filter_knows);

	std::vector<bool> filter_hasmember{true, true, false};
	extractInfoFile(con, prepath + "forum_hasMember_person_0_0.csv", "HasMember", filter_hasmember);

	std::vector<bool> filter_containerof{true, true};
	extractInfoFile(con, prepath + "forum_containerOf_post_0_0.csv", "ContainerOf", filter_containerof);

	std::vector<bool> filter_hascreator{true, true};
	extractInfoFile(con, prepath + "post_hasCreator_person_0_0.csv", "HasCreator", filter_hascreator);
}

TEST_CASE("Test ldbc ic5", "[storage][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	//CreateGraphFromFile(con);
	std::vector<string> table_names{"Person", "Forum", "Post", "Knows", "HasMember", "ContainerOf", "HasCreator"};
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
	    con.Query("INSERT INTO Post VALUES (" + to_string(i) + ")");
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

	auto roq = con.Query("SELECT f.title FROM "
	          "Knows k1, Person p2, HasMember hm, Forum f, ContainerOf cof, Post po, HasCreator hc "
	          "WHERE p2.id = k1.id2 AND p2.id = hm.personId AND f.id = hm.forumId AND f.id = cof.forumId AND "
	          "po.id = cof.postId AND po.id = hc.postId AND p2.id = hc.personId AND k1.id1 = \'0\'");

	// auto roq = con.Query("SELECT f.title FROM "
	//                     "Knows k1, Person p2, HasMember hm, Forum f, ContainerOf cof, Post po, HasCreator hc "
	//                     "WHERE p2.id = k1.id2 AND p2.id = hm.personId AND f.id = hm.forumId AND f.id = cof.forumId AND "
	//                     "po.id = cof.postId AND po.id = hc.postId AND p2.id = hc.personId AND k1.id1 = \'4398046511628\'");

	// auto roq = con.Query("SELECT title FROM Person p1, Knows k1 WHERE p1.id = k1.id2 AND p1.id = 1 AND k1.id1 = 2");

	// auto roq = con.Query("SELECT * FROM Person p1, Knows k1 WHERE p1.id = k1.id1 AND p1.id = 1 AND k1.id1 = 1");

	//auto roq = con.Query("select title from (select title, f.id as f_forumid, p2.id2 as k_person2id from Forum f, HasMember hm, "
	//                     "( select id2 from Knows where id1 = \'4398046511628\') p2 where f.id = hm.forumid and hm.personid = p2.id2) tmp, "
	//                     "HasCreator hc, Post po, ContainerOf cof where tmp.f_forumid = cof.forumid and cof.postid = po.id and po.id = hc.postid "
	//                     "and hc.personid = tmp.k_person2id");

	roq->Print();

}