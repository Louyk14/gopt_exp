#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/storage/alist.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/rai.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/execution/operator/list.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test rai append", "[rai]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE a(id INTEGER)");
	con.Query("CREATE TABLE b(a_id INTEGER, c_id INTEGER)");
	con.Query("CREATE TABLE c(id INTEGER)");

	//! Insert values into a
	for (idx_t i = 0; i < 10; i++) {
		auto result = con.Query("INSERT INTO a VALUES (" + to_string(i) + ")");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}
	//! Insert values into c
	for (idx_t i = 0; i < 1500; i++) {
		auto result = con.Query("INSERT INTO c VALUES (" + to_string(i) + ")");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}
	//! Insert values into b
	for (idx_t i = 0; i < 10; i++) {
		auto inserted_tuples = (i * 150) + 1;
		for (idx_t j = 0; j < inserted_tuples; j++) {
			auto result = con.Query("INSERT INTO b VALUES (" + to_string(i) + ", " + to_string(j) + ")");
			REQUIRE(CHECK_COLUMN(result, 0, {1}));
		}
	}
	//! Create RAI on b
	con.Query("CREATE RAI b_rai ON b (FROM a_id REFERENCES a.id, TO c_id REFERENCES c.id)");
	//! Get AList of b_rai
	con.Query("BEGIN TRANSACTION");
	auto &catalog = Catalog::GetCatalog(*con.context);
	auto schema = catalog.GetSchema(*con.context);
	auto table = reinterpret_cast<TableCatalogEntry *>(schema->GetEntry(*con.context, CatalogType::TABLE, "b", true));
	auto &alist = table->storage->info->rais[0]->alist->compact_forward_list;
	//! Check result
	idx_t edge_id = 0;
	for (idx_t i = 0; i < 10; i++) {
		auto expected_count = (i * 150) + 1;
		auto offset = alist.offsets[i];
		REQUIRE(alist.sizes[i] == expected_count);
		for (idx_t j = 0; j < expected_count; j++) {
			REQUIRE(alist.edges[offset + j] == edge_id++);
			REQUIRE(alist.vertices[offset + j] == j);
		}
	}
	con.Query("COMMIT");
}

TEST_CASE("Test rai fetch", "[rai]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

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
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}
	//! Insert values into c
	for (idx_t i = 0; i < V_NUM; i++) {
		result = con.Query("INSERT INTO c VALUES (" + to_string(i) + ")");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
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
		REQUIRE(CHECK_COLUMN(result, 0, values));
	}
	//! Create RAI on b
	con.context->profiler.StartPhase("Build Index");
	con.Query("CREATE RAI b_rai ON b (FROM a_id REFERENCES a.id, TO c_id REFERENCES c.id)");
	con.context->profiler.EndPhase();
	con.Query("SELECT * FROM a JOIN b ON a.id=b.a_id");
	con.context->profiler.Print();

	//! Get AList of b_rai
	con.Query("BEGIN TRANSACTION");
	auto &catalog = Catalog::GetCatalog(*con.context);
	auto schema = catalog.GetSchema(*con.context);
	auto table = reinterpret_cast<TableCatalogEntry *>(schema->GetEntry(*con.context, CatalogType::TABLE, "b", true));
	auto &rai = table->storage->info->rais[0];
	//! Check result
	vector<TypeId> result_types = {TypeId::INT64, TypeId::VARCHAR, TypeId::INT64, TypeId::INT64};
	vector<TypeId> rid_types = {TypeId::INT64};
	vector<TypeId> right_types = {TypeId::INT64, TypeId::VARCHAR};
	ChunkCollection rid_chunks;
	DataChunk rid_chunk;
	rid_chunk.Initialize(rid_types);
	rid_chunk.SetCardinality(S_NUM);
	ChunkCollection right_chunks;
	DataChunk right_chunk;
	right_chunk.Initialize(right_types);
	right_chunk.SetCardinality(S_NUM);
	for (idx_t i = 0; i < S_NUM; i++) {
		rid_chunk.SetValue(0, i, Value::BIGINT(i));
		right_chunk.SetValue(0, i, Value::BIGINT(10244));
		right_chunk.SetValue(1, i, Value::CreateValue("102441024410244"));
	}
	rid_chunks.Append(rid_chunk);
	right_chunks.Append(right_chunk);
	rid_chunks.count = S_NUM;
	right_chunks.count = S_NUM;

	ChunkCollection chunks;
	DataChunk new_chunk;
	new_chunk.Initialize(result_types);
	idx_t left_tuple = 0, right_tuple = 0, right_chunk_idx = 0;
	do {
		if (right_tuple >= rid_chunks.chunks[right_chunk_idx]->size()) {
			right_chunk_idx++;
			if (right_chunk_idx >= rid_chunks.chunks.size()) {
				break;
			}
		}
		new_chunk.Reset();
		auto &rid_chunk = *rid_chunks.chunks[right_chunk_idx];
		auto &right_chunk = *right_chunks.chunks[right_chunk_idx];
		rai->GetChunk(right_chunk, rid_chunk, new_chunk, left_tuple, right_tuple, true);
		chunks.Append(new_chunk);
	} while (new_chunk.size() == 0 || right_chunk_idx < rid_chunks.chunks.size());

	idx_t row_id = 0;
	auto col1 = Value::BIGINT(10244);
	auto col2 = Value::CreateValue("102441024410244");
	for (idx_t i = 0; i < S_NUM; i++) {
		auto count = (i * E_NUM) + 1;
		for (idx_t j = 0; j < count; j++) {
			auto row = chunks.GetRow(row_id);
			REQUIRE(row[0] == col1);
			REQUIRE(row[1] == col2);
			REQUIRE(row[2] == Value::BIGINT(row_id++));
			REQUIRE(row[3] == Value::BIGINT(j));
		}
	}
	REQUIRE(chunks.count == row_id);
	con.Query("COMMIT");
}


TEST_CASE("Test trivial query", "[rai]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

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
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}
	//! Insert values into c
	for (idx_t i = 0; i < V_NUM; i++) {
		result = con.Query("INSERT INTO c VALUES (" + to_string(i) + ")");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
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
		REQUIRE(CHECK_COLUMN(result, 0, values));
	}
	//! Create RAI on b
	con.context->profiler.StartPhase("Build Index");
	con.Query("CREATE RAI b_rai ON b (FROM a_id REFERENCES a.id, TO c_id REFERENCES c.id)");
	con.context->profiler.EndPhase();
	con.Query("(SELECT * FROM a, b WHERE a.id=b.a_id) INTERSECT (SELECT * FROM c, b WHERE c.id=b.a_id)");

	con.Query("SELECT * FROM a, b, c WHERE a.id=b.a_id AND b.c_id = c.id");
	con.context->profiler.Print();
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
	vector<string> table_names{"Person", "Forum", "Post", "Knows", "HasMember", "ContainerOf", "HasCreator"};
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

	vector<bool> filter_person{true, false, false, false, false, false, false, false, false, false};
	extractInfoFile(con, "../../resource/sf1/person_0_0.csv", "Person", filter_person);

	vector<bool> filter_forum{true, true, false};
	extractInfoFile(con, "../../resource/sf1/forum_0_0.csv", "Forum", filter_forum);

	vector<bool> filter_post{true, false, false, false, false, false, false, false};
	extractInfoFile(con, "../../resource/sf1/post_0_0.csv", "Post", filter_post);

	vector<bool> filter_knows{true, true, false};
	extractInfoFile(con, "../../resource/sf1/person_knows_person_0_0.csv", "Knows", filter_knows);

	vector<bool> filter_hasmember{true, true, false};
	extractInfoFile(con, "../../resource/sf1/forum_hasMember_person_0_0.csv", "HasMember", filter_hasmember);

	vector<bool> filter_containerof{true, true};
	extractInfoFile(con, "../../resource/sf1/forum_containerOf_post_0_0.csv", "ContainerOf", filter_containerof);

	vector<bool> filter_hascreator{true, true};
	extractInfoFile(con, "../../resource/sf1/post_hasCreator_person_0_0.csv", "HasCreator", filter_hascreator);
}

TEST_CASE("Test ldbc ic5", "[rai]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	//CreateGraphFromFile(con);
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

	con.Query("CREATE UNDIRECTED RAI knows_rai ON Knows (FROM id1 REFERENCES Person.id, TO id2 REFERENCES Person.id)");
	con.Query("CREATE UNDIRECTED RAI hasMember_rai ON HasMember (FROM forumId REFERENCES Forum.id, TO personId REFERENCES Person.id)");
	// con.Query("CREATE RAI hasMember_rai_back ON HasMember (FROM personId REFERENCES Person.id, TO forumId REFERENCES Forum.id)");
	con.Query("CREATE UNDIRECTED RAI containerOf_rai ON ContainerOf (FROM forumId REFERENCES Forum.id, TO postId REFERENCES Post.id)");
	// con.Query("CREATE RAI containerOf_rai_back ON ContainerOf (FROM postId REFERENCES Post.id, TO forumId REFERENCES Forum.id)");
	con.Query("CREATE UNDIRECTED RAI hasCreator_rai ON HasCreator (FROM postId REFERENCES Post.id, TO personId REFERENCES Person.id)");
	// con.Query("CREATE RAI hasCreator_rai_back ON HasCreator (FROM personId REFERENCES Person.id, TO postId REFERENCES Post.id)");

	// con.Query("CREATE INDEX person_idx ON Person (id)");

	//auto roq = con.Query("SELECT f.title FROM "
	//          "Knows k1, Person p2, HasMember hm, Forum f, ContainerOf cof, Post po, HasCreator hc "
	//          "WHERE p2.id = k1.id2 AND p2.id = hm.personId AND f.id = hm.forumId AND f.id = cof.forumId AND "
	//          "po.id = cof.postId AND po.id = hc.postId AND p2.id = hc.personId AND k1.id1 = \'4398046511628\'");

	//auto roq = con.Query("SELECT p1.id FROM Person p1, Knows k1, Person p2, HasMember hm, Forum f WHERE p1.id = k1.id1 AND k1.id2 = p2.id AND hm.forumId = f.id AND hm.personId = p2.id");

	// auto roq = con.Query("SELECT * FROM Person p1, Knows k1 WHERE p1.id = k1.id1 AND p1.id = 1 AND k1.id1 = 1");

	auto roq = con.Query("select title from (select title, f.id as f_forumid, p2.id2 as k_person2id from Forum f, HasMember hm, "
	                     "( select id2 from Knows where id1 = \'4398046511628\') p2 where f.id = hm.forumid and hm.personid = p2.id2) tmp, "
	                     "HasCreator hc, Post po, ContainerOf cof where tmp.f_forumid = cof.forumid and cof.postid = po.id and po.id = hc.postid "
	                     "and hc.personid = tmp.k_person2id");

	roq->Print();

}