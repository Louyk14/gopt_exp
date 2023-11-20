//
// Created by Louyk on 2023/9/4.
//
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

using namespace duckdb;
using namespace std;

/*
TEST_CASE("Test trivial optimizer without sjoin", "[rai]") {
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
	con.Query("CREATE RAI b_rai ON b (FROM a_id REFERENCES a.id, TO c_id REFERENCES c.id)");

	//! Then query on it
	con.Query("SELECT * FROM a, b, c ON a.id = b.a_id and b.c_id = c.id");
}
*/