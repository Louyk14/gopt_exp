#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_data/create_rai_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;

unique_ptr<CreateStatement> Transformer::TransformCreateRAI(duckdb_libpgquery::PGCreateRAIStmt& stmt) {
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateRAIInfo>();

	info->name = stmt.name->relname;
	auto tableref = make_uniq<BaseTableRef>();
	tableref->table_name = stmt.table->relname;
	if (stmt.table->schemaname) {
		tableref->schema_name = stmt.table->schemaname;
	}
	info->table = move(tableref);
	switch (stmt.direction) {
	case duckdb_libpgquery::PGPostgresRAIDirection::PG_RAI_UNDIRECTED:
		info->direction = RAIDirection::UNDIRECTED;
		break;
	case duckdb_libpgquery::PGPostgresRAIDirection::PG_RAI_SELF:
		info->direction = RAIDirection::SELF;
		break;
	case duckdb_libpgquery::PGPostgresRAIDirection::PG_RAI_DIRECTED:
		info->direction = RAIDirection::DIRECTED;
		break;
	case duckdb_libpgquery::PGPostgresRAIDirection::PG_RAI_PKFK:
		info->direction = RAIDirection::PKFK;
		break;
	}

	auto fromref = make_uniq<BaseTableRef>();
	fromref->table_name = stmt.from_ref->schemaname;
	auto toref = make_uniq<BaseTableRef>();
	toref->table_name = stmt.to_ref->schemaname;

	fromref->alias = fromref->table_name;
	toref->alias = toref->table_name;
	if (fromref->table_name == toref->table_name) {
		fromref->alias = fromref->table_name + "_from";
		toref->alias = toref->table_name + "_to";
	} else {
		if (fromref->table_name == stmt.table->relname) {
			fromref->alias = fromref->table_name + "_from";
		}
		if (toref->table_name == stmt.table->relname) {
			toref->alias = toref->table_name + "_to";
		}
	}

    info->columns.push_back(make_uniq<ColumnRefExpression>(stmt.from_col, stmt.table->relname));
    info->references.push_back(make_uniq<ColumnRefExpression>(stmt.from_ref->relname, fromref->alias));
    info->referenced_tables.push_back(move(fromref));
    info->columns.push_back(make_uniq<ColumnRefExpression>(stmt.to_col, stmt.table->relname));
    info->references.push_back(make_uniq<ColumnRefExpression>(stmt.to_ref->relname, toref->alias));
    info->referenced_tables.push_back(move(toref));

	result->info = move(info);

	return result;
}
