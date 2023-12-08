//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/column_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/to_string.hpp"

#include <functional>

using std::hash;

namespace duckdb {
class Serializer;
class Deserializer;

struct ColumnBinding {
	idx_t table_index;
	// This index is local to a Binding, and has no meaning outside of the context of the Binding that created it
	idx_t column_index;

    //  added
    idx_t column_ordinal;
    TableCatalogEntry *table;

	ColumnBinding() : table_index(DConstants::INVALID_INDEX), column_index(DConstants::INVALID_INDEX), column_ordinal(DConstants::INVALID_INDEX), table(nullptr) {
	}
	ColumnBinding(idx_t table, idx_t column) : table_index(table), column_index(column), column_ordinal(column), table(nullptr) {
	}
    ColumnBinding(idx_t table_index, idx_t column_index, idx_t column_ordinal)
            : table_index(table_index), column_index(column_index), column_ordinal(column_ordinal), table(nullptr) {
    }
    ColumnBinding(idx_t table_index, idx_t column_index, TableCatalogEntry *table)
            : table_index(table_index), column_index(column_index), column_ordinal(column_index), table(table) {
    }
    ColumnBinding(idx_t table_index, idx_t column_index, idx_t column_ordinal, TableCatalogEntry *table)
            : table_index(table_index), column_index(column_index), column_ordinal(column_ordinal), table(table) {
    }

	string ToString() const {
		return "#[" + to_string(table_index) + "." + to_string(column_index) + "]";
	}

	bool operator==(const ColumnBinding &rhs) const {
		return table_index == rhs.table_index && column_index == rhs.column_index;
	}

	bool operator!=(const ColumnBinding &rhs) const {
		return !(*this == rhs);
	}

	void Serialize(Serializer &serializer) const;
	static ColumnBinding Deserialize(Deserializer &deserializer);
};

struct ColumnBindingHasher {
    size_t operator()(const ColumnBinding &binding) const {
        return ((hash<idx_t>()(binding.table_index) ^ hash<idx_t>()(binding.column_index) << 1) >> 1);
    }
};

} // namespace duckdb
