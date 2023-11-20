#include "duckdb/main/extension/generated_extension_loader.hpp"

namespace duckdb{

//! Looks through the CMake-generated list of extensions that are linked into DuckDB currently to try load <extension>
bool TryLoadLinkedExtension(DuckDB &db, const string &extension) {

    if (extension=="parquet") {
        db.LoadExtension<ParquetExtension>();
        return true;
    }
    if (extension=="jemalloc") {
        db.LoadExtension<JemallocExtension>();
        return true;
    }

    return false;
}

vector<string> linked_extensions(1, "parquet");// = {
//	"parquet",
//	"jemalloc"
//};

vector<string> loaded_extension_test_paths = {
};

}
