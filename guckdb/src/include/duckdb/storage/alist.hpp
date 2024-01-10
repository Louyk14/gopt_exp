//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/alist.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/rai_direction.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include <assert.h>

namespace duckdb {

typedef std::vector<bool> bitmask_vector;
typedef std::vector<row_t> rows_vector;

// List for each source vertex (with edges and destination vertices)
struct EList {
	EList() : size(0) {
		edges = make_uniq<std::vector<int64_t>>();
		vertices = make_uniq<std::vector<int64_t>>();
	}

	idx_t size;
    std::unique_ptr<std::vector<int64_t>> edges;
    std::unique_ptr<std::vector<int64_t>> vertices;
};

// adjacency lists stored in CSR format
struct CompactList {
	unique_ptr<idx_t[]> offsets;
	unique_ptr<idx_t[]> sizes;
	unique_ptr<int64_t[]> edges;
	unique_ptr<int64_t[]> vertices;
};

class RAIInfo;

class AList {
public:
	AList(string alias)
	    : alias(std::move(alias)), enabled(false), source_num(0), target_num(0), edge_num(0), src_avg_degree(0.0),
	      dst_avg_degree(0.0) {
	}

	string alias;
	bool enabled; // if alist in stored in CSR format

	//! Append rai tuples (source, edge, target) into alist
	void Append(Vector &source, Vector &edges, Vector &target, idx_t count, RAIDirection direction);
	void AppendPKFK(Vector &source, Vector &edges, idx_t count, RAIDirection direction = RAIDirection::PKFK);
	//! Finalize write to a compact immutable storage layout
	void Finalize(RAIDirection direction);
	//! Fetch <edge, target> pairs from alist based on a given source id, and return selection vector for right chunk
	//! l refers to alist, r refers to rid
	idx_t Fetch(idx_t &lpos, idx_t &rpos, Vector &rvector, idx_t rsize, SelectionVector &rsel, Vector &r0vector,
	            Vector &r1vector, bool forward);
	idx_t FetchVertexes(idx_t &lpos, idx_t &rpos, Vector &rvector, idx_t rsize, SelectionVector &rsel, Vector &r0vector,
	                    bool forward);
    idx_t FetchVertexes(std::vector<idx_t> &lpos, idx_t &rpos, std::vector<Vector*> &rvector, idx_t rsize, SelectionVector &rsel, Vector &r0vector,
                        vector<RAIInfo*>& merge_rais);
	// WARNING: Deprecated
	idx_t BuildZoneFilter(data_ptr_t *hashmap, idx_t size, bitmask_vector &zone_filter, bool forward);
	// WARNING: Deprecated
	idx_t BuildZoneFilterWithExtra(data_ptr_t *hashmap, idx_t size, bitmask_vector &zone_filter,
	                               bitmask_vector &extra_zone_filter, bool forward);
	// WARNING: Deprecated
	idx_t BuildRowsFilter(data_ptr_t *hashmap, idx_t size, std::vector<row_t> &rows_filter, bool forward);
	// WARNING: Deprecated
	idx_t BuildRowsFilterWithExtra(data_ptr_t *hashmap, idx_t size, std::vector<row_t> &rows_filter,
                                   std::vector<row_t> &extra_rows_filter, bool forward);
	void Serialize(string path);

public:
	idx_t source_num;
	idx_t target_num;
	idx_t edge_num;
	double src_avg_degree;
	double dst_avg_degree;
	std::unordered_map<int64_t, unique_ptr<EList>> forward_map;
	std::unordered_map<int64_t, unique_ptr<EList>> backward_map;
	CompactList compact_forward_list;
	CompactList compact_backward_list;
};
} // namespace duckdb
