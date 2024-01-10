#include "duckdb/storage/alist.hpp"
#include "duckdb/storage/rai.hpp"
#include <iostream>

using namespace duckdb;
using namespace std;

void AList::AppendPKFK(Vector &source, Vector &edges, idx_t count, RAIDirection direction) {
    UnifiedVectorFormat source_data, edges_data;
	source.ToUnifiedFormat(count, source_data);
	edges.ToUnifiedFormat(count, edges_data);
	auto source_ids = UnifiedVectorFormat::GetData<hash_t>(source_data);
	auto edge_ids = UnifiedVectorFormat::GetData<hash_t>(edges_data);
	for (idx_t i = 0; i < count; i++) {
		auto source_idx = source_data.sel->get_index(i);
		if (!source_data.validity.RowIsValid(source_idx)) {
			continue;
		}
		auto source_rid = source_ids[source_idx];
		auto edge_rid = edge_ids[edges_data.sel->get_index(i)];
		if (forward_map.find(source_rid) == forward_map.end()) {
			auto elist = make_uniq<EList>();
			forward_map[source_rid] = move(elist);
		}
		forward_map[source_rid]->edges->push_back(edge_rid);
		forward_map[source_rid]->size++;
	}
	edge_num += count;
}

void AList::Append(Vector &source, Vector &edges, Vector &target, idx_t count, RAIDirection direction) {
    UnifiedVectorFormat source_data{}, edges_data{}, target_data{};
	source.ToUnifiedFormat(count, source_data);
	edges.ToUnifiedFormat(count, edges_data);
	target.ToUnifiedFormat(count, target_data);
	auto source_ids = UnifiedVectorFormat::GetData<hash_t>(source_data);
	auto edge_ids = UnifiedVectorFormat::GetData<hash_t>(edges_data);
	auto target_ids = UnifiedVectorFormat::GetData<hash_t>(target_data);
	for (idx_t i = 0; i < count; i++) {
		auto source_idx = source_data.sel->get_index(i);
		auto target_idx = target_data.sel->get_index(i);
		if (!source_data.validity.RowIsValid(source_idx) || !target_data.validity.RowIsValid(target_idx)) {
			continue;
		}
		auto source_rid = source_ids[source_idx];
		auto edge_rid = edge_ids[edges_data.sel->get_index(i)];
		auto target_rid = target_ids[target_idx];
		if (forward_map.find(source_rid) == forward_map.end()) {
			auto elist = make_uniq<EList>();
			forward_map[source_rid] = move(elist);
		}
		forward_map[source_rid]->edges->push_back(edge_rid);
		forward_map[source_rid]->vertices->push_back(target_rid);
		forward_map[source_rid]->size++;
	}
	if (direction == RAIDirection::SELF || direction == RAIDirection::UNDIRECTED) {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = source_data.sel->get_index(i);
			auto target_idx = target_data.sel->get_index(i);
			if (!source_data.validity.RowIsValid(source_idx) || !target_data.validity.RowIsValid(target_idx)) {
				continue;
			}
			auto source_rid = source_ids[source_idx];
			auto edge_rid = edge_ids[edges_data.sel->get_index(i)];
			auto target_rid = target_ids[target_idx];
			if (backward_map.find(target_rid) == backward_map.end()) {
				auto elist = make_uniq<EList>();
				backward_map[target_rid] = move(elist);
			}
			backward_map[target_rid]->edges->push_back(edge_rid);
			backward_map[target_rid]->vertices->push_back(source_rid);
			backward_map[target_rid]->size++;
		}
	}
	edge_num += count;
}

void AList::Finalize(RAIDirection direction) {
	if (direction == RAIDirection::PKFK) {
		compact_forward_list.offsets = unique_ptr<idx_t[]>(new idx_t[source_num]);
		compact_forward_list.sizes = unique_ptr<idx_t[]>(new idx_t[source_num]);
		compact_forward_list.edges = unique_ptr<int64_t[]>(new int64_t[edge_num]);
		idx_t current_offset = 0;
		for (idx_t i = 0; i < source_num; i++) {
			if (forward_map.find(i) != forward_map.end()) {
				auto elist_size = forward_map[i]->size;
				memcpy((int64_t *)(compact_forward_list.edges.get()) + current_offset,
				       (int64_t *)(&forward_map[i]->edges->operator[](0)), elist_size * sizeof(int64_t));
				compact_forward_list.sizes[i] = elist_size;
				compact_forward_list.offsets[i] = current_offset;
				current_offset += elist_size;
			} else {
				compact_forward_list.sizes[i] = 0;
				compact_forward_list.offsets[i] = current_offset;
			}
		}
		forward_map.clear();
	} else {
		compact_forward_list.offsets = unique_ptr<idx_t[]>(new idx_t[source_num]);
		compact_forward_list.sizes = unique_ptr<idx_t[]>(new idx_t[source_num]);
		compact_forward_list.edges = unique_ptr<int64_t[]>(new int64_t[edge_num]);
		compact_forward_list.vertices = unique_ptr<int64_t[]>(new int64_t[edge_num]);
		idx_t current_offset = 0;
		for (idx_t i = 0; i < source_num; i++) {
			if (forward_map.find(i) != forward_map.end()) {
				auto elist_size = forward_map[i]->size;
                sort(forward_map[i]->vertices->begin(), forward_map[i]->vertices->begin() + elist_size);
				memcpy((int64_t *)(compact_forward_list.edges.get()) + current_offset,
				       (int64_t *)(&forward_map[i]->edges->operator[](0)), elist_size * sizeof(int64_t));
				memcpy((int64_t *)(compact_forward_list.vertices.get()) + current_offset,
				       (int64_t *)(&forward_map[i]->vertices->operator[](0)), elist_size * sizeof(int64_t));
				compact_forward_list.sizes[i] = elist_size;
				compact_forward_list.offsets[i] = current_offset;
				current_offset += elist_size;
			} else {
				compact_forward_list.sizes[i] = 0;
				compact_forward_list.offsets[i] = current_offset;
			}
		}
		if (backward_map.size() != 0) {
			compact_backward_list.offsets = unique_ptr<idx_t[]>(new idx_t[target_num]);
			compact_backward_list.sizes = unique_ptr<idx_t[]>(new idx_t[target_num]);
			compact_backward_list.edges = unique_ptr<int64_t[]>(new int64_t[edge_num]);
			compact_backward_list.vertices = unique_ptr<int64_t[]>(new int64_t[edge_num]);
			current_offset = 0;
			for (idx_t i = 0; i < target_num; i++) {
				if (backward_map.find(i) != backward_map.end()) {
					auto elist_size = backward_map[i]->size;
                    sort(backward_map[i]->vertices->begin(), backward_map[i]->vertices->begin() + elist_size);
					memcpy((int64_t *)(compact_backward_list.edges.get()) + current_offset,
					       (int64_t *)(&backward_map[i]->edges->operator[](0)), elist_size * sizeof(int64_t));
					memcpy((int64_t *)(compact_backward_list.vertices.get()) + current_offset,
					       (int64_t *)(&backward_map[i]->vertices->operator[](0)), elist_size * sizeof(int64_t));
					compact_backward_list.sizes[i] = elist_size;
					compact_backward_list.offsets[i] = current_offset;
					current_offset += elist_size;
				} else {
					compact_backward_list.sizes[i] = 0;
					compact_backward_list.offsets[i] = current_offset;
				}
			}
		}
		forward_map.clear();
		backward_map.clear();
	}
	enabled = true;
	src_avg_degree = (double)edge_num / (double)source_num;
	dst_avg_degree = (double)edge_num / (double)target_num;
}

static idx_t FetchInternal(CompactList &alist, idx_t &lpos, idx_t &rpos, Vector &rvector, idx_t rsize,
                           SelectionVector &rsel, Vector &r0vector, Vector &r1vector) {
	// assert(r0vector.vector_type == VectorType::FLAT_VECTOR);
	// assert(r1vector.vector_type == VectorType::FLAT_VECTOR);
	if (rpos >= rsize) {
		return 0;
	}

	r0vector.SetVectorType(VectorType::FLAT_VECTOR);
	r1vector.SetVectorType(VectorType::FLAT_VECTOR);
    UnifiedVectorFormat right_data;
	rvector.ToUnifiedFormat(rsize, right_data);
	auto rdata = UnifiedVectorFormat::GetData<hash_t>(right_data);
	auto r0data = (int64_t *)FlatVector::GetData(r0vector);
	auto r1data = (int64_t *)FlatVector::GetData(r1vector);
	idx_t result_count = 0;

    if (!right_data.validity.AllValid()) {
		while (rpos < rsize) {
			idx_t right_position = right_data.sel->get_index(rpos);
			if (!right_data.validity.RowIsValid(right_position)) {
				continue;
			}
			auto rid = rdata[right_position];
			auto offset = alist.offsets.operator[](rid);
			auto length = alist.sizes.operator[](rid);
			auto result_size = std::min(length - lpos, STANDARD_VECTOR_SIZE - result_count);
			memcpy(r0data + result_count, alist.edges.get() + (offset + lpos), result_size * sizeof(int64_t));
			memcpy(r1data + result_count, alist.vertices.get() + (offset + lpos), result_size * sizeof(int64_t));
			for (idx_t i = 0; i < result_size; i++) {
				rsel.set_index(result_count++, rpos);
			}
			lpos += result_size;
			if (lpos == length) {
				lpos = 0;
				rpos++;
			}
			if (result_count == STANDARD_VECTOR_SIZE) {
				return result_count;
			}
		}
	} else {
		while (rpos < rsize) {
			idx_t right_position = right_data.sel->get_index(rpos);
			auto rid = rdata[right_position];
			auto offset = alist.offsets.operator[](rid);
			auto length = alist.sizes.operator[](rid);
			auto result_size = std::min(length - lpos, STANDARD_VECTOR_SIZE - result_count);
			memcpy(r0data + result_count, alist.edges.get() + (offset + lpos), result_size * sizeof(int64_t));
			memcpy(r1data + result_count, alist.vertices.get() + (offset + lpos), result_size * sizeof(int64_t));
			for (idx_t i = 0; i < result_size; i++) {
				rsel.set_index(result_count++, rpos);
			}
			lpos += result_size;
			if (lpos == length) {
				lpos = 0;
				rpos++;
			}
			if (result_count == STANDARD_VECTOR_SIZE) {
				return result_count;
			}
		}
	}

	return result_count;
}

static idx_t FetchVertexesInternal(CompactList &alist, idx_t &lpos, idx_t &rpos, Vector &rvector, idx_t rsize,
                                   SelectionVector &rsel, Vector &r0vector) {
	assert(r0vector.GetVectorType() == VectorType::FLAT_VECTOR);
	if (rpos >= rsize) {
		return 0;
	}

    UnifiedVectorFormat right_data;
	rvector.ToUnifiedFormat(rsize, right_data);
	auto rdata = (int64_t *)right_data.data;
	auto r0data = (int64_t *)FlatVector::GetData(r0vector);
	idx_t result_count = 0;

    if (!right_data.validity.AllValid()) {
        while (rpos < rsize) {
			idx_t right_position = right_data.sel->get_index(rpos);
			if (!right_data.validity.RowIsValid(right_position)) {
				continue;
			}
			auto rid = rdata[right_position];
			auto offset = alist.offsets.operator[](rid);
			auto length = alist.sizes.operator[](rid);
			auto result_size = std::min(length - lpos, STANDARD_VECTOR_SIZE - result_count);
			memcpy(r0data + result_count, alist.vertices.get() + (offset + lpos), result_size * sizeof(int64_t));
			for (idx_t i = 0; i < result_size; i++) {
				rsel.set_index(result_count++, rpos);
			}
			lpos += result_size;
			if (lpos == length) {
				lpos = 0;
				rpos++;
			}
			if (result_count == STANDARD_VECTOR_SIZE) {
				return result_count;
			}
		}
	} else {
		while (rpos < rsize) {
			idx_t right_position = right_data.sel->get_index(rpos);
			auto rid = rdata[right_position];
			auto offset = alist.offsets.operator[](rid);
			auto length = alist.sizes.operator[](rid);
			auto result_size = std::min(length - lpos, STANDARD_VECTOR_SIZE - result_count);
			memcpy(r0data + result_count, alist.vertices.get() + (offset + lpos), result_size * sizeof(int64_t));
			for (idx_t i = 0; i < result_size; i++) {
				rsel.set_index(result_count++, rpos);
			}
			lpos += result_size;
			if (lpos == length) {
				lpos = 0;
				rpos++;
			}
			if (result_count == STANDARD_VECTOR_SIZE) {
				return result_count;
			}
		}
	}
	return result_count;
}

static idx_t FetchVertexesInternalMerge(CompactList &alist, std::vector<idx_t> &lpos, idx_t &rpos, std::vector<Vector*> &rvector, idx_t rsize,
                                   SelectionVector &rsel, Vector &r0vector, const duckdb::vector<RAIInfo*>& merge_rais, const std::vector<CompactList*>& compact_lists) {
    assert(r0vector.GetVectorType() == VectorType::FLAT_VECTOR);
    if (rpos >= rsize) {
        return 0;
    }

    UnifiedVectorFormat right_data;
    rvector[0]->ToUnifiedFormat(rsize, right_data);
    auto rdata = (int64_t *)right_data.data;
    auto r0data = (int64_t *)FlatVector::GetData(r0vector);
    idx_t result_count = 0;

    std::vector<UnifiedVectorFormat> right_selects(merge_rais.size() + 1);
    std::vector<int64_t*> datas(merge_rais.size() + 1);
    for (int i = 1; i < merge_rais.size(); ++i) {
        rvector[i]->ToUnifiedFormat(rsize, right_selects[i]);
        datas[i] = (int64_t*)right_selects[i].data;
    }

    if (!right_data.validity.AllValid()) {
        std::cout << "may be wrong" << std::endl;
        while (rpos < rsize) {
            idx_t right_position = right_data.sel->get_index(rpos);
            if (!right_data.validity.RowIsValid(right_position)) {
                continue;
            }
            auto rid = rdata[right_position];
            auto offset = alist.offsets.operator[](rid);
            auto length = alist.sizes.operator[](rid);
            auto result_size = std::min(length - lpos[0], STANDARD_VECTOR_SIZE - result_count);
            memcpy(r0data + result_count, alist.vertices.get() + (offset + lpos[0]), result_size * sizeof(int64_t));
            for (idx_t i = 0; i < result_size; i++) {
                rsel.set_index(result_count++, rpos);
            }
            lpos[0] += result_size;
            if (lpos[0] == length) {
                lpos[0] = 0;
                rpos++;
            }
            if (result_count == STANDARD_VECTOR_SIZE) {
                return result_count;
            }
        }
    } else {
        while (rpos < rsize) {
            idx_t right_position = right_data.sel->get_index(rpos);
            auto rid = rdata[right_position];
            auto offset = alist.offsets.operator[](rid);
            auto length = alist.sizes.operator[](rid);
            auto result_size = std::min(length - lpos[0], STANDARD_VECTOR_SIZE - result_count);

            std::vector<int> offsetk(merge_rais.size() + 1, 0);
            std::vector<int> lengthk(merge_rais.size() + 1, 0);
            for (int k = 1; k < merge_rais.size(); ++k) {
                auto right_position_k = right_selects[k].sel->get_index(rpos);
                // if (right_position != right_position_k) {
                //     std::cout << rpos << " " << right_position << " " << right_position_k << std::endl;
                // }
                auto rid_other = datas[k][right_position_k];
                offsetk[k] = compact_lists[k]->offsets.operator[](rid_other);
                lengthk[k] = compact_lists[k]->sizes.operator[](rid_other);
            }

            bool possible = true;
            for (int j = 0; j < result_size; ++j) {
                idx_t nid = *(alist.vertices.get() + (offset + lpos[0] + j));
                int agrees = 0;
                // check in other lists
                for (int k = 1; k < merge_rais.size(); ++k) {
                    // if (right_position != right_position_k) {
                    //     std::cout << rpos << " " << right_position << " " << right_position_k << std::endl;
                    // }
                    if (lpos[k] >= lengthk[k]) {
                        possible = false;
                        break;
                    }
                    idx_t nid_other = *(compact_lists[k]->vertices.get() + (offsetk[k] + lpos[k]));
                    if (nid_other < nid) {
                        lpos[k]++;
                        --k;
                        continue;
                    }
                    else if (nid_other == nid) {
                        agrees++;
                        continue;
                    }
                    else
                        break;
                }
                if (!possible)
                    break;
                if (agrees == merge_rais.size() - 1) {
                    memcpy(r0data + result_count, &nid, sizeof(int64_t));
                    rsel.set_index(result_count++, rpos);
                }
            }

            lpos[0] += result_size;
            if (lpos[0] == length) {
                for (int i = 0; i < lpos.size(); ++i)
                    lpos[i] = 0;
                rpos++;
            }
            if (result_count == STANDARD_VECTOR_SIZE) {
                return result_count;
            }
        }
    }
    return result_count;
}

idx_t AList::Fetch(idx_t &lpos, idx_t &rpos, Vector &rvector, idx_t rsize, SelectionVector &rsel, Vector &r0vector,
                   Vector &r1vector, bool forward) {
	if (forward) {
		return FetchInternal(compact_forward_list, lpos, rpos, rvector, rsize, rsel, r0vector, r1vector);
	}
	return FetchInternal(compact_backward_list, lpos, rpos, rvector, rsize, rsel, r0vector, r1vector);
}

idx_t AList::FetchVertexes(idx_t &lpos, idx_t &rpos, Vector &rvector, idx_t rsize, SelectionVector &rsel,
                           Vector &r0vector, bool forward) {
	if (forward) {
		return FetchVertexesInternal(compact_forward_list, lpos, rpos, rvector, rsize, rsel, r0vector);
	}
	return FetchVertexesInternal(compact_backward_list, lpos, rpos, rvector, rsize, rsel, r0vector);
}

idx_t AList::FetchVertexes(std::vector<idx_t> &lpos, idx_t &rpos, std::vector<Vector*> &rvector, idx_t rsize, SelectionVector &rsel,
                           Vector &r0vector, const vector<RAIInfo*>& merge_rais, const std::vector<CompactList*>& compact_lists) {
    if (merge_rais[0]->forward) {
        return FetchVertexesInternalMerge(compact_forward_list, lpos, rpos, rvector, rsize, rsel, r0vector, merge_rais, compact_lists);
    }
    return FetchVertexesInternalMerge(compact_backward_list, lpos, rpos, rvector, rsize, rsel, r0vector, merge_rais, compact_lists);
}

static idx_t BuildZoneFilterInternal(CompactList &alist, data_ptr_t *hashmap, idx_t size, bitmask_vector &zone_filter) {
	idx_t matched_count = 0;
	for (idx_t i = 0; i < size; i++) {
		if (*((data_ptr_t *)(hashmap + i))) {
			auto offset = alist.offsets[i];
			auto length = alist.sizes[i];
			matched_count += length;
			for (idx_t idx = 0; idx < length; idx++) {
				auto edge_id = alist.edges[idx + offset];
				auto zone_id = edge_id / STANDARD_VECTOR_SIZE;
				auto index_in_zone = edge_id - (zone_id * STANDARD_VECTOR_SIZE);
				if (!zone_filter[zone_id]) {
					zone_filter[zone_id] = new bitset<STANDARD_VECTOR_SIZE>();
				}
				//				zone_filter[zone_id]->operator[](index_in_zone) = true;
			}
		}
	}
	return matched_count;
}

idx_t AList::BuildZoneFilter(data_ptr_t *hashmap, idx_t size, bitmask_vector &zone_filter, bool forward) {
	if (forward) {
		return BuildZoneFilterInternal(compact_forward_list, hashmap, size, zone_filter);
	}
	return BuildZoneFilterInternal(compact_backward_list, hashmap, size, zone_filter);
}

static idx_t BuildZoneFilterWithExtraInternal(CompactList &alist, data_ptr_t *hashmap, idx_t size,
                                              bitmask_vector &zone_filter, bitmask_vector &extra_zone_filter) {
	idx_t matched_count = 0;
	for (idx_t i = 0; i < size; i++) {
		if (*((data_ptr_t *)(hashmap + i))) {
			auto offset = alist.offsets[i];
			auto length = alist.sizes[i];
			matched_count += length;
			for (idx_t idx = 0; idx < length; idx++) {
				auto edge_id = alist.edges[idx + offset];
				auto zone_id = edge_id / STANDARD_VECTOR_SIZE;
				auto index_in_zone = edge_id - (zone_id * STANDARD_VECTOR_SIZE);
				if (!zone_filter[zone_id]) {
					zone_filter[zone_id] = new bitset<STANDARD_VECTOR_SIZE>();
				}
				//				zone_filter[zone_id]->operator[](index_in_zone) = true;
			}
			for (idx_t idx = 0; idx < length; idx++) {
				auto edge_id = alist.vertices[idx + offset];
				auto zone_id = edge_id / STANDARD_VECTOR_SIZE;
				auto index_in_zone = edge_id - (zone_id * STANDARD_VECTOR_SIZE);
				if (!extra_zone_filter[zone_id]) {
					extra_zone_filter[zone_id] = new bitset<STANDARD_VECTOR_SIZE>();
				}
				//				extra_zone_filter[zone_id]->operator[](index_in_zone) = true;
			}
		}
	}
	return matched_count;
}

idx_t AList::BuildZoneFilterWithExtra(data_ptr_t *hashmap, idx_t size, bitmask_vector &zone_filter,
                                      bitmask_vector &extra_zone_filter, bool forward) {
	if (forward) {
		return BuildZoneFilterWithExtraInternal(compact_forward_list, hashmap, size, zone_filter, extra_zone_filter);
	}
	return BuildZoneFilterWithExtraInternal(compact_backward_list, hashmap, size, zone_filter, extra_zone_filter);
}

static idx_t BuildRowsFilterInternal(CompactList &alist, data_ptr_t *hashmap, idx_t size, std::vector<row_t> &rows_filter) {
	idx_t matched_count = 0;
	for (idx_t i = 0; i < size; i++) {
		if (*((data_ptr_t *)(hashmap + i))) {
			auto offset = alist.offsets[i];
			auto length = alist.sizes[i];
			for (idx_t idx = 0; idx < length; idx++) {
				rows_filter[matched_count++] = alist.edges[idx + offset];
			}
		}
	}
	return matched_count;
}

idx_t AList::BuildRowsFilter(data_ptr_t *hashmap, idx_t size, std::vector<row_t> &rows_filter, bool forward) {
	if (forward) {
		return BuildRowsFilterInternal(compact_forward_list, hashmap, size, rows_filter);
	}
	return BuildRowsFilterInternal(compact_backward_list, hashmap, size, rows_filter);
}

static idx_t BuildRowsFilterWithExtraInternal(CompactList &alist, data_ptr_t *hashmap, idx_t size,
                                              std::vector<row_t> &rows_filter, std::vector<row_t> &extra_rows_filter) {
	idx_t matched_count = 0;
	idx_t extra_matched_count = 0;
	for (idx_t i = 0; i < size; i++) {
		if (*((data_ptr_t *)(hashmap + i))) {
			auto offset = alist.offsets[i];
			auto length = alist.sizes[i];
			for (idx_t idx = 0; idx < length; idx++) {
				rows_filter[matched_count++] = alist.edges[idx + offset];
			}
			for (idx_t idx = 0; idx < length; idx++) {
				extra_rows_filter[extra_matched_count++] = alist.vertices[idx + offset];
			}
		}
	}
    assert(matched_count == extra_matched_count);
	return matched_count;
}

idx_t AList::BuildRowsFilterWithExtra(data_ptr_t *hashmap, idx_t size, std::vector<row_t> &rows_filter,
                                      std::vector<row_t> &extra_rows_filter, bool forward) {
	if (forward) {
		return BuildRowsFilterWithExtraInternal(compact_forward_list, hashmap, size, rows_filter, extra_rows_filter);
	}
	return BuildRowsFilterWithExtraInternal(compact_backward_list, hashmap, size, rows_filter, extra_rows_filter);
}
