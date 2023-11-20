#include "duckdb/execution/operator/join/physical_hash_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

class PhysicalHashJoinState : public PhysicalComparisonJoinState {
public:
	PhysicalHashJoinState(PhysicalOperator *left, PhysicalOperator *right, vector<JoinCondition> &conditions)
	    : PhysicalComparisonJoinState(left, right, conditions), initialized(false) {
	}

	bool initialized;
	DataChunk cached_chunk;
	DataChunk join_keys;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
};

PhysicalHashJoin::PhysicalHashJoin(ClientContext &context, LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   vector<idx_t> left_projection_map, vector<idx_t> right_projection_map)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, move(cond), join_type),
      right_projection_map(right_projection_map) {
	children.push_back(move(left));
	children.push_back(move(right));

	assert(left_projection_map.size() == 0);

	hash_table =
	    make_unique<JoinHashTable>(BufferManager::GetBufferManager(context), conditions,
	                               LogicalOperator::MapTypes(children[1]->GetTypes(), right_projection_map), type);
}

PhysicalHashJoin::PhysicalHashJoin(ClientContext &context, LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type)
    : PhysicalHashJoin(context, op, move(left), move(right), move(cond), join_type, {}, {}) {
}


void PhysicalHashJoin::BuildHashTable(ClientContext &context, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalHashJoinState *>(state_);

	// build the HT
	auto right_state = children[1]->GetOperatorState();
	auto types = children[1]->GetTypes();

	DataChunk right_chunk, build_chunk;
	right_chunk.Initialize(types);

	if (right_projection_map.size() > 0) {
		build_chunk.Initialize(hash_table->build_types);
	}

	state->join_keys.Initialize(hash_table->condition_types);
	while (true) {
		// get the child chunk
		children[1]->GetChunk(context, right_chunk, right_state.get());
		if (right_chunk.size() == 0) {
			break;
		}
		// resolve the join keys for the right chunk
		state->rhs_executor.Execute(right_chunk, state->join_keys);
		// build the HT
		if (right_projection_map.size() > 0) {
			// there is a projection map: fill the build chunk with the projected columns
			build_chunk.Reset();
			build_chunk.SetCardinality(right_chunk);
			for (idx_t i = 0; i < right_projection_map.size(); i++) {
				build_chunk.data[i].Reference(right_chunk.data[right_projection_map[i]]);
			}
			hash_table->Build(state->join_keys, build_chunk);
		} else {
			// there is not a projected map: place the entire right chunk in the HT
			hash_table->Build(state->join_keys, right_chunk);
		}
	}
	// auto build_finalize_start = std::chrono::high_resolution_clock::now();
	hash_table->Finalize();
	// auto build_finalize_end = std::chrono::high_resolution_clock::now();
	// build_finalize_time += (build_finalize_end - build_finalize_start).count();
}

void PhysicalHashJoin::ProbeHashTable(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalHashJoinState *>(state_);
	if (state->child_chunk.size() > 0 && state->scan_structure) {
		// still have elements remaining from the previous probe (i.e. we got
		// >1024 elements in the previous probe)
		// auto ht_next_start = std::chrono::high_resolution_clock::now();
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
		// auto ht_next_end = std::chrono::high_resolution_clock::now();
		// ht_next_time += (ht_next_end - ht_next_start).count();
		if (chunk.size() > 0) {
			return;
		}
		state->scan_structure = nullptr;
	}

	// probe the HT
	do {
		// fetch the chunk from the left side
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
		// remove any selection vectors
		if (hash_table->size() == 0) {
			// empty hash table, special case
			if (hash_table->join_type == JoinType::ANTI) {
				// anti join with empty hash table, NOP join
				// return the input
				assert(chunk.column_count() == state->child_chunk.column_count());
				chunk.Reference(state->child_chunk);
				return;
			} else if (hash_table->join_type == JoinType::MARK) {
				// MARK join with empty hash table
				assert(hash_table->join_type == JoinType::MARK);
				assert(chunk.column_count() == state->child_chunk.column_count() + 1);
				auto &result_vector = chunk.data.back();
				assert(result_vector.type == TypeId::BOOL);
				// for every data vector, we just reference the child chunk
				chunk.SetCardinality(state->child_chunk);
				for (idx_t i = 0; i < state->child_chunk.column_count(); i++) {
					chunk.data[i].Reference(state->child_chunk.data[i]);
				}
				// for the MARK vector:
				// if the HT has no NULL values (i.e. empty result set), return a vector that has false for every input
				// entry if the HT has NULL values (i.e. result set had values, but all were NULL), return a vector that
				// has NULL for every input entry
				if (!hash_table->has_null) {
					auto bool_result = FlatVector::GetData<bool>(result_vector);
					for (idx_t i = 0; i < chunk.size(); i++) {
						bool_result[i] = false;
					}
				} else {
					FlatVector::Nullmask(result_vector).set();
				}
				return;
			} else if (hash_table->join_type == JoinType::LEFT || hash_table->join_type == JoinType::OUTER ||
			           hash_table->join_type == JoinType::SINGLE) {
				// LEFT/FULL OUTER/SINGLE join and build side is empty
				// for the LHS we reference the data
				chunk.SetCardinality(state->child_chunk.size());
				for (idx_t i = 0; i < state->child_chunk.column_count(); i++) {
					chunk.data[i].Reference(state->child_chunk.data[i]);
				}
				// for the RHS
				for (idx_t k = state->child_chunk.column_count(); k < chunk.column_count(); k++) {
					chunk.data[k].vector_type = VectorType::CONSTANT_VECTOR;
					ConstantVector::SetNull(chunk.data[k], true);
				}
				return;
			}
		}
		// resolve the join keys for the left chunk
		// auto ht_expression_start = std::chrono::high_resolution_clock::now();
		state->lhs_executor.Execute(state->child_chunk, state->join_keys);

		// perform the actual probe
		// auto ht_probe_start = std::chrono::high_resolution_clock::now();
		state->scan_structure = hash_table->Probe(state->join_keys);
		// auto ht_probe_end = std::chrono::high_resolution_clock::now();
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
		// auto ht_next_end = std::chrono::high_resolution_clock::now();
		// ht_expr_time += (ht_probe_start - ht_expression_start).count();
		// ht_probe_time += (ht_probe_end - ht_probe_start).count();
		// ht_next_time += (ht_next_end - ht_probe_end).count();
	} while (chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalHashJoin::GetOperatorState() {
	return make_unique<PhysicalHashJoinState>(children[0].get(), children[1].get(), conditions);
}

void PhysicalHashJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
                                        SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	auto state = reinterpret_cast<PhysicalHashJoinState *>(state_);
	if (!state->initialized) {
		// auto build_start = std::chrono::high_resolution_clock::now();
		state->cached_chunk.Initialize(types);
        if (hash_table->finalized)
            hash_table->Initialize();
		BuildHashTable(context, state_);
		state->initialized = true;
		// auto build_end = std::chrono::high_resolution_clock::now();
		// build_time += (build_end - build_start).count();
		if (hash_table->size() == 0 &&
		    (hash_table->join_type == JoinType::INNER || hash_table->join_type == JoinType::SEMI)) {
			// empty hash table with INNER or SEMI join means empty result set
			return;
		}
	}
	do {
		// auto probe_start = std::chrono::high_resolution_clock::now();
		ProbeHashTable(context, chunk, state);
		// auto probe_end = std::chrono::high_resolution_clock::now();
		// probe_time += (probe_end - probe_start).count();
#if STANDARD_VECTOR_SIZE >= 128
		if (chunk.size() == 0) {
			if (state->cached_chunk.size() > 0) {
				// finished probing but cached data remains, return cached chunk
				chunk.Reference(state->cached_chunk);
				state->cached_chunk.Reset();
			}
			return;
		} else if (chunk.size() < 64) {
			// small chunk: add it to chunk cache and continue
			state->cached_chunk.Append(chunk);
			if (state->cached_chunk.size() >= (STANDARD_VECTOR_SIZE - 64)) {
				// chunk cache full: return it
				chunk.Reference(state->cached_chunk);
				state->cached_chunk.Reset();
				return;
			} else {
				// chunk cache not full: probe again
				chunk.Reset();
			}
		} else {
			return;
		}
#else
		return;
#endif
	} while (true);
}

string PhysicalHashJoin::ExtraRenderInformation() const {
	string extra_info = "HASH_JOIN[";
	extra_info += JoinTypeToString(type);
	extra_info += ", build: ";
	extra_info +=
	    to_string(right_projection_map.size() == 0 ? children[1]->GetTypes().size() : right_projection_map.size());

	if (right_projection_map.empty()) {
		extra_info += " empty ";
	}
	else {
		extra_info += " ";
		for (int i = 0; i < right_projection_map.size(); ++i) {
			extra_info += to_string(right_projection_map[i]) + ",";
		}
	}

	extra_info += "]\n";
	for (auto &it : conditions) {
		string op = ExpressionTypeToOperator(it.comparison);
		BoundReferenceExpression* left = (BoundReferenceExpression*) it.left.get();
		BoundReferenceExpression* right = (BoundReferenceExpression*) it.right.get();
		extra_info += to_string(left->index) + op + to_string(right->index) + "--";
		extra_info += it.left->GetName() + op + it.right->GetName() + "\n";
	}
	return extra_info;
}

string PhysicalHashJoin::GetSubstraitInfo(unordered_map<ExpressionType, idx_t>& func_map, idx_t& func_num, duckdb::idx_t depth) const {
	string join_str = AssignBlank(depth) + "\"hashJoin\": {\n";

	string left_str = AssignBlank(++depth) + "\"left\": {\n";
	string right_str = AssignBlank(depth) + "\"right\": {\n";

	left_str += children[0]->GetSubstraitInfo(func_map, func_num, depth + 1) + AssignBlank(depth) + "},\n";
	right_str += children[1]->GetSubstraitInfo(func_map, func_num, depth + 1) + AssignBlank(depth) + "},\n";

	string type_str = AssignBlank(depth) + "\"type\": ";
	type_str += "\"" + JoinTypeToString(this->type) + "\",\n";

	string common_str = AssignBlank(depth) + "\"common\": {\n";
	string emit_str = AssignBlank(++depth) + "\"emit\": {\n";
	string output_mapping_str = AssignBlank(++depth) + "\"outputMapping\": ";

	string select_right_str = "[\n";

	++depth;

	for (int i = 0; i < right_projection_map.size(); ++i) {
		select_right_str += AssignBlank(depth) + to_string(right_projection_map[i]);
		if (i != right_projection_map.size() - 1) {
			select_right_str += ",\n";
		}
		else {
			select_right_str += "\n";
		}
	}
	select_right_str += AssignBlank(--depth) + "]";
	output_mapping_str += select_right_str + "\n";
	emit_str += output_mapping_str + AssignBlank(--depth) + "}\n";
	common_str += emit_str + AssignBlank(--depth) + "},\n";

	string condition_str = "";
	string left_condition_str = AssignBlank(depth) + "\"leftKeys\": [\n";
	string right_condition_str = AssignBlank(depth) + "\"rightKeys\": [\n";

	++depth;

	for (int i = 0; i < conditions.size(); ++i) {
		BoundReferenceExpression* lexp = (BoundReferenceExpression*) conditions[i].left.get();
		BoundReferenceExpression* rexp = (BoundReferenceExpression*) conditions[i].right.get();

		left_condition_str += AssignBlank(depth) + "{\n";
		left_condition_str += AssignBlank(++depth) + "\"directReference\": {\n";
		left_condition_str += AssignBlank(++depth) + "\"structField\": {\n";
		left_condition_str += AssignBlank(++depth) + "\"field\": " + to_string(lexp->index) + "\n";
		left_condition_str += AssignBlank(--depth) + "}\n";
		left_condition_str += AssignBlank(--depth) + "}\n";
		left_condition_str += AssignBlank(--depth) + "}";

		right_condition_str += AssignBlank(depth) + "{\n";
		right_condition_str += AssignBlank(++depth) + "\"directReference\": {\n";
		right_condition_str += AssignBlank(++depth) + "\"structField\": {\n";
		right_condition_str += AssignBlank(++depth) + "\"field\": " + to_string(rexp->index) + "\n";
		right_condition_str += AssignBlank(--depth) + "}\n";
		right_condition_str += AssignBlank(--depth) + "}\n";
		right_condition_str += AssignBlank(--depth) + "}";

		if (i != conditions.size() - 1) {
			left_condition_str += ",\n";
			right_condition_str += ",\n";
		}
		else {
			left_condition_str += "\n";
			right_condition_str += "\n";
		}
	}

	--depth;

	left_condition_str += AssignBlank(depth) + "],\n";
	right_condition_str += AssignBlank(depth) + "],\n";

	condition_str += left_condition_str + right_condition_str;

	join_str += left_str + right_str + type_str + common_str + condition_str + AssignBlank(--depth) + "}\n";

	return join_str;
}

substrait::Rel* PhysicalHashJoin::ToSubstraitClass(unordered_map<int, string>& tableid2name) const {
	substrait::Rel* hash_join_rel = new substrait::Rel();
	substrait::HashJoinRel* hash_join = new substrait::HashJoinRel();

	hash_join->set_allocated_left(children[0]->ToSubstraitClass(tableid2name));
	hash_join->set_allocated_right(children[1]->ToSubstraitClass(tableid2name));

	substrait::RelCommon* common = new substrait::RelCommon();
	substrait::RelCommon_Emit* emit = new substrait::RelCommon_Emit();

	for (int i = 0; i < right_projection_map.size(); ++i) {
		emit->add_output_mapping(right_projection_map[i]);
	}

	common->set_allocated_emit(emit);
	hash_join->set_allocated_common(common);
    hash_join->set_type(JoinTypeToSubstraitHashJoinType(type));

	for (int i = 0; i < conditions.size(); ++i) {
		BoundReferenceExpression* lexp = (BoundReferenceExpression*) conditions[i].left.get();
		BoundReferenceExpression* rexp = (BoundReferenceExpression*) conditions[i].right.get();

		substrait::Expression_FieldReference* field_reference_left = new substrait::Expression_FieldReference();
		substrait::Expression_ReferenceSegment* direct_reference_left = new substrait::Expression_ReferenceSegment();
		substrait::Expression_ReferenceSegment_MapKey* map_key_variable_left = new substrait::Expression_ReferenceSegment_MapKey();
		substrait::Expression_Literal* variable_name_left = new substrait::Expression_Literal();
		substrait::Expression_ReferenceSegment* child_variable_type_left = new substrait::Expression_ReferenceSegment();
		substrait::Expression_ReferenceSegment_MapKey* map_key_type_left = new substrait::Expression_ReferenceSegment_MapKey();
		substrait::Expression_Literal* type_left = new substrait::Expression_Literal();
		substrait::Expression_ReferenceSegment* child_variable_index_left = new substrait::Expression_ReferenceSegment();
		substrait::Expression_ReferenceSegment_StructField* field_variable_index_left = new substrait::Expression_ReferenceSegment_StructField();

		field_variable_index_left->set_field(lexp->index);
		child_variable_index_left->set_allocated_struct_field(field_variable_index_left);
		string* type_left_str = new string(TypeIdToString(lexp->return_type));
		type_left->set_allocated_string(type_left_str);
		map_key_type_left->set_allocated_map_key(type_left);
		map_key_type_left->set_allocated_child(child_variable_index_left);
		child_variable_type_left->set_allocated_map_key(map_key_type_left);
		variable_name_left->set_allocated_string(&lexp->alias);
		map_key_variable_left->set_allocated_map_key(variable_name_left);
		map_key_variable_left->set_allocated_child(child_variable_type_left);
		direct_reference_left->set_allocated_map_key(map_key_variable_left);
		field_reference_left->set_allocated_direct_reference(direct_reference_left);
		*hash_join->add_left_keys() = *field_reference_left;
        delete field_reference_left;
		// hash_join->mutable_left_keys()->AddAllocated(field_reference_left);

		substrait::Expression_FieldReference* field_reference_right = new substrait::Expression_FieldReference();
		substrait::Expression_ReferenceSegment* direct_reference_right = new substrait::Expression_ReferenceSegment();
		substrait::Expression_ReferenceSegment_MapKey* map_key_variable_right = new substrait::Expression_ReferenceSegment_MapKey();
		substrait::Expression_Literal* variable_name_right = new substrait::Expression_Literal();
		substrait::Expression_ReferenceSegment* child_variable_type_right = new substrait::Expression_ReferenceSegment();
		substrait::Expression_ReferenceSegment_MapKey* map_key_type_right = new substrait::Expression_ReferenceSegment_MapKey();
		substrait::Expression_Literal* type_right = new substrait::Expression_Literal();
		substrait::Expression_ReferenceSegment* child_variable_index_right = new substrait::Expression_ReferenceSegment();
		substrait::Expression_ReferenceSegment_StructField* field_variable_index_right = new substrait::Expression_ReferenceSegment_StructField();

		field_variable_index_right->set_field(rexp->index);
		child_variable_index_right->set_allocated_struct_field(field_variable_index_right);
		string* type_right_str = new string(TypeIdToString(rexp->return_type));
		type_right->set_allocated_string(type_right_str);
		map_key_type_right->set_allocated_map_key(type_right);
		map_key_type_right->set_allocated_child(child_variable_index_right);
		child_variable_type_right->set_allocated_map_key(map_key_type_right);
		variable_name_right->set_allocated_string(&rexp->alias);
		map_key_variable_right->set_allocated_map_key(variable_name_right);
		map_key_variable_right->set_allocated_child(child_variable_type_right);
		direct_reference_right->set_allocated_map_key(map_key_variable_right);
		field_reference_right->set_allocated_direct_reference(direct_reference_right);
		*hash_join->add_right_keys() = *field_reference_right;
        delete field_reference_right;
		// hash_join->mutable_right_keys()->AddAllocated(field_reference_right);
	}

	for (int i = 0; i < types.size(); ++i)
		hash_join->add_out_types(TypeIdToString(types[i]));

	hash_join_rel->set_allocated_hash_join(hash_join);

	return hash_join_rel;
}
