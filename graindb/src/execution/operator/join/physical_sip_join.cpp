#include "duckdb/execution/operator/join/physical_sip_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/rai.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <iostream>

using namespace duckdb;
using namespace std;

class PhysicalSIPJoinState : public PhysicalComparisonJoinState {
public:
	PhysicalSIPJoinState(PhysicalOperator *left, PhysicalOperator *right, vector<JoinCondition> &conditions)
	    : PhysicalComparisonJoinState(left, right, conditions), initialized(false) {
	}

	bool initialized;
	DataChunk cached_chunk;
	DataChunk join_keys;
	// state for SHJoin
	unique_ptr<SIPHashTable::SIPScanStructure> scan_structure;
};

PhysicalSIPJoin::PhysicalSIPJoin(ClientContext &context, LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                 unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                 vector<idx_t> &left_projection_map, vector<idx_t> &right_projection_map)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::SIP_JOIN, move(cond), join_type),
      right_projection_map(right_projection_map) {
	children.push_back(move(left));
	children.push_back(move(right));

	assert(left_projection_map.size() == 0);

	hash_table =
	    make_unique<SIPHashTable>(BufferManager::GetBufferManager(context), conditions,
	                              LogicalOperator::MapTypes(children[1]->GetTypes(), right_projection_map), type);
}

void PhysicalSIPJoin::InitializeAList() {
	auto &rai_info = conditions[0].rais[0];
	// determine the alist for usage
	switch (rai_info->rai_type) {
	case RAIType::SELF:
	case RAIType::EDGE_SOURCE: {
		rai_info->compact_list = rai_info->forward ? &rai_info->rai->alist->compact_forward_list
		                                           : &rai_info->rai->alist->compact_backward_list;
		break;
	}
	case RAIType::EDGE_TARGET: {
		if (rai_info->rai->rai_direction == RAIDirection::UNDIRECTED) {
			rai_info->compact_list = &rai_info->rai->alist->compact_backward_list;
		} else {
			rai_info->compact_list = nullptr;
		}
		break;
	}
	default:
		rai_info->compact_list = nullptr;
		break;
	}
}

void PhysicalSIPJoin::ProbeHashTable(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalSIPJoinState *>(state_);
	if (state->child_chunk.size() > 0 && state->scan_structure) {
		// still have elements remaining from the previous probe (i.e. we got
		// >1024 elements in the previous probe)
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
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
		state->lhs_executor.Execute(state->child_chunk, state->join_keys);

		// perform the actual probe
		state->scan_structure = hash_table->Probe(state->join_keys);
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
	} while (chunk.size() == 0);
}

void PhysicalSIPJoin::PassBitMaskFilter() {
	// actually do the pushdown
	auto &rai_info = conditions[0].rais[0];
	children[0]->PushdownZoneFilter(rai_info->passing_tables[0], rai_info->row_bitmask, rai_info->zone_bitmask);
	if (rai_info->passing_tables[1] != 0) {
		children[0]->PushdownZoneFilter(rai_info->passing_tables[1], rai_info->extra_row_bitmask,
		                                rai_info->extra_zone_bitmask);
	}
}

void PhysicalSIPJoin::PerformSHJoin(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalSIPJoinState *>(state_);
	do {
		ProbeHashTable(context, chunk, state);
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

void PhysicalSIPJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
                                       SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	auto state = reinterpret_cast<PhysicalSIPJoinState *>(state_);
	if (!state->initialized) {
		state->cached_chunk.Initialize(types);

		// build the HT
		auto right_state = children[1]->GetOperatorState();
		auto right_types = children[1]->GetTypes();

		DataChunk right_chunk, build_chunk;
		right_chunk.Initialize(right_types);

		if (right_projection_map.size() > 0) {
			build_chunk.InitializeEmpty(hash_table->build_types);
		}

		state->join_keys.InitializeEmpty(hash_table->condition_types);
		auto rai_info = conditions[0].rais[0].get();
		// append ht blocks
		while (true) {
			children[1]->GetChunk(context, right_chunk, right_state.get());
			if (right_chunk.size() == 0) {
				break;
			}
			state->rhs_executor.Execute(right_chunk, state->join_keys);
			if (right_projection_map.size() > 0) {
				build_chunk.Reset();
				build_chunk.SetCardinality(right_chunk);
				for (idx_t i = 0; i < right_projection_map.size(); i++) {
					build_chunk.data[i].Reference(right_chunk.data[right_projection_map[i]]);
				}
				hash_table->Build(state->join_keys, build_chunk);
			} else {
				hash_table->Build(state->join_keys, right_chunk);
			}
		}
		hash_table->Finalize();

		// estimate distinct build side keys as non empty hash slots num, assume no conflicts in each slot.
		idx_t non_empty_hash_slots = 0;
		auto pointers = (data_ptr_t *)hash_table->hash_map->node->buffer;
		for (idx_t i = 0; i < hash_table->bitmask; i++) {
			non_empty_hash_slots += (pointers[i] != nullptr);
		}

		// estimate semi-join filter passing ratio
		double avg_degree = rai_info->GetAverageDegree(rai_info->rai_type, rai_info->forward);
		auto probe_table_card = (double)rai_info->left_cardinalities[0];
		auto filter_passing_ratio = non_empty_hash_slots * avg_degree / probe_table_card;
		if (filter_passing_ratio <= SIPJoin::SHJ_MAGIC) {
			// if passing ratio is low, generate and pass semi-join filter
			// after the right tree is finished, the left tree is initialized with rai, and the rowmask of tablescan is updated
			hash_table->GenerateBitmaskFilter(*rai_info, rai_info->compact_list != nullptr);
			PassBitMaskFilter();
		}
		state->initialized = true;

		if (hash_table->size() == 0 &&
		    (hash_table->join_type == JoinType::INNER || hash_table->join_type == JoinType::SEMI)) {
			// empty hash table with INNER or SEMI join means empty result set
			return;
		}
	}

	// perform SHJ
	PerformSHJoin(context, chunk, state);
}

unique_ptr<PhysicalOperatorState> PhysicalSIPJoin::GetOperatorState() {
	return make_unique<PhysicalSIPJoinState>(children[0].get(), children[1].get(), conditions);
}

string PhysicalSIPJoin::ExtraRenderInformation() const {
	string extra_info;
	extra_info = "SIP_JOIN[";
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

	auto &rai_info = conditions[0].rais[0];
	extra_info += rai_info->ToString() + "\n";
	return extra_info;
}


string PhysicalSIPJoin::GetSubstraitInfo(unordered_map<ExpressionType, idx_t>& func_map, idx_t& func_num, duckdb::idx_t depth) const {
	string join_str = AssignBlank(depth) + "\"sipJoin\": {\n";

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
	string rai_index_str = AssignBlank(depth) + "\"raiIndex\": ";
	string rai_name_str = AssignBlank(depth) + "\"raiName\": ";
	string rai_type_str = AssignBlank(depth) + "\"raiType\": ";
	string rai_forward_str = AssignBlank(depth) + "\"raiForward\": ";
	string rai_vertex_id_str = AssignBlank(depth) + "\"raiVertexId\": ";

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

		if (!conditions[i].rais.empty()) {
			rai_index_str += to_string(i) + ",\n";
			rai_name_str += "\"" + conditions[i].rais[0]->rai->name + "\",\n";
			rai_type_str += "\"" + conditions[i].rais[0]->RAITypeToString() + "\",\n";
			rai_forward_str += to_string(conditions[i].rais[0]->forward) + ",\n";
			rai_vertex_id_str += to_string(conditions[i].rais[0]->vertex_id) + "\n";
		}
	}

	--depth;

	left_condition_str += AssignBlank(depth) + "],\n";
	right_condition_str += AssignBlank(depth) + "],\n";

	condition_str += left_condition_str + right_condition_str;
	condition_str += rai_index_str + rai_name_str + rai_type_str + rai_forward_str
	                 + rai_vertex_id_str;

	join_str += left_str + right_str + type_str + common_str + condition_str + AssignBlank(--depth) + "}\n";

	return join_str;
}


substrait::Rel* PhysicalSIPJoin::ToSubstraitClass(unordered_map<int, string>& tableid2name) const {
	substrait::Rel* sip_join_rel = new substrait::Rel();
	substrait::SIPJoinRel* sip_join = new substrait::SIPJoinRel();

	sip_join->set_allocated_left(children[0]->ToSubstraitClass(tableid2name));
	sip_join->set_allocated_right(children[1]->ToSubstraitClass(tableid2name));

	substrait::RelCommon* common = new substrait::RelCommon();
	substrait::RelCommon_Emit* emit = new substrait::RelCommon_Emit();

	for (int i = 0; i < right_projection_map.size(); ++i) {
		emit->add_output_mapping(right_projection_map[i]);
	}

	common->set_allocated_emit(emit);
	sip_join->set_allocated_common(common);

	sip_join->set_type(JoinTypeToSubstraitSIPJoinType(type));	

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

		int size_index_left = sip_join->left_keys_size();
		*sip_join->add_left_keys() = *field_reference_left;
		// sip_join->mutable_left_keys()->AddAllocated(field_reference_left);
		delete field_reference_left;
		string* string_val_left = new string(lexp->alias);
		variable_name_left->set_allocated_string(string_val_left);
		map_key_variable_left->set_allocated_map_key(variable_name_left);
		map_key_variable_left->set_allocated_child(child_variable_type_left);
		direct_reference_left->set_allocated_map_key(map_key_variable_left);
		sip_join->mutable_left_keys(size_index_left)->set_allocated_direct_reference(direct_reference_left);


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

		int size_index_right = sip_join->right_keys_size();
		*sip_join->add_right_keys() = *field_reference_right;
		// sip_join->mutable_left_keys()->AddAllocated(field_reference_left);
		delete field_reference_right;
		string* string_val_right = new string(rexp->alias);
		variable_name_right->set_allocated_string(string_val_right);
		map_key_variable_right->set_allocated_map_key(variable_name_right);
		map_key_variable_right->set_allocated_child(child_variable_type_right);
		direct_reference_right->set_allocated_map_key(map_key_variable_right);
		sip_join->mutable_right_keys(size_index_right)->set_allocated_direct_reference(direct_reference_right);


		if (!conditions[i].rais.empty()) {
			sip_join->set_rai_index(i);
			sip_join->set_rai_name(conditions[i].rais[0]->rai->name);
			sip_join->set_rai_type(conditions[i].rais[0]->RAITypeToString());
			sip_join->set_rai_forward(conditions[i].rais[0]->forward);
			sip_join->set_rai_vertex(conditions[i].rais[0]->vertex->name);

			if (conditions[i].rais[0]->passing_tables[0] != 0)
				sip_join->add_rai_passing_tables(tableid2name[conditions[i].rais[0]->passing_tables[0]]);
			else
				sip_join->add_rai_passing_tables("");

			if (conditions[i].rais[0]->passing_tables[1] != 0)
				sip_join->add_rai_passing_tables(tableid2name[conditions[i].rais[0]->passing_tables[1]]);
			else
				sip_join->add_rai_passing_tables("");

			sip_join->add_rai_left_cardinalities(conditions[i].rais[0]->left_cardinalities[0]);
			sip_join->add_rai_left_cardinalities(conditions[i].rais[0]->left_cardinalities[1]);
			sip_join->set_rai_compact_list(conditions[i].rais[0]->compact_list != NULL);
		}
	}

	for (int i = 0; i < types.size(); ++i)
		sip_join->add_out_types(TypeIdToString(types[i]));

	sip_join_rel->set_allocated_sip_join(sip_join);

	return sip_join_rel;
}
