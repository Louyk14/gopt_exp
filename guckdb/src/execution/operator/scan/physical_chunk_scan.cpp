#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"

using namespace duckdb;
using namespace std;

class PhysicalChunkScanState : public PhysicalOperatorState {
public:
	PhysicalChunkScanState() : PhysicalOperatorState(nullptr), chunk_index(0) {
	}

	//! The current position in the scan
	idx_t chunk_index;
};

void PhysicalChunkScan::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
                                         SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	auto state = (PhysicalChunkScanState *)state_;
	assert(collection);
	if (collection->count == 0) {
		return;
	}
	assert(chunk.GetTypes() == collection->types);
	if (state->chunk_index >= collection->chunks.size()) {
		return;
	}
	auto &collection_chunk = *collection->chunks[state->chunk_index];
	chunk.Reference(collection_chunk);
	state->chunk_index++;
}

unique_ptr<PhysicalOperatorState> PhysicalChunkScan::GetOperatorState() {
	return make_unique<PhysicalChunkScanState>();
}

substrait::Rel* PhysicalChunkScan::ToSubstraitClass(unordered_map<int, string>& tableid2name) const {
    substrait::Rel* chunk_scan_rel = new substrait::Rel();
    substrait::ReadRel* chunk_scan = new substrait::ReadRel();
    substrait::RelCommon* common = new substrait::RelCommon();
    substrait::RelCommon_Emit* emit = new substrait::RelCommon_Emit();

    for (int i = 0; i < types.size(); ++i) {
        emit->add_output_types(TypeIdToString(types[i]));
    }
    emit->add_output_exp_type(PhysicalOperatorToString(type));

    common->set_allocated_emit(emit);

    chunk_scan->set_allocated_common(common);
    chunk_scan_rel->set_allocated_read(chunk_scan);

    return chunk_scan_rel;
}