#include "duckdb/execution/operator/set/physical_union.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

class PhysicalUnionOperatorState : public PhysicalOperatorState {
public:
	PhysicalUnionOperatorState() : PhysicalOperatorState(nullptr), top_done(false) {
	}
	unique_ptr<PhysicalOperatorState> top_state;
	unique_ptr<PhysicalOperatorState> bottom_state;
	bool top_done = false;
};

PhysicalUnion::PhysicalUnion(LogicalOperator &op, unique_ptr<PhysicalOperator> top, unique_ptr<PhysicalOperator> bottom)
    : PhysicalOperator(PhysicalOperatorType::UNION, op.types) {
	children.push_back(move(top));
	children.push_back(move(bottom));
}

// first exhaust top, then exhaust bottom. state to remember which.
void PhysicalUnion::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
                                     SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	auto state = reinterpret_cast<PhysicalUnionOperatorState *>(state_);
	if (!state->top_done) {
		children[0]->GetChunk(context, chunk, state->top_state.get());
		if (chunk.size() == 0) {
			state->top_done = true;
		}
	}
	if (state->top_done) {
		children[1]->GetChunk(context, chunk, state->bottom_state.get());
	}
	if (chunk.size() == 0) {
		state->finished = true;
	}
}

unique_ptr<PhysicalOperatorState> PhysicalUnion::GetOperatorState() {
	auto state = make_unique<PhysicalUnionOperatorState>();
	state->top_state = children[0]->GetOperatorState();
	state->bottom_state = children[1]->GetOperatorState();
	return (move(state));
}

substrait::Rel* PhysicalUnion::ToSubstraitClass(unordered_map<int, std::string> &tableid2name) const {
    substrait::Rel *union_rel = new substrait::Rel();
    substrait::SetRel *union_set = new substrait::SetRel();

    for (int i = 0; i < children.size(); ++i) {
        substrait::Rel* child_out = children[i]->ToSubstraitClass(tableid2name);
        *union_set->add_inputs() = *child_out;
        delete child_out;
    }

    if (type == PhysicalOperatorType::UNION)
        union_set->set_op(substrait::SetRel_SetOp_SET_OP_UNION_DISTINCT);

    substrait::RelCommon *common = new substrait::RelCommon();
    substrait::RelCommon_Emit *emit = new substrait::RelCommon_Emit();

    for (int i = 0; i < types.size(); ++i) {
        emit->add_output_types(TypeIdToString(types[i]));
    }
    common->set_allocated_emit(emit);
    union_set->set_allocated_common(common);

    union_rel->set_allocated_set(union_set);

    return union_rel;
}
