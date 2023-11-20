#include "duckdb/execution/operator/order/physical_order.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

using namespace duckdb;
using namespace std;

class PhysicalOrderOperatorState : public PhysicalOperatorState {
public:
	PhysicalOrderOperatorState(PhysicalOperator *child) : PhysicalOperatorState(child), position(0) {
	}

	idx_t position;
	ChunkCollection sorted_data;
	unique_ptr<idx_t[]> sorted_vector;
};

void PhysicalOrder::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
                                     SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	auto state = reinterpret_cast<PhysicalOrderOperatorState *>(state_);
	ChunkCollection &big_data = state->sorted_data;
	if (state->position == 0) {
		// first concatenate all the data of the child chunks
		do {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			big_data.Append(state->child_chunk);
		} while (state->child_chunk.size() != 0);

		// now perform the actual ordering of the data
		// compute the sorting columns from the input data
		ExpressionExecutor executor;
		vector<TypeId> sort_types;
		vector<OrderType> order_types;
		for (idx_t i = 0; i < orders.size(); i++) {
			auto &expr = orders[i].expression;
			sort_types.push_back(expr->return_type);
			order_types.push_back(orders[i].type);
			executor.AddExpression(*expr);
		}

		ChunkCollection sort_collection;
		for (idx_t i = 0; i < big_data.chunks.size(); i++) {
			DataChunk sort_chunk;
			sort_chunk.Initialize(sort_types);

			executor.Execute(*big_data.chunks[i], sort_chunk);
			sort_collection.Append(sort_chunk);
		}

		assert(sort_collection.count == big_data.count);

		// now perform the actual sort
		state->sorted_vector = unique_ptr<idx_t[]>(new idx_t[sort_collection.count]);
		sort_collection.Sort(order_types, state->sorted_vector.get());
	}

	if (state->position >= big_data.count) {
		return;
	}

	big_data.MaterializeSortedChunk(chunk, state->sorted_vector.get(), state->position);
	state->position += STANDARD_VECTOR_SIZE;
}

unique_ptr<PhysicalOperatorState> PhysicalOrder::GetOperatorState() {
	return make_unique<PhysicalOrderOperatorState>(children[0].get());
}

substrait::Rel* PhysicalOrder::ToSubstraitClass(unordered_map<int, std::string> &tableid2name) const {
    substrait::Rel *order_rel = new substrait::Rel();
    substrait::FetchRel *order = new substrait::FetchRel();

    order->set_offset(-1);

    for (int i = 0; i < children.size(); ++i) {
        order->set_allocated_input(children[i]->ToSubstraitClass(tableid2name));
    }

    substrait::RelCommon *common = new substrait::RelCommon();
    substrait::RelCommon_Emit *emit = new substrait::RelCommon_Emit();

    for (int i = 0; i < orders.size(); ++i) {
        BoundReferenceExpression* expr = (BoundReferenceExpression*) orders[i].expression.get();
        emit->add_output_mapping(expr->index);
        emit->add_output_names(expr->alias);
        emit->add_output_types(TypeIdToString(expr->return_type));
        emit->add_output_order(static_cast<int>(orders[i].type));
        emit->add_output_exp_type(ExpressionTypeToString(expr->type));
    }

    for (int i = 0; i < types.size(); ++i) {
        order->add_types(TypeIdToString(types[i]));
    }

    common->set_allocated_emit(emit);
    order->set_allocated_common(common);
    order_rel->set_allocated_fetch(order);

    return order_rel;
}
