#include "duckdb/execution/operator/order/physical_top_n.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

using namespace duckdb;
using namespace std;

class PhysicalTopNOperatorState : public PhysicalOperatorState {
public:
	PhysicalTopNOperatorState(PhysicalOperator *child) : PhysicalOperatorState(child), position(0) {
	}

	idx_t position;
	idx_t current_offset;
	ChunkCollection sorted_data;
	unique_ptr<idx_t[]> heap;
};

void PhysicalTopN::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
                                    SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	auto state = reinterpret_cast<PhysicalTopNOperatorState *>(state_);
	ChunkCollection &big_data = state->sorted_data;

	if (state->position == 0) {
		// first concatenate all the data of the child chunks
		do {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get(), sel, rid_vector, rai_chunk);
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

		CalculateHeapSize(big_data.count);
		if (heap_size == 0) {
			return;
		}

		ChunkCollection heap_collection;
		for (idx_t i = 0; i < big_data.chunks.size(); i++) {
			DataChunk heap_chunk;
			heap_chunk.Initialize(sort_types);

			executor.Execute(*big_data.chunks[i], heap_chunk);
			heap_collection.Append(heap_chunk);
		}

		assert(heap_collection.count == big_data.count);

		// create and use the heap
		state->heap = unique_ptr<idx_t[]>(new idx_t[heap_size]);
		heap_collection.Heap(order_types, state->heap.get(), heap_size);
	}

	if (state->position >= heap_size) {
		return;
	} else if (state->position < offset) {
		state->position = offset;
	}

	state->position += big_data.MaterializeHeapChunk(chunk, state->heap.get(), state->position, heap_size);
}

unique_ptr<PhysicalOperatorState> PhysicalTopN::GetOperatorState() {
	return make_unique<PhysicalTopNOperatorState>(children[0].get());
}

void PhysicalTopN::CalculateHeapSize(idx_t rows) {
	heap_size = (rows > offset) ? min(limit + offset, rows) : 0;
}

uint8_t OrderTypeToInt(OrderType type) {
    if (type == OrderType::INVALID)
        return 0;
    else if (type == OrderType::ASCENDING)
        return 1;
    else if (type == OrderType::DESCENDING)
        return 2;
    return -1;
}

substrait::Rel* PhysicalTopN::ToSubstraitClass(unordered_map<int, std::string> &tableid2name) const {
    substrait::Rel *topn_rel = new substrait::Rel();
    substrait::FetchRel *topn = new substrait::FetchRel();
    topn->set_offset(offset);
    topn->set_count(limit);

    for (int i = 0; i < children.size(); ++i) {
        topn->set_allocated_input(children[i]->ToSubstraitClass(tableid2name));
    }

    substrait::RelCommon *common = new substrait::RelCommon();
    substrait::RelCommon_Emit *emit = new substrait::RelCommon_Emit();

    for (int i = 0; i < orders.size(); ++i) {
        BoundReferenceExpression* expr = (BoundReferenceExpression*) orders[i].expression.get();
        emit->add_output_mapping(expr->index);
        emit->add_output_names(expr->alias);
        emit->add_output_types(TypeIdToString(expr->return_type));
        emit->add_output_order(OrderTypeToInt(orders[i].type));
        emit->add_output_exp_type(ExpressionTypeToString(expr->type));
    }

    for (int i = 0; i < types.size(); ++i) {
        topn->add_types(TypeIdToString(types[i]));
    }

    common->set_allocated_emit(emit);
    topn->set_allocated_common(common);
    topn_rel->set_allocated_fetch(topn);

    return topn_rel;
}
