#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

using namespace duckdb;
using namespace std;

class PhysicalHashAggregateState : public PhysicalOperatorState {
public:
	PhysicalHashAggregateState(PhysicalHashAggregate *parent, PhysicalOperator *child);

	//! Materialized GROUP BY expression
	DataChunk group_chunk;
	//! Materialized aggregates
	DataChunk aggregate_chunk;
	//! The current position to scan the HT for output tuples
	idx_t ht_scan_position;
	idx_t tuples_scanned;
	//! The HT
	unique_ptr<SuperLargeHashTable> ht;
	//! The payload chunk, only used while filling the HT
	DataChunk payload_chunk;
	//! Expression executor for the GROUP BY chunk
	ExpressionExecutor group_executor;
	//! Expression state for the payload
	ExpressionExecutor payload_executor;
};

PhysicalHashAggregate::PhysicalHashAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
                                             PhysicalOperatorType type)
    : PhysicalHashAggregate(types, move(expressions), {}, type) {
}

PhysicalHashAggregate::PhysicalHashAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups, PhysicalOperatorType type)
    : PhysicalOperator(type, types), groups(move(groups)) {
	// get a list of all aggregates to be computed
	// fake a single group with a constant value for aggregation without groups
	if (this->groups.size() == 0) {
		auto ce = make_unique<BoundConstantExpression>(Value::TINYINT(42));
		this->groups.push_back(move(ce));
		is_implicit_aggr = true;
	} else {
		is_implicit_aggr = false;
	}
	for (auto &expr : expressions) {
		assert(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
		assert(expr->IsAggregate());
		aggregates.push_back(move(expr));
	}
}

void PhysicalHashAggregate::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
                                             SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	auto state = reinterpret_cast<PhysicalHashAggregateState *>(state_);
	do {
		// resolve the child chunk if there is one
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get(), sel, rid_vector, rai_chunk);
		if (state->child_chunk.size() == 0) {
			break;
		}
		// aggregation with groups
		DataChunk &group_chunk = state->group_chunk;
		DataChunk &payload_chunk = state->payload_chunk;
		state->group_executor.Execute(state->child_chunk, group_chunk);
		state->payload_executor.SetChunk(state->child_chunk);

		payload_chunk.Reset();
		idx_t payload_idx = 0, payload_expr_idx = 0;
		payload_chunk.SetCardinality(group_chunk);
		for (idx_t i = 0; i < aggregates.size(); i++) {
			auto &aggr = (BoundAggregateExpression &)*aggregates[i];
			if (aggr.children.size()) {
				for (idx_t j = 0; j < aggr.children.size(); ++j) {
					state->payload_executor.ExecuteExpression(payload_expr_idx, payload_chunk.data[payload_idx]);
					payload_idx++;
					payload_expr_idx++;
				}
			} else {
				payload_idx++;
			}
		}

		group_chunk.Verify();
		payload_chunk.Verify();
		assert(payload_chunk.column_count() == 0 || group_chunk.size() == payload_chunk.size());

		state->ht->AddChunk(group_chunk, payload_chunk);
		state->tuples_scanned += state->child_chunk.size();
	} while (state->child_chunk.size() > 0);

	state->group_chunk.Reset();
	state->aggregate_chunk.Reset();
	idx_t elements_found = state->ht->Scan(state->ht_scan_position, state->group_chunk, state->aggregate_chunk);

	// special case hack to sort out aggregating from empty intermediates
	// for aggregations without groups
	if (elements_found == 0 && state->tuples_scanned == 0 && is_implicit_aggr) {
		assert(chunk.column_count() == aggregates.size());
		// for each column in the aggregates, set to initial state
		chunk.SetCardinality(1);
		for (idx_t i = 0; i < chunk.column_count(); i++) {
			assert(aggregates[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = (BoundAggregateExpression &)*aggregates[i];
			auto aggr_state = unique_ptr<data_t[]>(new data_t[aggr.function.state_size()]);
			aggr.function.initialize(aggr_state.get());

			Vector state_vector(Value::POINTER((uintptr_t)aggr_state.get()));
			aggr.function.finalize(state_vector, chunk.data[i], 1);
		}
		state->finished = true;
		return;
	}
	if (elements_found == 0 && !state->finished) {
		state->finished = true;
		return;
	}
	// we finished the child chunk
	// actually compute the final projection list now
	idx_t chunk_index = 0;
	chunk.SetCardinality(elements_found);
	if (state->group_chunk.column_count() + state->aggregate_chunk.column_count() == chunk.column_count()) {
		for (idx_t col_idx = 0; col_idx < state->group_chunk.column_count(); col_idx++) {
			chunk.data[chunk_index++].Reference(state->group_chunk.data[col_idx]);
		}
	} else {
		assert(state->aggregate_chunk.column_count() == chunk.column_count());
	}

	for (idx_t col_idx = 0; col_idx < state->aggregate_chunk.column_count(); col_idx++) {
		chunk.data[chunk_index++].Reference(state->aggregate_chunk.data[col_idx]);
	}
}

unique_ptr<PhysicalOperatorState> PhysicalHashAggregate::GetOperatorState() {
	assert(children.size() > 0);
	auto state = make_unique<PhysicalHashAggregateState>(this, children[0].get());
	state->tuples_scanned = 0;
	vector<TypeId> group_types, payload_types;
	vector<BoundAggregateExpression *> aggregate_kind;
	for (auto &expr : groups) {
		group_types.push_back(expr->return_type);
	}
	for (auto &expr : aggregates) {
		assert(expr->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		auto &aggr = (BoundAggregateExpression &)*expr;
		aggregate_kind.push_back(&aggr);
		if (aggr.children.size()) {
			for (idx_t i = 0; i < aggr.children.size(); ++i) {
				payload_types.push_back(aggr.children[i]->return_type);
				state->payload_executor.AddExpression(*aggr.children[i]);
			}
		} else {
			// COUNT(*)
			payload_types.push_back(TypeId::INT64);
		}
	}
	if (payload_types.size() > 0) {
		state->payload_chunk.Initialize(payload_types);
	}

	state->ht = make_unique<SuperLargeHashTable>(1024, group_types, payload_types, aggregate_kind);
	return move(state);
}


substrait::Rel* PhysicalHashAggregate::ToSubstraitClass(unordered_map<int, string>& tableid2name) const {
    substrait::Rel *aggregate_rel = new substrait::Rel();
    substrait::AggregateRel *aggregate = new substrait::AggregateRel();

    for (int i = 0; i < children.size(); ++i) {
        aggregate->set_allocated_input(children[i]->ToSubstraitClass(tableid2name));
    }

    substrait::RelCommon *common = new substrait::RelCommon();
    substrait::RelCommon_Emit *emit = new substrait::RelCommon_Emit();

    for (int i = 0; i < types.size(); ++i) {
        emit->add_output_types(TypeIdToString(types[i]));
    }
    common->set_allocated_emit(emit);
    aggregate->set_allocated_common(common);

    aggregate->set_type(PhysicalOperatorToString(type));

    substrait::AggregateRel_Grouping* groupings = new substrait::AggregateRel_Grouping();
    for (int i = 0; i < groups.size(); ++i) {
        substrait::Expression* group_expr = new substrait::Expression();

        substrait::Expression_FieldReference* expr_fr = new substrait::Expression_FieldReference();
        group_expr->set_allocated_selection(expr_fr);

        substrait::Expression_ReferenceSegment* direct_reference = new substrait::Expression_ReferenceSegment();
        expr_fr->set_allocated_direct_reference(direct_reference);

        substrait::Expression_ReferenceSegment_MapKey* map_key_name = new substrait::Expression_ReferenceSegment_MapKey();
        direct_reference->set_allocated_map_key(map_key_name);
        substrait::Expression_Literal* expr_name = new substrait::Expression_Literal();
        string* name = new string(groups[i]->alias);
        expr_name->set_allocated_string(name);
        map_key_name->set_allocated_map_key(expr_name);

        substrait::Expression_ReferenceSegment* direct_type = new substrait::Expression_ReferenceSegment();
        map_key_name->set_allocated_child(direct_type);
        substrait::Expression_ReferenceSegment_MapKey* map_key_type = new substrait::Expression_ReferenceSegment_MapKey();
        direct_type->set_allocated_map_key(map_key_type);
        substrait::Expression_Literal* expr_type = new substrait::Expression_Literal();
        string* type = new string(TypeIdToString(groups[i]->return_type));
        expr_type->set_allocated_string(type);
        map_key_type->set_allocated_map_key(expr_type);

        substrait::Expression_ReferenceSegment* direct_index = new substrait::Expression_ReferenceSegment();
        map_key_type->set_allocated_child(direct_index);
        substrait::Expression_ReferenceSegment_StructField* field_index = new substrait::Expression_ReferenceSegment_StructField();
        direct_index->set_allocated_struct_field(field_index);
        BoundReferenceExpression* brf = (BoundReferenceExpression*) groups[i].get();
        field_index->set_field(brf->index);

        *groupings->add_grouping_expressions() = *group_expr;
        delete group_expr;
    }
    *aggregate->add_groupings() = *groupings;
    delete groupings;

    for (int i = 0; i < aggregates.size(); ++i) {
        substrait::AggregateRel_Measure* measure = new substrait::AggregateRel_Measure();
        substrait::AggregateFunction* function = aggregates[i].get()->ToAggregateFunction();
        measure->set_allocated_measure(function);
        *aggregate->add_measures() = *measure;
        delete measure;
    }

    aggregate_rel->set_allocated_aggregate(aggregate);

    return aggregate_rel;
}


PhysicalHashAggregateState::PhysicalHashAggregateState(PhysicalHashAggregate *parent, PhysicalOperator *child)
    : PhysicalOperatorState(child), ht_scan_position(0), tuples_scanned(0), group_executor(parent->groups) {
	vector<TypeId> group_types, aggregate_types;
	for (auto &expr : parent->groups) {
		group_types.push_back(expr->return_type);
	}
	group_chunk.Initialize(group_types);
	for (auto &expr : parent->aggregates) {
		aggregate_types.push_back(expr->return_type);
	}
	if (aggregate_types.size() > 0) {
		aggregate_chunk.Initialize(aggregate_types);
	}
}

