#include "duckdb/execution/operator/set/physical_union.hpp"

#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

PhysicalUnion::PhysicalUnion(vector<LogicalType> types, unique_ptr<PhysicalOperator> top,
                             unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::UNION, std::move(types), estimated_cardinality) {
	children.push_back(std::move(top));
	children.push_back(std::move(bottom));
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalUnion::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();

	// order matters if any of the downstream operators are order dependent,
	// or if the sink preserves order, but does not support batch indices to do so
	auto sink = meta_pipeline.GetSink();
	bool order_matters = false;
	if (current.IsOrderDependent()) {
		order_matters = true;
	}
	if (sink) {
		if (sink->SinkOrderDependent() || sink->RequiresBatchIndex()) {
			order_matters = true;
		}
		if (!sink->ParallelSink()) {
			order_matters = true;
		}
	}

	// create a union pipeline that is identical to 'current'
	auto union_pipeline = meta_pipeline.CreateUnionPipeline(current, order_matters);

	// continue with the current pipeline
	children[0]->BuildPipelines(current, meta_pipeline);

	if (order_matters) {
		// order matters, so 'union_pipeline' must come after all pipelines created by building out 'current'
		meta_pipeline.AddDependenciesFrom(union_pipeline, union_pipeline, false);
	}

	// build the union pipeline
	children[1]->BuildPipelines(*union_pipeline, meta_pipeline);

	// Assign proper batch index to the union pipeline
	// This needs to happen after the pipelines have been built because unions can be nested
	meta_pipeline.AssignNextBatchIndex(union_pipeline);
}

vector<const_reference<PhysicalOperator>> PhysicalUnion::GetSources() const {
	vector<const_reference<PhysicalOperator>> result;
	for (auto &child : children) {
		auto child_sources = child->GetSources();
		result.insert(result.end(), child_sources.begin(), child_sources.end());
	}
	return result;
}

substrait::Rel* PhysicalUnion::ToSubstraitClass(unordered_map<int, string>& tableid2name) const {
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
        emit->add_output_types(TypeIdToString(types[i].InternalType()));
    }
    common->set_allocated_emit(emit);
    union_set->set_allocated_common(common);

    union_rel->set_allocated_set(union_set);

    return union_rel;
}


} // namespace duckdb
