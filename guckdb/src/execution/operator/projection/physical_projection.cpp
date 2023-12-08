#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

    class ProjectionState : public OperatorState {
    public:
        explicit ProjectionState(ExecutionContext &context, const vector<unique_ptr<Expression>> &expressions)
                : executor(context.client, expressions) {
        }

        ExpressionExecutor executor;

    public:
        void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
            context.thread.profiler.Flush(op, executor, "projection", 0);
        }
    };

    PhysicalProjection::PhysicalProjection(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                                           idx_t estimated_cardinality)
            : PhysicalOperator(PhysicalOperatorType::PROJECTION, std::move(types), estimated_cardinality),
              select_list(std::move(select_list)) {
    }

    OperatorResultType PhysicalProjection::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate, OperatorState &state_p) const {
        auto &state = state_p.Cast<ProjectionState>();
        state.executor.Execute(input, chunk);
        return OperatorResultType::NEED_MORE_INPUT;
    }

    unique_ptr<OperatorState> PhysicalProjection::GetOperatorState(ExecutionContext &context) const {
        return make_uniq<ProjectionState>(context, select_list);
    }

    unique_ptr<PhysicalOperator>
    PhysicalProjection::CreateJoinProjection(vector<LogicalType> proj_types, const vector<LogicalType> &lhs_types,
                                             const vector<LogicalType> &rhs_types,
                                             const vector<idx_t> &left_projection_map,
                                             const vector<idx_t> &right_projection_map,
                                             const idx_t estimated_cardinality) {

        vector<unique_ptr<Expression>> proj_selects;
        proj_selects.reserve(proj_types.size());

        if (left_projection_map.empty()) {
            for (storage_t i = 0; i < lhs_types.size(); ++i) {
                proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(lhs_types[i], i));
            }
        } else {
            for (auto i: left_projection_map) {
                proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(lhs_types[i], i));
            }
        }
        const auto left_cols = lhs_types.size();

        if (right_projection_map.empty()) {
            for (storage_t i = 0; i < rhs_types.size(); ++i) {
                proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(rhs_types[i], left_cols + i));
            }

        } else {
            for (auto i: right_projection_map) {
                proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(rhs_types[i], left_cols + i));
            }
        }

        return make_uniq<PhysicalProjection>(std::move(proj_types), std::move(proj_selects), estimated_cardinality);
    }

    string PhysicalProjection::ParamsToString() const {
        string extra_info;
        for (auto &expr: select_list) {
            extra_info += expr->GetName() + "\n";
        }
        return extra_info;
    }

    substrait::RelCommon_Emit* formulateSelectList(const duckdb::vector<unique_ptr<Expression>>& select_list) {
        substrait::RelCommon_Emit* emit = new substrait::RelCommon_Emit();
        for (int i = 0; i < select_list.size(); ++i) {
            if (select_list[i].get()->type == ExpressionType::BOUND_REF) {
                BoundReferenceExpression *bexp = (BoundReferenceExpression *) select_list[i].get();
                emit->add_output_mapping(bexp->index);
                emit->add_output_names(bexp->alias);
                emit->add_output_types(TypeIdToString(bexp->return_type.InternalType()));
                emit->add_output_order(0);
                emit->add_output_exp_type("");
            }
            else if (select_list[i].get()->type == ExpressionType::VALUE_CONSTANT) {
                BoundConstantExpression* bexp = (BoundConstantExpression*) select_list[i].get();
                emit->add_output_mapping(-1);
                emit->add_output_names(bexp->alias);
                emit->add_output_types(TypeIdToString(bexp->return_type.InternalType()));
                emit->add_output_order(1);
                emit->add_output_exp_type(bexp->value.ToString());
            }
        }

        return emit;
    }

    string PhysicalProjection::GetSubstraitInfo(unordered_map<ExpressionType, idx_t> &func_map, idx_t &func_num,
                                                duckdb::idx_t depth) const {
        string project_str = AssignBlank(depth) + "\"project\": {\n";

        string common_str = AssignBlank(++depth) + "\"common\": {\n";
        string emit_str = AssignBlank(++depth) + "\"emit\": {\n";
        string output_mapping_str = AssignBlank(++depth) + "\"outputMapping\": [\n";
        string output_mapping_names = AssignBlank(depth) + "\"outputNames\": [\n";
        string output_mapping_types = AssignBlank(depth) + "\"outputTypes\": [\n";

        ++depth;
        string select_list_str = "";
        string select_list_names = "";
        string select_list_types = "";

        for (int i = 0; i < select_list.size(); ++i) {
            BoundReferenceExpression *bexp = (BoundReferenceExpression *) select_list[i].get();
            select_list_str += AssignBlank(depth) + to_string(bexp->index);
            select_list_names += AssignBlank(depth) + "\"" + bexp->alias + "\"";
            select_list_types += AssignBlank(depth) + "\"" + "\"";//TypeIdToString(bexp->return_type) + "\"";
            if (i != select_list.size() - 1) {
                select_list_str += ",\n";
                select_list_names += ",\n";
                select_list_types += ",\n";
            } else {
                select_list_str += "\n";
                select_list_names += "\n";
                select_list_types += "\n";
            }
        }
        select_list_str += AssignBlank(--depth) + "]";
        select_list_names += AssignBlank(depth) + "]";
        select_list_types += AssignBlank(depth) + "]";
        output_mapping_str += select_list_str + ",\n";
        output_mapping_names += select_list_names + ",\n";
        output_mapping_types += select_list_types + "\n";
        emit_str += output_mapping_str + output_mapping_names + output_mapping_types + AssignBlank(--depth) + "}\n";
        common_str += emit_str + AssignBlank(--depth) + "},\n";

        string input_str = AssignBlank(depth) + "\"input\": {\n";
        for (int i = 0; i < children.size(); ++i) {
            input_str += children[i]->GetSubstraitInfo(func_map, func_num, depth + 1);
        }
        input_str += AssignBlank(depth) + "}\n";

        project_str += common_str + input_str + AssignBlank(--depth) + "}\n";

        return project_str;
    }

    substrait::Rel *PhysicalProjection::ToSubstraitClass(unordered_map<int, string> &tableid2name) const {
        substrait::Rel *project_rel = new substrait::Rel();
        substrait::ProjectRel *project = new substrait::ProjectRel();
        substrait::RelCommon *common = new substrait::RelCommon();
        substrait::RelCommon_Emit *emit = formulateSelectList(select_list);

        common->set_allocated_emit(emit);
        project->set_allocated_common(common);

        for (int i = 0; i < children.size(); ++i) {
            project->set_allocated_input(children[i]->ToSubstraitClass(tableid2name));
        }

        project_rel->set_allocated_project(project);

        return project_rel;
    }

} // namespace duckdb
