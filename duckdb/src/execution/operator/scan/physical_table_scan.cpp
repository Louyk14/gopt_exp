#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"

#include <utility>

namespace duckdb {

PhysicalTableScan::PhysicalTableScan(vector<LogicalType> types, TableFunction function_p,
                                     unique_ptr<FunctionData> bind_data_p, vector<LogicalType> returned_types_p,
                                     vector<column_t> column_ids_p, vector<idx_t> projection_ids_p,
                                     vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                     idx_t estimated_cardinality, ExtraOperatorInfo extra_info)
    : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data_p)), returned_types(std::move(returned_types_p)),
      column_ids(std::move(column_ids_p)), projection_ids(std::move(projection_ids_p)), names(std::move(names_p)),
      table_filters(std::move(table_filters_p)), extra_info(extra_info) {
}

class TableScanGlobalSourceState : public GlobalSourceState {
public:
	TableScanGlobalSourceState(ClientContext &context, const PhysicalTableScan &op) {
		if (op.function.init_global) {
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids, op.table_filters.get());
			global_state = op.function.init_global(context, input);
			if (global_state) {
				max_threads = global_state->MaxThreads();
			}
		} else {
			max_threads = 1;
		}
	}

	idx_t max_threads = 0;
	unique_ptr<GlobalTableFunctionState> global_state;

	idx_t MaxThreads() override {
		return max_threads;
	}
};

class TableScanLocalSourceState : public LocalSourceState {
public:
	TableScanLocalSourceState(ExecutionContext &context, TableScanGlobalSourceState &gstate,
	                          const PhysicalTableScan &op) {
		if (op.function.init_local) {
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids, op.table_filters.get());
			local_state = op.function.init_local(context, input, gstate.global_state.get());
		}
	}

	unique_ptr<LocalTableFunctionState> local_state;
};

unique_ptr<LocalSourceState> PhysicalTableScan::GetLocalSourceState(ExecutionContext &context,
                                                                    GlobalSourceState &gstate) const {
	return make_uniq<TableScanLocalSourceState>(context, gstate.Cast<TableScanGlobalSourceState>(), *this);
}

unique_ptr<GlobalSourceState> PhysicalTableScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<TableScanGlobalSourceState>(context, *this);
}

SourceResultType PhysicalTableScan::GetData(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
	D_ASSERT(!column_ids.empty());
	auto &gstate = input.global_state.Cast<TableScanGlobalSourceState>();
	auto &state = input.local_state.Cast<TableScanLocalSourceState>();

	TableFunctionInput data(bind_data.get(), state.local_state.get(), gstate.global_state.get());
	function.function(context.client, data, chunk);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

double PhysicalTableScan::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	if (function.table_scan_progress) {
		return function.table_scan_progress(context, bind_data.get(), gstate.global_state.get());
	}
	// if table_scan_progress is not implemented we don't support this function yet in the progress bar
	return -1;
}

idx_t PhysicalTableScan::GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                       LocalSourceState &lstate) const {
	D_ASSERT(SupportsBatchIndex());
	D_ASSERT(function.get_batch_index);
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	auto &state = lstate.Cast<TableScanLocalSourceState>();
	return function.get_batch_index(context.client, bind_data.get(), state.local_state.get(),
	                                gstate.global_state.get());
}

string PhysicalTableScan::GetName() const {
	return StringUtil::Upper(function.name + " " + function.extra_info);
}

string PhysicalTableScan::ParamsToString() const {
	string result;
	if (function.to_string) {
		result = function.to_string(bind_data.get());
		result += "\n[INFOSEPARATOR]\n";
	}
	if (function.projection_pushdown) {
		if (function.filter_prune) {
			for (idx_t i = 0; i < projection_ids.size(); i++) {
				const auto &column_id = column_ids[projection_ids[i]];
				if (column_id < names.size()) {
					if (i > 0) {
						result += "\n";
					}
					result += names[column_id];
				}
			}
		} else {
			for (idx_t i = 0; i < column_ids.size(); i++) {
				const auto &column_id = column_ids[i];
				if (column_id < names.size()) {
					if (i > 0) {
						result += "\n";
					}
					result += names[column_id];
				}
			}
		}
	}
	if (function.filter_pushdown && table_filters) {
		result += "\n[INFOSEPARATOR]\n";
		result += "Filters: ";
		for (auto &f : table_filters->filters) {
			auto &column_index = f.first;
			auto &filter = f.second;
			if (column_index < names.size()) {
				result += filter->ToString(names[column_ids[column_index]]);
				result += "\n";
			}
		}
	}
	if (!extra_info.file_filters.empty()) {
		result += "\n[INFOSEPARATOR]\n";
		result += "File Filters: " + extra_info.file_filters;
	}
	result += "\n[INFOSEPARATOR]\n";
	result += StringUtil::Format("EC: %llu", estimated_cardinality);
	return result;
}

bool PhysicalTableScan::Equals(const PhysicalOperator &other_p) const {
	if (type != other_p.type) {
		return false;
	}
	auto &other = other_p.Cast<PhysicalTableScan>();
	if (function.function != other.function.function) {
		return false;
	}
	if (column_ids != other.column_ids) {
		return false;
	}
	if (!FunctionData::Equals(bind_data.get(), other.bind_data.get())) {
		return false;
	}
	return true;
}

    string PhysicalTableScan::GetSubstraitInfo(unordered_map<ExpressionType, idx_t>& func_map, idx_t& func_num, duckdb::idx_t depth) const {
        return "";
    }

    substrait::Rel* PhysicalTableScan::ToSubstraitClass(unordered_map<int, string>& tableid2name) const {
        substrait::Rel* table_scan_rel = new substrait::Rel();
        substrait::ReadRel* read = new substrait::ReadRel();
        substrait::RelCommon* common = new substrait::RelCommon();
        substrait::RelCommon_Emit* emit = new substrait::RelCommon_Emit();

        for (int i = 0; i < column_ids.size(); ++i) {
            emit->add_output_mapping(column_ids[i]);
            emit->add_output_types(TypeIdToString(returned_types[i].InternalType()));
        }

        common->set_allocated_emit(emit);

        substrait::ReadRel_NamedTable* named_table = new substrait::ReadRel_NamedTable();
        TableScanBindData* table_bind_data = (TableScanBindData*) bind_data.get();
        string table_name = table_bind_data->table.name;
        std::transform(table_name.begin(), table_name.end(), table_name.begin(),
                                                      [](unsigned char c){ return std::tolower(c); });
        // named_table->add_names(table_bind_data->table->table);
        named_table->add_names(table_name);

        substrait::Expression* filter = new substrait::Expression();
        if (table_filters != NULL && !table_filters->filters.empty()) {
            for (const auto& pair : table_filters->filters) {
                int read_index = pair.first;
                int column_index = column_ids[read_index];
                substrait::Expression_ScalarFunction *scalar_function = new substrait::Expression_ScalarFunction();

                if (pair.second->filter_type == TableFilterType::CONJUNCTION_AND) {
                    ConjunctionAndFilter* cur_filter = (ConjunctionAndFilter*) pair.second.get();
                    for (int i = 0; i < cur_filter->child_filters.size(); ++i) {
                        ConstantFilter* cfilter = (ConstantFilter*) cur_filter->child_filters[i].get();

                        switch (cfilter->comparison_type) {
                            case ExpressionType::COMPARE_EQUAL:
                                scalar_function->set_function_reference(0);
                                break;
                            case ExpressionType::COMPARE_GREATERTHAN:
                                scalar_function->set_function_reference(1);
                                break;
                            default:
                                scalar_function->set_function_reference(-1);
                                break;
                        }

                        substrait::Type *output_type = new substrait::Type();
                        substrait::Type_Boolean *boolean = new substrait::Type_Boolean();
                        boolean->set_nullability(substrait::Type_Nullability_NULLABILITY_REQUIRED);
                        output_type->set_allocated_bool_(boolean);

                        substrait::FunctionArgument *arg1 = new substrait::FunctionArgument();
                        substrait::Expression *value1 = new substrait::Expression();
                        substrait::Expression_FieldReference *selection1 = new substrait::Expression_FieldReference();
                        substrait::Expression_ReferenceSegment *direct_reference1 = new substrait::Expression_ReferenceSegment();
                        substrait::Expression_ReferenceSegment_StructField *struct_field1 = new substrait::Expression_ReferenceSegment_StructField();

                        struct_field1->set_field(pair.first);
                        direct_reference1->set_allocated_struct_field(struct_field1);
                        selection1->set_allocated_direct_reference(direct_reference1);
                        value1->set_allocated_selection(selection1);
                        arg1->set_allocated_value(value1);

                        substrait::FunctionArgument *arg2 = new substrait::FunctionArgument();
                        substrait::Expression *value2 = new substrait::Expression();
                        substrait::Expression_Literal *literal = new substrait::Expression_Literal();

                        if (cfilter->constant.type().InternalType() == PhysicalType::VARCHAR) {
                            string *literal_str = new string(cfilter->constant.GetValue<string>());
                            literal->set_allocated_string(literal_str);
                        }

                        value2->set_allocated_literal(literal);
                        arg2->set_allocated_value(value2);

                        scalar_function->set_allocated_output_type(output_type);
                        *scalar_function->add_arguments() = *arg1;
                        *scalar_function->add_arguments() = *arg2;
                        filter->set_allocated_scalar_function(scalar_function);

                        delete arg1;
                        delete arg2;

                        break;
                    }
                }
            }
        }

        read->set_allocated_common(common);
        read->set_allocated_named_table(named_table);
        read->set_allocated_filter(filter);

        table_scan_rel->set_allocated_read(read);

        if (!projection_ids.empty() && projection_ids.size() != column_ids.size()) {
            substrait::Rel *project_rel = new substrait::Rel();
            substrait::ProjectRel *project = new substrait::ProjectRel();
            substrait::RelCommon *project_common = new substrait::RelCommon();
            substrait::RelCommon_Emit *project_emit = new substrait::RelCommon_Emit();

            TableScanBindData* table_bind_data = (TableScanBindData*) bind_data.get();

            for (int i = 0; i < projection_ids.size(); ++i) {
                project_emit->add_output_mapping(projection_ids[i]);
                int col_id = column_ids[projection_ids[i]];
                LogicalIndex idx(col_id);
                const ColumnDefinition& column_definition = table_bind_data->table.GetColumn(idx);
                project_emit->add_output_names(column_definition.GetName());
                project_emit->add_output_types(TypeIdToString(column_definition.GetType().InternalType()));
            }

            project_common->set_allocated_emit(project_emit);
            project->set_allocated_common(project_common);

            project->set_allocated_input(table_scan_rel);

            project_rel->set_allocated_project(project);
            return project_rel;
        }

        return table_scan_rel;
    }

} // namespace duckdb
