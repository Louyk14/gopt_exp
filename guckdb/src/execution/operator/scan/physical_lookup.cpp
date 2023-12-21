#include "duckdb/execution/operator/scan/physical_lookup.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/function/function.hpp"

#include <utility>

namespace duckdb {

    PhysicalLookup::PhysicalLookup(vector<LogicalType> types, TableFunction function_p, idx_t table_index,
                                         unique_ptr<FunctionData> bind_data_p, vector<LogicalType> returned_types_p,
                                         vector<column_t> column_ids_p, vector<idx_t> projection_ids_p,
                                         vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                         idx_t estimated_cardinality, ExtraOperatorInfo extra_info)
            : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, std::move(types), estimated_cardinality),
              function(std::move(function_p)), table_index(table_index), bind_data(std::move(bind_data_p)), returned_types(std::move(returned_types_p)),
              column_ids(std::move(column_ids_p)), projection_ids(std::move(projection_ids_p)), names(std::move(names_p)),
              table_filters(std::move(table_filters_p)), extra_info(extra_info) {
    }

    template <class T>
    void inline PhysicalLookup::Lookup(ClientContext &context, shared_ptr<RowGroupSegmentTree>& row_groups, row_t *row_ids, Vector &result,
                                       idx_t count, idx_t col_index, idx_t type_size) {

        auto& segment_ptrs = segment_ptrs_map[col_index];
        auto result_data = FlatVector::GetData(result);
        idx_t s_size = row_groups->GetRootSegment()->count;
        for (idx_t i = 0; i < count; i++) {
            row_t row_id = row_ids[i];
            idx_t s_index = row_id / s_size;
            idx_t s_offset = row_id % s_size;
            idx_t vector_index = s_offset / STANDARD_VECTOR_SIZE;
            idx_t id_in_vector = s_offset - vector_index * STANDARD_VECTOR_SIZE;
            // get segment buffer
            ColumnData& column = row_groups->GetSegment(s_index)->GetColumn(col_index);
            if (segment_ptrs.find(s_index) == segment_ptrs.end()) {
                auto transient_segment = (ColumnSegment *) column.data.GetSegment(vector_index);
                if (transient_segment->block == nullptr)
                    continue;
                segment_ptrs[s_index] = transient_segment->block->buffer->buffer;
            }

            auto s_data = segment_ptrs[s_index] + ValidityMask::STANDARD_MASK_SIZE;
            memcpy(result_data + (i * type_size), s_data + (id_in_vector * type_size), type_size);
        }
    }

    class LookupGlobalSourceState : public GlobalSourceState {
    public:
        LookupGlobalSourceState(ClientContext &context, const PhysicalLookup &op) {
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

    class LookupLocalSourceState : public LocalSourceState {
    public:
        LookupLocalSourceState(ExecutionContext &context, LookupGlobalSourceState &gstate,
                                  const PhysicalLookup &op) {
            if (op.function.init_local) {
                TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids, op.table_filters.get());
                local_state = op.function.init_local(context, input, gstate.global_state.get());
            }
        }

        unique_ptr<LocalTableFunctionState> local_state;
        Vector *rid_vector;
        DataChunk *rai_chunk;
        //! rows_count
        shared_ptr<rows_vector> rows_filter;
        shared_ptr<bitmask_vector> row_bitmask;
        shared_ptr<bitmask_vector> zone_bitmask;
        row_t rows_count;
        TableFilterSet* table_filters;
        ExpressionExecutor* executor;
        SelectionVector *sel;
    };

    unique_ptr<LocalSourceState> PhysicalLookup::GetLocalSourceState(ExecutionContext &context,
                                                                        GlobalSourceState &gstate) const {
        return make_uniq<LookupLocalSourceState>(context, gstate.Cast<LookupGlobalSourceState>(), *this);
    }

    unique_ptr<GlobalSourceState> PhysicalLookup::GetGlobalSourceState(ClientContext &context) const {
        return make_uniq<LookupGlobalSourceState>(context, *this);
    }

    SourceResultType PhysicalLookup::GetData(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
        D_ASSERT(!column_ids.empty());
        auto &gstate = input.global_state.Cast<LookupGlobalSourceState>();
        auto &state = input.local_state.Cast<LookupLocalSourceState>();

        auto reference_column_idx = state.rai_chunk->ColumnCount() - 1;
        auto fetch_count = state.rai_chunk->size();
        if (fetch_count == 0) {
            return SourceResultType::FINISHED;
        }
        // perform lookups
        auto row_ids = FlatVector::GetData<row_t>(*state.rid_vector);
        vector<column_t> rai_columns;
        chunk.SetCardinality(fetch_count);

        auto &bind = bind_data->Cast<TableScanBindData>();
        auto &table = bind.table.GetStorage();

        for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
            auto col = column_ids[col_idx];
            if (col == COLUMN_IDENTIFIER_ROW_ID || col == table.GetTypes().size()) {
                chunk.data[col_idx].Reference(*state.rid_vector);
            } else if (col == table.GetTypes().size() + 2) {
                chunk.data[col_idx].Reference(state.rai_chunk->data[reference_column_idx]);
            } else {
                /*auto column_type = table.column_definitions[col].GetType();
                if (column_type == LogicalType::TINYINT) {
                    Lookup<int8_t>(context.client, table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, segment_ptrs_map[col_idx], col);
                }
                else if (column_type == LogicalType::TINYINT) {
                    Lookup<int8_t>(context, table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, segment_ptrs_map[col_idx], col);
                }
                else if (column_type == LogicalType::UTINYINT) {
                    Lookup<uint8_t>(context, table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, segment_ptrs_map[col_idx], col);
                }
                else if (column_type == LogicalType::SMALLINT) {
                    Lookup<int16_t>(context, table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, segment_ptrs_map[col_idx], col);
                }
                else if (column_type == LogicalType::HASH || column_type == LogicalType::USMALLINT) {
                    Lookup<uint16_t>(context, table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, segment_ptrs_map[col_idx], col);
                }
                else if (column_type == LogicalType::INTEGER) {
                    Lookup<int32_t>(context, table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, segment_ptrs_map[col_idx], col);
                }
                else if (column_type == LogicalType::UINTEGER) {
                    Lookup<uint32_t>(context, table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, segment_ptrs_map[col_idx], col);
                }
                else if (column_type == LogicalType::TIMESTAMP || column_type == LogicalType::BIGINT) {
                    Lookup<int64_t>(context, table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, segment_ptrs_map[col_idx], col);
                }
                else if (column_type == LogicalType::UBIGINT) {
                    Lookup<uint64_t>(context, table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, segment_ptrs_map[col_idx], col);
                }
                else if (column_type == LogicalType::FLOAT) {
                    Lookup<float_t>(context, table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, segment_ptrs_map[col_idx], col);
                }
                else if (column_type == LogicalType::DOUBLE) {
                    Lookup<double_t>(context, table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, segment_ptrs_map[col_idx], col);
                }
                else if (column_type == LogicalType::POINTER) {
                    Lookup<uintptr_t>(context, table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, segment_ptrs_map[col_idx], col);
                }
                else {
                    //for (idx_t i = 0; i < size; i++) {
                    //    this->GetColumn(cid).FetchRow(transaction, state, rowids->operator[](i + offset), result.data[col_idx], i);
                    //}
                }*/
            }
        }
        // filter
        SelectionVector filter_sel(fetch_count);
        auto result_count = fetch_count;
        if (table_filters->filters.size() > 0) {
            result_count = state.executor->SelectExpression(chunk, filter_sel);
        }
        // reference
        //	auto rai_column_idx = column_ids.size();
        //	for (auto &col : rai_columns) {
        //		if (col == table.types.size()) {
        //			chunk.data[rai_column_idx++].Reference(*rid_vector);
        //		}
        //		if (col == table.types.size() + 2) {
        //			chunk.data[rai_column_idx++].Reference(rai_chunk->data[reference_column_idx]);
        //		}
        //	}
        if (result_count == fetch_count) {
            // nothing was filtered: skip adding any selection vectors
            return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
        }
        // slice
        chunk.Slice(filter_sel, result_count);
        //	rai_chunk->Slice(filter_sel, result_count);
        auto sel_data = state.sel->Slice(filter_sel, result_count);
        state.sel->Initialize(move(sel_data));

        return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
    }

    double PhysicalLookup::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
        auto &gstate = gstate_p.Cast<LookupGlobalSourceState>();
        if (function.table_scan_progress) {
            return function.table_scan_progress(context, bind_data.get(), gstate.global_state.get());
        }
        // if table_scan_progress is not implemented we don't support this function yet in the progress bar
        return -1;
    }

    idx_t PhysicalLookup::GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                           LocalSourceState &lstate) const {
        D_ASSERT(SupportsBatchIndex());
        D_ASSERT(function.get_batch_index);
        auto &gstate = gstate_p.Cast<LookupGlobalSourceState>();
        auto &state = lstate.Cast<LookupLocalSourceState>();
        return function.get_batch_index(context.client, bind_data.get(), state.local_state.get(),
                                        gstate.global_state.get());
    }

    string PhysicalLookup::GetName() const {
        return StringUtil::Upper(function.name + " " + function.extra_info);
    }

    string PhysicalLookup::ParamsToString() const {
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

    bool PhysicalLookup::Equals(const PhysicalOperator &other_p) const {
        if (type != other_p.type) {
            return false;
        }
        auto &other = other_p.Cast<PhysicalLookup>();
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
} // namespace duckdb
