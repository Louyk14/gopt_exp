#include "duckdb/function/table/table_scan.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Table Scan
//===--------------------------------------------------------------------===//
bool TableScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                LocalTableFunctionState *local_state, GlobalTableFunctionState *gstate);

struct TableScanLocalState : public LocalTableFunctionState {
    TableScanLocalState(Vector *rid = nullptr): rid_vector(rid), rai_chunk(nullptr), table_filters(nullptr), executor(nullptr), initialized(false), rows_count(-1), sel(nullptr) {

    }
    bool initialized;
	//! The current position in the scan
	TableScanState scan_state;
	//! The DataChunk containing all read columns (even filter columns that are immediately removed)
	DataChunk all_columns;
    //! rid_vector
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

static storage_t GetStorageIndex(TableCatalogEntry &table, column_t column_id) {
	if (column_id == DConstants::INVALID_INDEX) {
		return column_id;
	}
	auto &col = table.GetColumn(LogicalIndex(column_id));
	return col.StorageOid();
}

struct TableScanGlobalState : public GlobalTableFunctionState {
	TableScanGlobalState(ClientContext &context, const FunctionData *bind_data_p) {
		D_ASSERT(bind_data_p);
		auto &bind_data = bind_data_p->Cast<TableScanBindData>();
		max_threads = bind_data.table.GetStorage().MaxThreads(context);
	}

	ParallelTableScanState state;
	idx_t max_threads;

	vector<idx_t> projection_ids;
	vector<LogicalType> scanned_types;

	idx_t MaxThreads() const override {
		return max_threads;
	}

	bool CanRemoveFilterColumns() const {
		return !projection_ids.empty();
	}
};

static unique_ptr<LocalTableFunctionState> TableScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *gstate) {
	auto result = make_uniq<TableScanLocalState>();
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	vector<column_t> column_ids = input.column_ids;
	for (auto &col : column_ids) {
		auto storage_idx = GetStorageIndex(bind_data.table, col);
		col = storage_idx;
	}
	result->scan_state.Initialize(std::move(column_ids), input.filters.get());
	TableScanParallelStateNext(context.client, input.bind_data.get(), result.get(), gstate);
	if (input.CanRemoveFilterColumns()) {
		auto &tsgs = gstate->Cast<TableScanGlobalState>();
		result->all_columns.Initialize(context.client, tsgs.scanned_types);
	}

    result->rid_vector = input.input_rai.rid_vector;
    result->rai_chunk = input.input_rai.rai_chunk;
    result->rows_filter = input.input_rai.rows_filter;
    result->row_bitmask = input.input_rai.row_bitmask;
    result->zone_bitmask = input.input_rai.zone_bitmask;
    result->rows_count = input.input_rai.rows_count;
    result->table_filters = input.input_rai.table_filters;
    result->executor = input.input_rai.executor.get();
    result->sel = input.input_rai.sel;

    result->scan_state.zones = result->row_bitmask;
    result->scan_state.zones_sel = result->zone_bitmask;
    result->scan_state.rows_count = result->rows_count;
    result->scan_state.rowids = result->rows_filter;

	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> TableScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {

	D_ASSERT(input.bind_data);
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	auto result = make_uniq<TableScanGlobalState>(context, input.bind_data.get());
	bind_data.table.GetStorage().InitializeParallelScan(context, result->state);
	if (input.CanRemoveFilterColumns()) {
		result->projection_ids = input.projection_ids;
		const auto &columns = bind_data.table.GetColumns();
		for (const auto &col_idx : input.column_ids) {
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				result->scanned_types.emplace_back(LogicalType::ROW_TYPE);
			} else {
				result->scanned_types.push_back(columns.GetColumn(LogicalIndex(col_idx)).Type());
			}
		}
	}
	return std::move(result);
}

static unique_ptr<BaseStatistics> TableScanStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                      column_t column_id) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
	if (local_storage.Find(bind_data.table.GetStorage())) {
		// we don't emit any statistics for tables that have outstanding transaction-local data
		return nullptr;
	}
	return bind_data.table.GetStatistics(context, column_id);
}

void PerformSeqScan(DuckTransaction &transaction, DataChunk &chunk, TableScanLocalState &state, DataTable& storage) {
    /*if (!state.initialized) {
        if (state.rows_count != -1) {
            storage.InitializeScan(state.scan_state, state.scan_state.column_ids, state.rows_filter,
                                   state.rows_count, state.table_filters);
        } else if (state.row_bitmask) {
            storage.InitializeScan(state.scan_state, state.scan_state.column_ids, state.row_bitmask,
                                   state.zone_bitmask, state.table_filters);
        } else {
            storage.InitializeScan(transaction, state.scan_state, state.scan_state.column_ids, state.table_filters);
        }
        state.initialized = true;
    }*/
    storage.Scan(transaction, chunk, state.scan_state);
}

template <class T> static void Lookup(shared_ptr<RowGroupSegmentTree>& row_groups, row_t *row_ids, Vector &result, idx_t count, idx_t col_index) {
    auto result_data = FlatVector::GetData(result);
    auto type_size = sizeof(T);

    idx_t s_size = row_groups->GetRootSegment()->count;
    for (idx_t i = 0; i < count; i++) {
        row_t row_id = row_ids[i];
        idx_t s_index = row_id / s_size;
        idx_t s_offset = row_id % s_size;
        idx_t vector_index = s_offset / STANDARD_VECTOR_SIZE;
        idx_t id_in_vector = s_offset - vector_index * STANDARD_VECTOR_SIZE;
        // get segment buffer
        ColumnData& column = row_groups->GetSegment(s_index)->GetColumn(col_index);
        auto transient_segment = (ColumnSegment *) column.data.GetSegment(vector_index);
        auto s_base = transient_segment->block->buffer->buffer;
        auto s_data = s_base + ValidityMask::STANDARD_MASK_SIZE;
        memcpy(result_data + (i * type_size), s_data + (id_in_vector * type_size), type_size);
    }
}

void PerformLookup(DuckTransaction &transaction, DataChunk &chunk, TableScanLocalState& state, SelectionVector *sel,
                   Vector *rid_vector, DataChunk *rai_chunk, DataTable& table) {
    auto fetch_count = rai_chunk->size();
    if (fetch_count == 0) {
        return;
    }
    // perform lookups
    auto row_ids = FlatVector::GetData<row_t>(*rid_vector);
    vector<column_t> rai_columns;
    chunk.SetCardinality(fetch_count);
    for (idx_t col_idx = 0; col_idx < state.scan_state.column_ids.size(); col_idx++) {
        auto col = state.scan_state.column_ids[col_idx];
        if (col == COLUMN_IDENTIFIER_ROW_ID) {
            chunk.data[col_idx].Reference(*rid_vector);
        } else {
            auto column_type = table.column_definitions[col].GetType();
            if (column_type == LogicalType::TINYINT) {
                Lookup<int8_t>(table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, col);
            }
            else if (column_type == LogicalType::TINYINT) {
                Lookup<int8_t>(table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, col);
            }
            else if (column_type == LogicalType::UTINYINT) {
                Lookup<uint8_t>(table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, col);
            }
            else if (column_type == LogicalType::SMALLINT) {
                Lookup<int16_t>(table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, col);
            }
            else if (column_type == LogicalType::HASH || column_type == LogicalType::USMALLINT) {
                Lookup<uint16_t>(table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, col);
            }
            else if (column_type == LogicalType::INTEGER) {
                Lookup<int32_t>(table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, col);
            }
            else if (column_type == LogicalType::UINTEGER) {
                Lookup<uint32_t>(table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, col);
            }
            else if (column_type == LogicalType::TIMESTAMP || column_type == LogicalType::BIGINT) {
                Lookup<int64_t>(table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, col);
            }
            else if (column_type == LogicalType::UBIGINT) {
                Lookup<uint64_t>(table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, col);
            }
            else if (column_type == LogicalType::FLOAT) {
                Lookup<float_t>(table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, col);
            }
            else if (column_type == LogicalType::DOUBLE) {
                Lookup<double_t>(table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, col);
            }
            else if (column_type == LogicalType::POINTER) {
                Lookup<uintptr_t>(table.row_groups->row_groups, row_ids, chunk.data[col_idx], fetch_count, col);
            }
            else {
                //for (idx_t i = 0; i < size; i++) {
                //    this->GetColumn(cid).FetchRow(transaction, state, rowids->operator[](i + offset), result.data[col_idx], i);
                //}
            }
        }
    }
    // filter
    SelectionVector filter_sel(fetch_count);
    auto result_count = fetch_count;
    if (state.table_filters->filters.size() > 0) {
        result_count = state.executor->SelectExpression(chunk, filter_sel);
    }
#if ENABLE_PROFILING
    lookup_size += result_count;
#endif
    if (result_count == fetch_count) {
        // nothing was filtered: skip adding any selection vectors
        return;
    }
    // slice
    chunk.Slice(filter_sel, result_count);
    auto sel_data = sel->Slice(filter_sel, result_count);
    sel->Initialize(move(sel_data));
}


static void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<TableScanBindData>();
	auto &gstate = data_p.global_state->Cast<TableScanGlobalState>();
	auto &state = data_p.local_state->Cast<TableScanLocalState>();
	auto &transaction = DuckTransaction::Get(context, bind_data.table.catalog);
	auto &storage = bind_data.table.GetStorage();
	do {
		if (bind_data.is_create_index) {
			storage.CreateIndexScan(state.scan_state, output,
			                        TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED);
		} else if (gstate.CanRemoveFilterColumns()) {
			state.all_columns.Reset();
            if (state.rid_vector == nullptr) {
                PerformSeqScan(transaction, state.all_columns, state, storage);
            }
            else {
                PerformLookup(transaction, state.all_columns, state, state.sel, state.rid_vector, state.rai_chunk, storage);
            }
			// storage.Scan(transaction, state.all_columns, state.scan_state);
			output.ReferenceColumns(state.all_columns, gstate.projection_ids);
		}
        else {
            //storage.Scan(transaction, output, state.scan_state);
            if (state.rid_vector == nullptr) {
                PerformSeqScan(transaction, output, state, storage);
            } else {
                PerformLookup(transaction, output, state, state.sel, state.rid_vector, state.rai_chunk, storage);
            }
        }
		if (output.size() > 0) {
			return;
		}
		if (!TableScanParallelStateNext(context, data_p.bind_data.get(), data_p.local_state.get(),
		                                data_p.global_state.get())) {
			return;
		}
	} while (true);
}

bool TableScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &parallel_state = global_state->Cast<TableScanGlobalState>();
	auto &state = local_state->Cast<TableScanLocalState>();
	auto &storage = bind_data.table.GetStorage();

	return storage.NextParallelScan(context, parallel_state.state, state.scan_state);
}

double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p,
                         const GlobalTableFunctionState *gstate_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &gstate = gstate_p->Cast<TableScanGlobalState>();
	auto &storage = bind_data.table.GetStorage();
	idx_t total_rows = storage.GetTotalRows();
	if (total_rows == 0) {
		//! Table is either empty or smaller than a vector size, so it is finished
		return 100;
	}
	idx_t scanned_rows = gstate.state.scan_state.processed_rows;
	scanned_rows += gstate.state.local_state.processed_rows;
	auto percentage = 100 * (double(scanned_rows) / total_rows);
	if (percentage > 100) {
		//! In case the last chunk has less elements than STANDARD_VECTOR_SIZE, if our percentage is over 100
		//! It means we finished this table.
		return 100;
	}
	return percentage;
}

idx_t TableScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                             LocalTableFunctionState *local_state, GlobalTableFunctionState *gstate_p) {
	auto &state = local_state->Cast<TableScanLocalState>();
	if (state.scan_state.table_state.row_group) {
		return state.scan_state.table_state.batch_index;
	}
	if (state.scan_state.local_state.row_group) {
		return state.scan_state.table_state.batch_index + state.scan_state.local_state.batch_index;
	}
	return 0;
}

BindInfo TableScanGetBindInfo(const FunctionData *bind_data) {
	return BindInfo(ScanType::TABLE);
}

void TableScanDependency(DependencyList &entries, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	entries.AddDependency(bind_data.table);
}

unique_ptr<NodeStatistics> TableScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
	auto &storage = bind_data.table.GetStorage();
	idx_t estimated_cardinality = storage.info->cardinality + local_storage.AddedRows(bind_data.table.GetStorage());
	return make_uniq<NodeStatistics>(storage.info->cardinality, estimated_cardinality);
}


//===--------------------------------------------------------------------===//
// Index Scan
//===--------------------------------------------------------------------===//
struct IndexScanGlobalState : public GlobalTableFunctionState {
	explicit IndexScanGlobalState(data_ptr_t row_id_data) : row_ids(LogicalType::ROW_TYPE, row_id_data) {
	}

	Vector row_ids;
	ColumnFetchState fetch_state;
	TableScanState local_storage_state;
	vector<storage_t> column_ids;
	bool finished;
};

static unique_ptr<GlobalTableFunctionState> IndexScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	data_ptr_t row_id_data = nullptr;
	if (!bind_data.result_ids.empty()) {
		row_id_data = (data_ptr_t)&bind_data.result_ids[0]; // NOLINT - this is not pretty
	}
	auto result = make_uniq<IndexScanGlobalState>(row_id_data);
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);

	result->column_ids.reserve(input.column_ids.size());
	for (auto &id : input.column_ids) {
		result->column_ids.push_back(GetStorageIndex(bind_data.table, id));
	}
	result->local_storage_state.Initialize(result->column_ids, input.filters.get());
	local_storage.InitializeScan(bind_data.table.GetStorage(), result->local_storage_state.local_state, input.filters);

	result->finished = false;
	return std::move(result);
}

static void IndexScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<TableScanBindData>();
	auto &state = data_p.global_state->Cast<IndexScanGlobalState>();
	auto &transaction = DuckTransaction::Get(context, bind_data.table.catalog);
	auto &local_storage = LocalStorage::Get(transaction);

	if (!state.finished) {
		bind_data.table.GetStorage().Fetch(transaction, output, state.column_ids, state.row_ids,
		                                   bind_data.result_ids.size(), state.fetch_state);
		state.finished = true;
	}
	if (output.size() == 0) {
		local_storage.Scan(state.local_storage_state.local_state, state.column_ids, output);
	}
}

static void RewriteIndexExpression(Index &index, LogicalGet &get, Expression &expr, bool &rewrite_possible) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr.Cast<BoundColumnRefExpression>();
		// bound column ref: rewrite to fit in the current set of bound column ids
		bound_colref.binding.table_index = get.table_index;
		column_t referenced_column = index.column_ids[bound_colref.binding.column_index];
		// search for the referenced column in the set of column_ids
		for (idx_t i = 0; i < get.column_ids.size(); i++) {
			if (get.column_ids[i] == referenced_column) {
				bound_colref.binding.column_index = i;
				return;
			}
		}
		// column id not found in bound columns in the LogicalGet: rewrite not possible
		rewrite_possible = false;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { RewriteIndexExpression(index, get, child, rewrite_possible); });
}

void TableScanPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                    vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &table = bind_data.table;
	auto &storage = table.GetStorage();

	auto &config = ClientConfig::GetConfig(context);
	if (!config.enable_optimizer) {
		// we only push index scans if the optimizer is enabled
		return;
	}
	if (bind_data.is_index_scan) {
		return;
	}
	if (!get.table_filters.filters.empty()) {
		// if there were filters before we can't convert this to an index scan
		return;
	}
	if (!get.projection_ids.empty()) {
		// if columns were pruned by RemoveUnusedColumns we can't convert this to an index scan,
		// because index scan does not support filter_prune (yet)
		return;
	}
	if (filters.empty()) {
		// no indexes or no filters: skip the pushdown
		return;
	}
	// behold
	storage.info->indexes.Scan([&](Index &index) {
		// first rewrite the index expression so the ColumnBindings align with the column bindings of the current table

		if (index.unbound_expressions.size() > 1) {
			// NOTE: index scans are not (yet) supported for compound index keys
			return false;
		}

		auto index_expression = index.unbound_expressions[0]->Copy();
		bool rewrite_possible = true;
		RewriteIndexExpression(index, get, *index_expression, rewrite_possible);
		if (!rewrite_possible) {
			// could not rewrite!
			return false;
		}

		Value low_value, high_value, equal_value;
		ExpressionType low_comparison_type = ExpressionType::INVALID, high_comparison_type = ExpressionType::INVALID;
		// try to find a matching index for any of the filter expressions
		for (auto &filter : filters) {
			auto &expr = *filter;

			// create a matcher for a comparison with a constant
			ComparisonExpressionMatcher matcher;
			// match on a comparison type
			matcher.expr_type = make_uniq<ComparisonExpressionTypeMatcher>();
			// match on a constant comparison with the indexed expression
			matcher.matchers.push_back(make_uniq<ExpressionEqualityMatcher>(*index_expression));
			matcher.matchers.push_back(make_uniq<ConstantExpressionMatcher>());

			matcher.policy = SetMatcher::Policy::UNORDERED;

			vector<reference<Expression>> bindings;
			if (matcher.Match(expr, bindings)) {
				// range or equality comparison with constant value
				// we can use our index here
				// bindings[0] = the expression
				// bindings[1] = the index expression
				// bindings[2] = the constant
				auto &comparison = bindings[0].get().Cast<BoundComparisonExpression>();
				auto constant_value = bindings[2].get().Cast<BoundConstantExpression>().value;
				auto comparison_type = comparison.type;
				if (comparison.left->type == ExpressionType::VALUE_CONSTANT) {
					// the expression is on the right side, we flip them around
					comparison_type = FlipComparisonExpression(comparison_type);
				}
				if (comparison_type == ExpressionType::COMPARE_EQUAL) {
					// equality value
					// equality overrides any other bounds so we just break here
					equal_value = constant_value;
					break;
				} else if (comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
				           comparison_type == ExpressionType::COMPARE_GREATERTHAN) {
					// greater than means this is a lower bound
					low_value = constant_value;
					low_comparison_type = comparison_type;
				} else {
					// smaller than means this is an upper bound
					high_value = constant_value;
					high_comparison_type = comparison_type;
				}
			} else if (expr.type == ExpressionType::COMPARE_BETWEEN) {
				// BETWEEN expression
				auto &between = expr.Cast<BoundBetweenExpression>();
				if (!between.input->Equals(*index_expression)) {
					// expression doesn't match the current index expression
					continue;
				}
				if (between.lower->type != ExpressionType::VALUE_CONSTANT ||
				    between.upper->type != ExpressionType::VALUE_CONSTANT) {
					// not a constant comparison
					continue;
				}
				low_value = (between.lower->Cast<BoundConstantExpression>()).value;
				low_comparison_type = between.lower_inclusive ? ExpressionType::COMPARE_GREATERTHANOREQUALTO
				                                              : ExpressionType::COMPARE_GREATERTHAN;
				high_value = (between.upper->Cast<BoundConstantExpression>()).value;
				high_comparison_type = between.upper_inclusive ? ExpressionType::COMPARE_LESSTHANOREQUALTO
				                                               : ExpressionType::COMPARE_LESSTHAN;
				break;
			}
		}
		if (!equal_value.IsNull() || !low_value.IsNull() || !high_value.IsNull()) {
			// we can scan this index using this predicate: try a scan
			auto &transaction = Transaction::Get(context, bind_data.table.catalog);
			unique_ptr<IndexScanState> index_state;
			if (!equal_value.IsNull()) {
				// equality predicate
				index_state =
				    index.InitializeScanSinglePredicate(transaction, equal_value, ExpressionType::COMPARE_EQUAL);
			} else if (!low_value.IsNull() && !high_value.IsNull()) {
				// two-sided predicate
				index_state = index.InitializeScanTwoPredicates(transaction, low_value, low_comparison_type, high_value,
				                                                high_comparison_type);
			} else if (!low_value.IsNull()) {
				// less than predicate
				index_state = index.InitializeScanSinglePredicate(transaction, low_value, low_comparison_type);
			} else {
				D_ASSERT(!high_value.IsNull());
				index_state = index.InitializeScanSinglePredicate(transaction, high_value, high_comparison_type);
			}
			if (index.Scan(transaction, storage, *index_state, STANDARD_VECTOR_SIZE, bind_data.result_ids)) {
				// use an index scan!
				bind_data.is_index_scan = true;
				get.function = TableScanFunction::GetIndexScanFunction();
			} else {
				bind_data.result_ids.clear();
			}
			return true;
		}
		return false;
	});
}

string TableScanToString(const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	string result = bind_data.table.name;
	return result;
}

static void TableScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	serializer.WriteProperty(100, "catalog", bind_data.table.schema.catalog.GetName());
	serializer.WriteProperty(101, "schema", bind_data.table.schema.name);
	serializer.WriteProperty(102, "table", bind_data.table.name);
	serializer.WriteProperty(103, "is_index_scan", bind_data.is_index_scan);
	serializer.WriteProperty(104, "is_create_index", bind_data.is_create_index);
	serializer.WriteProperty(105, "result_ids", bind_data.result_ids);
}

static unique_ptr<FunctionData> TableScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	auto catalog = deserializer.ReadProperty<string>(100, "catalog");
	auto schema = deserializer.ReadProperty<string>(101, "schema");
	auto table = deserializer.ReadProperty<string>(102, "table");
	auto &catalog_entry =
	    Catalog::GetEntry<TableCatalogEntry>(deserializer.Get<ClientContext &>(), catalog, schema, table);
	if (catalog_entry.type != CatalogType::TABLE_ENTRY) {
		throw SerializationException("Cant find table for %s.%s", schema, table);
	}
	auto result = make_uniq<TableScanBindData>(catalog_entry.Cast<DuckTableEntry>());
	deserializer.ReadProperty(103, "is_index_scan", result->is_index_scan);
	deserializer.ReadProperty(104, "is_create_index", result->is_create_index);
	deserializer.ReadProperty(105, "result_ids", result->result_ids);
	return std::move(result);
}

TableFunction TableScanFunction::GetIndexScanFunction() {
	TableFunction scan_function("index_scan", {}, IndexScanFunction);
	scan_function.init_local = nullptr;
	scan_function.init_global = IndexScanInitGlobal;
	scan_function.statistics = TableScanStatistics;
	scan_function.dependency = TableScanDependency;
	scan_function.cardinality = TableScanCardinality;
	scan_function.pushdown_complex_filter = nullptr;
	scan_function.to_string = TableScanToString;
	scan_function.table_scan_progress = nullptr;
	scan_function.get_batch_index = nullptr;
	scan_function.projection_pushdown = true;
	scan_function.filter_pushdown = false;
	scan_function.serialize = TableScanSerialize;
	scan_function.deserialize = TableScanDeserialize;
	return scan_function;
}

TableFunction TableScanFunction::GetFunction() {
	TableFunction scan_function("seq_scan", {}, TableScanFunc);
	scan_function.init_local = TableScanInitLocal;
	scan_function.init_global = TableScanInitGlobal;
	scan_function.statistics = TableScanStatistics;
	scan_function.dependency = TableScanDependency;
	scan_function.cardinality = TableScanCardinality;
	scan_function.pushdown_complex_filter = TableScanPushdownComplexFilter;
	scan_function.to_string = TableScanToString;
	scan_function.table_scan_progress = TableScanProgress;
	scan_function.get_batch_index = TableScanGetBatchIndex;
	scan_function.get_batch_info = TableScanGetBindInfo;
	scan_function.projection_pushdown = true;
	scan_function.filter_pushdown = true;
	scan_function.filter_prune = true;
	scan_function.serialize = TableScanSerialize;
	scan_function.deserialize = TableScanDeserialize;
	return scan_function;
}

optional_ptr<TableCatalogEntry> TableScanFunction::GetTableEntry(const TableFunction &function,
                                                                 const optional_ptr<FunctionData> bind_data_p) {
	if (function.function != TableScanFunc || !bind_data_p) {
		return nullptr;
	}
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	return &bind_data.table;
}

void TableScanFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet table_scan_set("seq_scan");
	table_scan_set.AddFunction(GetFunction());
	set.AddFunction(std::move(table_scan_set));

	set.AddFunction(GetIndexScanFunction());
}

void BuiltinFunctions::RegisterTableScanFunctions() {
	TableScanFunction::RegisterFunction(*this);
}

} // namespace duckdb
