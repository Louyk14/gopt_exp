#include "duckdb/main/client_context.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/http_state.hpp"
#include "duckdb/common/preserved_error.hpp"
#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/statement/relation_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/pragma_handler.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/join/physical_sip_join.hpp"
#include "duckdb/execution/operator/join/physical_merge_sip_join.hpp"
#include "duckdb/execution/operator/order/physical_top_n.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

struct ActiveQueryContext {
	//! The query that is currently being executed
	string query;
	//! The currently open result
	BaseQueryResult *open_result = nullptr;
	//! Prepared statement data
	shared_ptr<PreparedStatementData> prepared;
	//! The query executor
	unique_ptr<Executor> executor;
	//! The progress bar
	unique_ptr<ProgressBar> progress_bar;
};

ClientContext::ClientContext(shared_ptr<DatabaseInstance> database, int sql_mode_input, string pb_file_input)
    : db(std::move(database)), interrupted(false), client_data(make_uniq<ClientData>(*this)), transaction(*this),
    sql_mode(sql_mode_input), pb_file(pb_file_input){
}

ClientContext::~ClientContext() {
	if (Exception::UncaughtException()) {
		return;
	}
	// destroy the client context and rollback if there is an active transaction
	// but only if we are not destroying this client context as part of an exception stack unwind
	Destroy();
}

unique_ptr<ClientContextLock> ClientContext::LockContext() {
	return make_uniq<ClientContextLock>(context_lock);
}

void ClientContext::Destroy() {
	auto lock = LockContext();
	if (transaction.HasActiveTransaction()) {
		transaction.ResetActiveQuery();
		if (!transaction.IsAutoCommit()) {
			transaction.Rollback();
		}
	}
	CleanupInternal(*lock);
}

unique_ptr<DataChunk> ClientContext::Fetch(ClientContextLock &lock, StreamQueryResult &result) {
	D_ASSERT(IsActiveResult(lock, &result));
	D_ASSERT(active_query->executor);
	return FetchInternal(lock, *active_query->executor, result);
}

unique_ptr<DataChunk> ClientContext::FetchInternal(ClientContextLock &lock, Executor &executor,
                                                   BaseQueryResult &result) {
	bool invalidate_query = true;
	try {
		// fetch the chunk and return it
		auto chunk = executor.FetchChunk();
		if (!chunk || chunk->size() == 0) {
			CleanupInternal(lock, &result);
		}
		return chunk;
	} catch (StandardException &ex) {
		// standard exceptions do not invalidate the current transaction
		result.SetError(PreservedError(ex));
		invalidate_query = false;
	} catch (FatalException &ex) {
		// fatal exceptions invalidate the entire database
		result.SetError(PreservedError(ex));
		auto &db = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db, ex.what());
	} catch (const Exception &ex) {
		result.SetError(PreservedError(ex));
	} catch (std::exception &ex) {
		result.SetError(PreservedError(ex));
	} catch (...) { // LCOV_EXCL_START
		result.SetError(PreservedError("Unhandled exception in FetchInternal"));
	} // LCOV_EXCL_STOP
	CleanupInternal(lock, &result, invalidate_query);
	return nullptr;
}

void ClientContext::BeginTransactionInternal(ClientContextLock &lock, bool requires_valid_transaction) {
	// check if we are on AutoCommit. In this case we should start a transaction
	D_ASSERT(!active_query);
	auto &db = DatabaseInstance::GetDatabase(*this);
	if (ValidChecker::IsInvalidated(db)) {
		throw FatalException(ErrorManager::FormatException(*this, ErrorType::INVALIDATED_DATABASE,
		                                                   ValidChecker::InvalidatedMessage(db)));
	}
	if (requires_valid_transaction && transaction.HasActiveTransaction() &&
	    ValidChecker::IsInvalidated(transaction.ActiveTransaction())) {
		throw Exception(ErrorManager::FormatException(*this, ErrorType::INVALIDATED_TRANSACTION));
	}
	active_query = make_uniq<ActiveQueryContext>();
	if (transaction.IsAutoCommit()) {
		transaction.BeginTransaction();
	}
}

void ClientContext::BeginQueryInternal(ClientContextLock &lock, const string &query) {
	BeginTransactionInternal(lock, false);
	LogQueryInternal(lock, query);
	active_query->query = query;
	query_progress = -1;
	transaction.SetActiveQuery(db->GetDatabaseManager().GetNewQueryNumber());
}

PreservedError ClientContext::EndQueryInternal(ClientContextLock &lock, bool success, bool invalidate_transaction) {
	client_data->profiler->EndQuery();

	if (client_data->http_state) {
		client_data->http_state->Reset();
	}

	// Notify any registered state of query end
	for (auto const &s : registered_state) {
		s.second->QueryEnd();
	}

	D_ASSERT(active_query.get());
	active_query.reset();
	query_progress = -1;
	PreservedError error;
	try {
		if (transaction.HasActiveTransaction()) {
			// Move the query profiler into the history
			auto &prev_profilers = client_data->query_profiler_history->GetPrevProfilers();
			prev_profilers.emplace_back(transaction.GetActiveQuery(), std::move(client_data->profiler));
			// Reinitialize the query profiler
			client_data->profiler = make_shared<QueryProfiler>(*this);
			// Propagate settings of the saved query into the new profiler.
			client_data->profiler->Propagate(*prev_profilers.back().second);
			if (prev_profilers.size() >= client_data->query_profiler_history->GetPrevProfilersSize()) {
				prev_profilers.pop_front();
			}

			transaction.ResetActiveQuery();
			if (transaction.IsAutoCommit()) {
				if (success) {
					transaction.Commit();
				} else {
					transaction.Rollback();
				}
			} else if (invalidate_transaction) {
				D_ASSERT(!success);
				ValidChecker::Invalidate(ActiveTransaction(), "Failed to commit");
			}
		}
	} catch (FatalException &ex) {
		auto &db = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db, ex.what());
		error = PreservedError(ex);
	} catch (const Exception &ex) {
		error = PreservedError(ex);
	} catch (std::exception &ex) {
		error = PreservedError(ex);
	} catch (...) { // LCOV_EXCL_START
		error = PreservedError("Unhandled exception!");
	} // LCOV_EXCL_STOP
	return error;
}

void ClientContext::CleanupInternal(ClientContextLock &lock, BaseQueryResult *result, bool invalidate_transaction) {
	client_data->http_state = make_shared<HTTPState>();
	if (!active_query) {
		// no query currently active
		return;
	}
	if (active_query->executor) {
		active_query->executor->CancelTasks();
	}
	active_query->progress_bar.reset();

	auto error = EndQueryInternal(lock, result ? !result->HasError() : false, invalidate_transaction);
	if (result && !result->HasError()) {
		// if an error occurred while committing report it in the result
		result->SetError(error);
	}
	D_ASSERT(!active_query);
}

Executor &ClientContext::GetExecutor() {
	D_ASSERT(active_query);
	D_ASSERT(active_query->executor);
	return *active_query->executor;
}

const string &ClientContext::GetCurrentQuery() {
	D_ASSERT(active_query);
	return active_query->query;
}

unique_ptr<QueryResult> ClientContext::FetchResultInternal(ClientContextLock &lock, PendingQueryResult &pending) {
	D_ASSERT(active_query);
	D_ASSERT(active_query->open_result == &pending);
	D_ASSERT(active_query->prepared);
	auto &executor = GetExecutor();
	auto &prepared = *active_query->prepared;
	bool create_stream_result = prepared.properties.allow_stream_result && pending.allow_stream_result;
	if (create_stream_result) {
		D_ASSERT(!executor.HasResultCollector());
		active_query->progress_bar.reset();
		query_progress = -1;

		// successfully compiled SELECT clause, and it is the last statement
		// return a StreamQueryResult so the client can call Fetch() on it and stream the result
		auto stream_result = make_uniq<StreamQueryResult>(pending.statement_type, pending.properties,
		                                                  shared_from_this(), pending.types, pending.names);
		active_query->open_result = stream_result.get();
		return std::move(stream_result);
	}
	unique_ptr<QueryResult> result;
	if (executor.HasResultCollector()) {
		// we have a result collector - fetch the result directly from the result collector
		result = executor.GetResult();
		CleanupInternal(lock, result.get(), false);
	} else {
		// no result collector - create a materialized result by continuously fetching
		auto result_collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), pending.types);
		D_ASSERT(!result_collection->Types().empty());
		auto materialized_result =
		    make_uniq<MaterializedQueryResult>(pending.statement_type, pending.properties, pending.names,
		                                       std::move(result_collection), GetClientProperties());

		auto &collection = materialized_result->Collection();
		D_ASSERT(!collection.Types().empty());
		ColumnDataAppendState append_state;
		collection.InitializeAppend(append_state);
		while (true) {
			auto chunk = FetchInternal(lock, GetExecutor(), *materialized_result);
			if (!chunk || chunk->size() == 0) {
				break;
			}
#ifdef DEBUG
			for (idx_t i = 0; i < chunk->ColumnCount(); i++) {
				if (pending.types[i].id() == LogicalTypeId::VARCHAR) {
					chunk->data[i].UTFVerify(chunk->size());
				}
			}
#endif
			collection.Append(append_state, *chunk);
		}
		result = std::move(materialized_result);
	}
	return result;
}

static bool IsExplainAnalyze(SQLStatement *statement) {
	if (!statement) {
		return false;
	}
	if (statement->type != StatementType::EXPLAIN_STATEMENT) {
		return false;
	}
	auto &explain = statement->Cast<ExplainStatement>();
	return explain.explain_type == ExplainType::EXPLAIN_ANALYZE;
}

shared_ptr<PreparedStatementData>
ClientContext::CreatePreparedStatement(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
                                       optional_ptr<case_insensitive_map_t<Value>> values) {
	StatementType statement_type = statement->type;
	auto result = make_shared<PreparedStatementData>(statement_type);

	auto &profiler = QueryProfiler::Get(*this);
	profiler.StartQuery(query, IsExplainAnalyze(statement.get()), true);
	profiler.StartPhase("planner");
	Planner planner(*this);
	if (values) {
		auto &parameter_values = *values;
		for (auto &value : parameter_values) {
			planner.parameter_data.emplace(value.first, BoundParameterData(value.second));
		}
	}

	client_data->http_state = make_shared<HTTPState>();
    if ((query[0] == 's' || query[0] == 'S')) {
        int k = 0;
    }
    planner.CreatePlan(std::move(statement));
	D_ASSERT(planner.plan || !planner.properties.bound_all_parameters);
	profiler.EndPhase();

	auto plan = std::move(planner.plan);
	// extract the result column names from the plan
	result->properties = planner.properties;
	result->names = planner.names;
	result->types = planner.types;
	result->value_map = std::move(planner.value_map);
	result->catalog_version = MetaTransaction::Get(*this).catalog_version;

	if (!planner.properties.bound_all_parameters) {
		return result;
	}
#ifdef DEBUG
	plan->Verify(*this);
#endif
	if (config.enable_optimizer && plan->RequireOptimizer()) {
		profiler.StartPhase("optimizer");
		Optimizer optimizer(*planner.binder, *this);
		plan = optimizer.Optimize(std::move(plan));
		D_ASSERT(plan);
		profiler.EndPhase();

#ifdef DEBUG
		plan->Verify(*this);
#endif
	}

	profiler.StartPhase("physical_planner");
	// now convert logical query plan into a physical query plan
	PhysicalPlanGenerator physical_planner(*this);
	auto physical_plan = physical_planner.CreatePlan(std::move(plan));
	profiler.EndPhase();

    if (sql_mode == 2 && (query[0] == 's' || query[0] == 'S')) {
        if (pb_file == "1-1") {
            auto physical_plan_by_hand = GenerateIC11Plan();
            physical_plan = move(physical_plan_by_hand);
        }
        else if (pb_file == "1-2") {
            auto physical_plan_by_hand = GenerateIC12Plan();
            physical_plan = move(physical_plan_by_hand);
        }
        else if (pb_file == "1-3") {
            auto physical_plan_by_hand = GenerateIC13Plan();
            physical_plan = move(physical_plan_by_hand);
        }
        else if (pb_file == "2-1") {
            auto physical_plan_by_hand = GenerateIC21Plan();
            physical_plan = move(physical_plan_by_hand);
        }
        else if (pb_file == "3-1") {
            auto physical_plan_by_hand = GenerateIC31Plan();
            physical_plan = move(physical_plan_by_hand);
        }
        else if (pb_file == "5-1") {
            auto physical_plan_by_hand = GenerateIC51Plan();
            physical_plan = move(physical_plan_by_hand);
        }
        else if (pb_file == "5-2") {
            auto physical_plan_by_hand = GenerateIC52PlanByPassFromPerson();
            physical_plan = move(physical_plan_by_hand);
        }
    }

    if (sql_mode == 1 && (query[0] == 's' || query[0] == 'S')) {
		// std::cout << physical_plan->ToString() << std::endl;
        unordered_map<int, string> tableid2name;
        // planner.binder->bind_context.GetBindingsMap(tableid2name);
        // string pb_file = "output.log";
        pb_serializer.SerializeToFile(pb_file, physical_plan.get(), tableid2name);
	}
#ifdef DEBUG
	D_ASSERT(!physical_plan->ToString().empty());
#endif
	result->plan = std::move(physical_plan);
	return result;
}

double ClientContext::GetProgress() {
	return query_progress.load();
}

unique_ptr<PendingQueryResult> ClientContext::PendingPreparedStatement(ClientContextLock &lock,
                                                                       shared_ptr<PreparedStatementData> statement_p,
                                                                       const PendingQueryParameters &parameters) {
	D_ASSERT(active_query);
	auto &statement = *statement_p;
	if (ValidChecker::IsInvalidated(ActiveTransaction()) && statement.properties.requires_valid_transaction) {
		throw Exception(ErrorManager::FormatException(*this, ErrorType::INVALIDATED_TRANSACTION));
	}
	auto &transaction = MetaTransaction::Get(*this);
	auto &manager = DatabaseManager::Get(*this);
	for (auto &modified_database : statement.properties.modified_databases) {
		auto entry = manager.GetDatabase(*this, modified_database);
		if (!entry) {
			throw InternalException("Database \"%s\" not found", modified_database);
		}
		if (entry->IsReadOnly()) {
			throw Exception(StringUtil::Format(
			    "Cannot execute statement of type \"%s\" on database \"%s\" which is attached in read-only mode!",
			    StatementTypeToString(statement.statement_type), modified_database));
		}
		transaction.ModifyDatabase(*entry);
	}

	// bind the bound values before execution
	case_insensitive_map_t<Value> owned_values;
	if (parameters.parameters) {
		auto &params = *parameters.parameters;
		for (auto &val : params) {
			owned_values.emplace(val);
		}
	}
	statement.Bind(std::move(owned_values));

	active_query->executor = make_uniq<Executor>(*this);
	auto &executor = *active_query->executor;
	if (config.enable_progress_bar) {
		progress_bar_display_create_func_t display_create_func = nullptr;
		if (config.print_progress_bar) {
			// If a custom display is set, use that, otherwise just use the default
			display_create_func =
			    config.display_create_func ? config.display_create_func : ProgressBar::DefaultProgressBarDisplay;
		}
		active_query->progress_bar = make_uniq<ProgressBar>(executor, config.wait_time, display_create_func);
		active_query->progress_bar->Start();
		query_progress = 0;
	}
	auto stream_result = parameters.allow_stream_result && statement.properties.allow_stream_result;
	if (!stream_result && statement.properties.return_type == StatementReturnType::QUERY_RESULT) {
		unique_ptr<PhysicalResultCollector> collector;
		auto &config = ClientConfig::GetConfig(*this);
		auto get_method =
		    config.result_collector ? config.result_collector : PhysicalResultCollector::GetResultCollector;
		collector = get_method(*this, statement);
		D_ASSERT(collector->type == PhysicalOperatorType::RESULT_COLLECTOR);
		executor.Initialize(std::move(collector));
	} else {
		executor.Initialize(*statement.plan);
	}
	auto types = executor.GetTypes();
	D_ASSERT(types == statement.types);
	D_ASSERT(!active_query->open_result);

	auto pending_result =
	    make_uniq<PendingQueryResult>(shared_from_this(), *statement_p, std::move(types), stream_result);
	active_query->prepared = std::move(statement_p);
	active_query->open_result = pending_result.get();
	return pending_result;
}

PendingExecutionResult ClientContext::ExecuteTaskInternal(ClientContextLock &lock, PendingQueryResult &result) {
	D_ASSERT(active_query);
	D_ASSERT(active_query->open_result == &result);
	try {
		auto result = active_query->executor->ExecuteTask();
		if (active_query->progress_bar) {
			active_query->progress_bar->Update(result == PendingExecutionResult::RESULT_READY);
			query_progress = active_query->progress_bar->GetCurrentPercentage();
		}
		return result;
	} catch (FatalException &ex) {
		// fatal exceptions invalidate the entire database
		result.SetError(PreservedError(ex));
		auto &db = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db, ex.what());
	} catch (const Exception &ex) {
		result.SetError(PreservedError(ex));
	} catch (std::exception &ex) {
		result.SetError(PreservedError(ex));
	} catch (...) { // LCOV_EXCL_START
		result.SetError(PreservedError("Unhandled exception in ExecuteTaskInternal"));
	} // LCOV_EXCL_STOP
	EndQueryInternal(lock, false, true);
	return PendingExecutionResult::EXECUTION_ERROR;
}

void ClientContext::InitialCleanup(ClientContextLock &lock) {
	//! Cleanup any open results and reset the interrupted flag
	CleanupInternal(lock);
	interrupted = false;
}

vector<unique_ptr<SQLStatement>> ClientContext::ParseStatements(const string &query) {
	auto lock = LockContext();
	return ParseStatementsInternal(*lock, query);
}

vector<unique_ptr<SQLStatement>> ClientContext::ParseStatementsInternal(ClientContextLock &lock, const string &query) {
	Parser parser(GetParserOptions());
	parser.ParseQuery(query);

	PragmaHandler handler(*this);
	handler.HandlePragmaStatements(lock, parser.statements);

	return std::move(parser.statements);
}

void ClientContext::HandlePragmaStatements(vector<unique_ptr<SQLStatement>> &statements) {
	auto lock = LockContext();

	PragmaHandler handler(*this);
	handler.HandlePragmaStatements(*lock, statements);
}

unique_ptr<LogicalOperator> ClientContext::ExtractPlan(const string &query) {
	auto lock = LockContext();

	auto statements = ParseStatementsInternal(*lock, query);
	if (statements.size() != 1) {
		throw Exception("ExtractPlan can only prepare a single statement");
	}

	unique_ptr<LogicalOperator> plan;
	client_data->http_state = make_shared<HTTPState>();
	RunFunctionInTransactionInternal(*lock, [&]() {
		Planner planner(*this);
		planner.CreatePlan(std::move(statements[0]));
		D_ASSERT(planner.plan);

		plan = std::move(planner.plan);

		if (config.enable_optimizer) {
			Optimizer optimizer(*planner.binder, *this);
			plan = optimizer.Optimize(std::move(plan));
		}

		ColumnBindingResolver resolver;
		resolver.Verify(*plan);
		resolver.VisitOperator(*plan);

		plan->ResolveOperatorTypes();
	});
	return plan;
}

unique_ptr<PreparedStatement> ClientContext::PrepareInternal(ClientContextLock &lock,
                                                             unique_ptr<SQLStatement> statement) {
	auto n_param = statement->n_param;
	auto named_param_map = std::move(statement->named_param_map);
	auto statement_query = statement->query;
	shared_ptr<PreparedStatementData> prepared_data;
	auto unbound_statement = statement->Copy();
	RunFunctionInTransactionInternal(
	    lock, [&]() { prepared_data = CreatePreparedStatement(lock, statement_query, std::move(statement)); }, false);
	prepared_data->unbound_statement = std::move(unbound_statement);
	return make_uniq<PreparedStatement>(shared_from_this(), std::move(prepared_data), std::move(statement_query),
	                                    n_param, std::move(named_param_map));
}

unique_ptr<PreparedStatement> ClientContext::Prepare(unique_ptr<SQLStatement> statement) {
	auto lock = LockContext();
	// prepare the query
	try {
		InitialCleanup(*lock);
		return PrepareInternal(*lock, std::move(statement));
	} catch (const Exception &ex) {
		return make_uniq<PreparedStatement>(PreservedError(ex));
	} catch (std::exception &ex) {
		return make_uniq<PreparedStatement>(PreservedError(ex));
	}
}

unique_ptr<PreparedStatement> ClientContext::Prepare(const string &query) {
	auto lock = LockContext();
	// prepare the query
	try {
		InitialCleanup(*lock);

		// first parse the query
		auto statements = ParseStatementsInternal(*lock, query);
		if (statements.empty()) {
			throw Exception("No statement to prepare!");
		}
		if (statements.size() > 1) {
			throw Exception("Cannot prepare multiple statements at once!");
		}
		return PrepareInternal(*lock, std::move(statements[0]));
	} catch (const Exception &ex) {
		return make_uniq<PreparedStatement>(PreservedError(ex));
	} catch (std::exception &ex) {
		return make_uniq<PreparedStatement>(PreservedError(ex));
	}
}

unique_ptr<PendingQueryResult> ClientContext::PendingQueryPreparedInternal(ClientContextLock &lock, const string &query,
                                                                           shared_ptr<PreparedStatementData> &prepared,
                                                                           const PendingQueryParameters &parameters) {
	try {
		InitialCleanup(lock);
	} catch (const Exception &ex) {
		return make_uniq<PendingQueryResult>(PreservedError(ex));
	} catch (std::exception &ex) {
		return make_uniq<PendingQueryResult>(PreservedError(ex));
	}
	return PendingStatementOrPreparedStatementInternal(lock, query, nullptr, prepared, parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(const string &query,
                                                           shared_ptr<PreparedStatementData> &prepared,
                                                           const PendingQueryParameters &parameters) {
	auto lock = LockContext();
	return PendingQueryPreparedInternal(*lock, query, prepared, parameters);
}

unique_ptr<QueryResult> ClientContext::Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
                                               const PendingQueryParameters &parameters) {
	auto lock = LockContext();
	auto pending = PendingQueryPreparedInternal(*lock, query, prepared, parameters);
	if (pending->HasError()) {
		return make_uniq<MaterializedQueryResult>(pending->GetErrorObject());
	}
	return pending->ExecuteInternal(*lock);
}

unique_ptr<QueryResult> ClientContext::Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
                                               case_insensitive_map_t<Value> &values, bool allow_stream_result) {
	PendingQueryParameters parameters;
	parameters.parameters = &values;
	parameters.allow_stream_result = allow_stream_result;
	return Execute(query, prepared, parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingStatementInternal(ClientContextLock &lock, const string &query,
                                                                       unique_ptr<SQLStatement> statement,
                                                                       const PendingQueryParameters &parameters) {
	// prepare the query for execution
	auto prepared = CreatePreparedStatement(lock, query, std::move(statement), parameters.parameters);
	idx_t parameter_count = !parameters.parameters ? 0 : parameters.parameters->size();
	if (prepared->properties.parameter_count > 0 && parameter_count == 0) {
		string error_message = StringUtil::Format("Expected %lld parameters, but none were supplied",
		                                          prepared->properties.parameter_count);
		return make_uniq<PendingQueryResult>(PreservedError(error_message));
	}
	if (!prepared->properties.bound_all_parameters) {
		return make_uniq<PendingQueryResult>(PreservedError("Not all parameters were bound"));
	}
	// execute the prepared statement
	return PendingPreparedStatement(lock, std::move(prepared), parameters);
}

unique_ptr<QueryResult> ClientContext::RunStatementInternal(ClientContextLock &lock, const string &query,
                                                            unique_ptr<SQLStatement> statement,
                                                            bool allow_stream_result, bool verify) {
	PendingQueryParameters parameters;
	parameters.allow_stream_result = allow_stream_result;
	auto pending = PendingQueryInternal(lock, std::move(statement), parameters, verify);
	if (pending->HasError()) {
		return make_uniq<MaterializedQueryResult>(pending->GetErrorObject());
	}
	return ExecutePendingQueryInternal(lock, *pending);
}

bool ClientContext::IsActiveResult(ClientContextLock &lock, BaseQueryResult *result) {
	if (!active_query) {
		return false;
	}
	return active_query->open_result == result;
}

unique_ptr<PendingQueryResult> ClientContext::PendingStatementOrPreparedStatementInternal(
    ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
    shared_ptr<PreparedStatementData> &prepared, const PendingQueryParameters &parameters) {
	// check if we are on AutoCommit. In this case we should start a transaction.
	if (statement && config.AnyVerification()) {
		// query verification is enabled
		// create a copy of the statement, and use the copy
		// this way we verify that the copy correctly copies all properties
		auto copied_statement = statement->Copy();
		switch (statement->type) {
		case StatementType::SELECT_STATEMENT: {
			// in case this is a select query, we verify the original statement
			PreservedError error;
			try {
				error = VerifyQuery(lock, query, std::move(statement));
			} catch (const Exception &ex) {
				error = PreservedError(ex);
			} catch (std::exception &ex) {
				error = PreservedError(ex);
			}
			if (error) {
				// error in verifying query
				return make_uniq<PendingQueryResult>(error);
			}
			statement = std::move(copied_statement);
			break;
		}
#ifndef DUCKDB_ALTERNATIVE_VERIFY
		case StatementType::COPY_STATEMENT:
		case StatementType::INSERT_STATEMENT:
		case StatementType::DELETE_STATEMENT:
		case StatementType::UPDATE_STATEMENT: {
			Parser parser;
			PreservedError error;
			try {
				parser.ParseQuery(statement->ToString());
			} catch (const Exception &ex) {
				error = PreservedError(ex);
			} catch (std::exception &ex) {
				error = PreservedError(ex);
			}
			if (error) {
				// error in verifying query
				return make_uniq<PendingQueryResult>(error);
			}
			statement = std::move(parser.statements[0]);
			break;
		}
#endif
		default:
			statement = std::move(copied_statement);
			break;
		}
	}
	return PendingStatementOrPreparedStatement(lock, query, std::move(statement), prepared, parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingStatementOrPreparedStatement(
    ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
    shared_ptr<PreparedStatementData> &prepared, const PendingQueryParameters &parameters) {
	unique_ptr<PendingQueryResult> result;

	try {
		BeginQueryInternal(lock, query);
	} catch (FatalException &ex) {
		// fatal exceptions invalidate the entire database
		auto &db = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db, ex.what());
		result = make_uniq<PendingQueryResult>(PreservedError(ex));
		return result;
	} catch (const Exception &ex) {
		return make_uniq<PendingQueryResult>(PreservedError(ex));
	} catch (std::exception &ex) {
		return make_uniq<PendingQueryResult>(PreservedError(ex));
	}
	// start the profiler
	auto &profiler = QueryProfiler::Get(*this);
	profiler.StartQuery(query, IsExplainAnalyze(statement ? statement.get() : prepared->unbound_statement.get()));

	bool invalidate_query = true;
	try {
		if (statement) {
			result = PendingStatementInternal(lock, query, std::move(statement), parameters);
		} else {
			if (prepared->RequireRebind(*this, parameters.parameters)) {
				// catalog was modified: rebind the statement before execution
				auto new_prepared =
				    CreatePreparedStatement(lock, query, prepared->unbound_statement->Copy(), parameters.parameters);
				D_ASSERT(new_prepared->properties.bound_all_parameters);
				new_prepared->unbound_statement = std::move(prepared->unbound_statement);
				prepared = std::move(new_prepared);
				prepared->properties.bound_all_parameters = false;
			}
			result = PendingPreparedStatement(lock, prepared, parameters);
		}
	} catch (StandardException &ex) {
		// standard exceptions do not invalidate the current transaction
		result = make_uniq<PendingQueryResult>(PreservedError(ex));
		invalidate_query = false;
	} catch (FatalException &ex) {
		// fatal exceptions invalidate the entire database
		if (!config.query_verification_enabled) {
			auto &db = DatabaseInstance::GetDatabase(*this);
			ValidChecker::Invalidate(db, ex.what());
		}
		result = make_uniq<PendingQueryResult>(PreservedError(ex));
	} catch (const Exception &ex) {
		// other types of exceptions do invalidate the current transaction
		result = make_uniq<PendingQueryResult>(PreservedError(ex));
	} catch (std::exception &ex) {
		// other types of exceptions do invalidate the current transaction
		result = make_uniq<PendingQueryResult>(PreservedError(ex));
	}
	if (result->HasError()) {
		// query failed: abort now
		EndQueryInternal(lock, false, invalidate_query);
		return result;
	}
	D_ASSERT(active_query->open_result == result.get());
	return result;
}

void ClientContext::LogQueryInternal(ClientContextLock &, const string &query) {
	if (!client_data->log_query_writer) {
#ifdef DUCKDB_FORCE_QUERY_LOG
		try {
			string log_path(DUCKDB_FORCE_QUERY_LOG);
			client_data->log_query_writer =
			    make_uniq<BufferedFileWriter>(FileSystem::GetFileSystem(*this), log_path,
			                                  BufferedFileWriter::DEFAULT_OPEN_FLAGS, client_data->file_opener.get());
		} catch (...) {
			return;
		}
#else
		return;
#endif
	}
	// log query path is set: log the query
	client_data->log_query_writer->WriteData(const_data_ptr_cast(query.c_str()), query.size());
	client_data->log_query_writer->WriteData(const_data_ptr_cast("\n"), 1);
	client_data->log_query_writer->Flush();
	client_data->log_query_writer->Sync();
}

unique_ptr<QueryResult> ClientContext::Query(unique_ptr<SQLStatement> statement, bool allow_stream_result) {
	auto pending_query = PendingQuery(std::move(statement), allow_stream_result);
	if (pending_query->HasError()) {
		return make_uniq<MaterializedQueryResult>(pending_query->GetErrorObject());
	}
	return pending_query->Execute();
}

unique_ptr<QueryResult> ClientContext::Query(const string &query, bool allow_stream_result) {
	auto lock = LockContext();

	PreservedError error;
	vector<unique_ptr<SQLStatement>> statements;
	if (!ParseStatements(*lock, query, statements, error)) {
		return make_uniq<MaterializedQueryResult>(std::move(error));
	}
	if (statements.empty()) {
		// no statements, return empty successful result
		StatementProperties properties;
		vector<string> names;
		auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator());
		return make_uniq<MaterializedQueryResult>(StatementType::INVALID_STATEMENT, properties, std::move(names),
		                                          std::move(collection), GetClientProperties());
	}

	unique_ptr<QueryResult> result;
	QueryResult *last_result = nullptr;
	bool last_had_result = false;
	for (idx_t i = 0; i < statements.size(); i++) {
		auto &statement = statements[i];
		bool is_last_statement = i + 1 == statements.size();
		PendingQueryParameters parameters;
		parameters.allow_stream_result = allow_stream_result && is_last_statement;
		auto pending_query = PendingQueryInternal(*lock, std::move(statement), parameters);
		auto has_result = pending_query->properties.return_type == StatementReturnType::QUERY_RESULT;

        // std::cout << "come here" << std::endl;
	if (sql_mode == 1) {
            return result;
        }

		unique_ptr<QueryResult> current_result;
		if (pending_query->HasError()) {
			current_result = make_uniq<MaterializedQueryResult>(pending_query->GetErrorObject());
		} else {
			current_result = ExecutePendingQueryInternal(*lock, *pending_query);
		}
		// now append the result to the list of results
		if (!last_result || !last_had_result) {
			// first result of the query
			result = std::move(current_result);
			last_result = result.get();
			last_had_result = has_result;
		} else {
			// later results; attach to the result chain
			// but only if there is a result
			if (!has_result) {
				continue;
			}
			last_result->next = std::move(current_result);
			last_result = last_result->next.get();
		}
	}
	return result;
}

bool ClientContext::ParseStatements(ClientContextLock &lock, const string &query,
                                    vector<unique_ptr<SQLStatement>> &result, PreservedError &error) {
	try {
		InitialCleanup(lock);
		// parse the query and transform it into a set of statements
		result = ParseStatementsInternal(lock, query);
		return true;
	} catch (const Exception &ex) {
		error = PreservedError(ex);
		return false;
	} catch (std::exception &ex) {
		error = PreservedError(ex);
		return false;
	}
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(const string &query, bool allow_stream_result) {
	auto lock = LockContext();

	PreservedError error;
	vector<unique_ptr<SQLStatement>> statements;
	if (!ParseStatements(*lock, query, statements, error)) {
		return make_uniq<PendingQueryResult>(std::move(error));
	}
	if (statements.size() != 1) {
		return make_uniq<PendingQueryResult>(PreservedError("PendingQuery can only take a single statement"));
	}
	PendingQueryParameters parameters;
	parameters.allow_stream_result = allow_stream_result;
	return PendingQueryInternal(*lock, std::move(statements[0]), parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(unique_ptr<SQLStatement> statement,
                                                           bool allow_stream_result) {
	auto lock = LockContext();
	PendingQueryParameters parameters;
	parameters.allow_stream_result = allow_stream_result;
	return PendingQueryInternal(*lock, std::move(statement), parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQueryInternal(ClientContextLock &lock,
                                                                   unique_ptr<SQLStatement> statement,
                                                                   const PendingQueryParameters &parameters,
                                                                   bool verify) {
	auto query = statement->query;
	shared_ptr<PreparedStatementData> prepared;
	if (verify) {
		return PendingStatementOrPreparedStatementInternal(lock, query, std::move(statement), prepared, parameters);
	} else {
		return PendingStatementOrPreparedStatement(lock, query, std::move(statement), prepared, parameters);
	}
}

unique_ptr<QueryResult> ClientContext::ExecutePendingQueryInternal(ClientContextLock &lock, PendingQueryResult &query) {
	return query.ExecuteInternal(lock);
}

void ClientContext::Interrupt() {
	interrupted = true;
}

void ClientContext::EnableProfiling() {
	auto lock = LockContext();
	auto &config = ClientConfig::GetConfig(*this);
	config.enable_profiler = true;
	config.emit_profiler_output = true;
}

void ClientContext::DisableProfiling() {
	auto lock = LockContext();
	auto &config = ClientConfig::GetConfig(*this);
	config.enable_profiler = false;
}

void ClientContext::RegisterFunction(CreateFunctionInfo &info) {
	RunFunctionInTransaction([&]() {
		auto existing_function = Catalog::GetEntry<ScalarFunctionCatalogEntry>(*this, INVALID_CATALOG, info.schema,
		                                                                       info.name, OnEntryNotFound::RETURN_NULL);
		if (existing_function) {
			auto &new_info = info.Cast<CreateScalarFunctionInfo>();
			if (new_info.functions.MergeFunctionSet(existing_function->functions)) {
				// function info was updated from catalog entry, rewrite is needed
				info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
			}
		}
		// create function
		auto &catalog = Catalog::GetSystemCatalog(*this);
		catalog.CreateFunction(*this, info);
	});
}

void ClientContext::RunFunctionInTransactionInternal(ClientContextLock &lock, const std::function<void(void)> &fun,
                                                     bool requires_valid_transaction) {
	if (requires_valid_transaction && transaction.HasActiveTransaction() &&
	    ValidChecker::IsInvalidated(ActiveTransaction())) {
		throw TransactionException(ErrorManager::FormatException(*this, ErrorType::INVALIDATED_TRANSACTION));
	}
	// check if we are on AutoCommit. In this case we should start a transaction
	bool require_new_transaction = transaction.IsAutoCommit() && !transaction.HasActiveTransaction();
	if (require_new_transaction) {
		D_ASSERT(!active_query);
		transaction.BeginTransaction();
	}
	try {
		fun();
	} catch (StandardException &ex) {
		if (require_new_transaction) {
			transaction.Rollback();
		}
		throw;
	} catch (FatalException &ex) {
		auto &db = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db, ex.what());
		throw;
	} catch (std::exception &ex) {
		if (require_new_transaction) {
			transaction.Rollback();
		} else {
			ValidChecker::Invalidate(ActiveTransaction(), ex.what());
		}
		throw;
	}
	if (require_new_transaction) {
		transaction.Commit();
	}
}

void ClientContext::RunFunctionInTransaction(const std::function<void(void)> &fun, bool requires_valid_transaction) {
	auto lock = LockContext();
	RunFunctionInTransactionInternal(*lock, fun, requires_valid_transaction);
}

unique_ptr<TableDescription> ClientContext::TableInfo(const string &schema_name, const string &table_name) {
	unique_ptr<TableDescription> result;
	RunFunctionInTransaction([&]() {
		// obtain the table info
		auto table = Catalog::GetEntry<TableCatalogEntry>(*this, INVALID_CATALOG, schema_name, table_name,
		                                                  OnEntryNotFound::RETURN_NULL);
		if (!table) {
			return;
		}
		// write the table info to the result
		result = make_uniq<TableDescription>();
		result->schema = schema_name;
		result->table = table_name;
		for (auto &column : table->GetColumns().Logical()) {
			result->columns.emplace_back(column.Name(), column.Type());
		}
	});
	return result;
}

void ClientContext::Append(TableDescription &description, ColumnDataCollection &collection) {
	RunFunctionInTransaction([&]() {
		auto &table_entry =
		    Catalog::GetEntry<TableCatalogEntry>(*this, INVALID_CATALOG, description.schema, description.table);
		// verify that the table columns and types match up
		if (description.columns.size() != table_entry.GetColumns().PhysicalColumnCount()) {
			throw Exception("Failed to append: table entry has different number of columns!");
		}
		for (idx_t i = 0; i < description.columns.size(); i++) {
			if (description.columns[i].Type() != table_entry.GetColumns().GetColumn(PhysicalIndex(i)).Type()) {
				throw Exception("Failed to append: table entry has different number of columns!");
			}
		}
		table_entry.GetStorage().LocalAppend(table_entry, *this, collection);
	});
}

void ClientContext::TryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns) {
#ifdef DEBUG
	D_ASSERT(!relation.GetAlias().empty());
	D_ASSERT(!relation.ToString().empty());
#endif
	client_data->http_state = make_shared<HTTPState>();
	RunFunctionInTransaction([&]() {
		// bind the expressions
		auto binder = Binder::CreateBinder(*this);
		auto result = relation.Bind(*binder);
		D_ASSERT(result.names.size() == result.types.size());

		result_columns.reserve(result_columns.size() + result.names.size());
		for (idx_t i = 0; i < result.names.size(); i++) {
			result_columns.emplace_back(result.names[i], result.types[i]);
		}
	});
}

unordered_set<string> ClientContext::GetTableNames(const string &query) {
	auto lock = LockContext();

	auto statements = ParseStatementsInternal(*lock, query);
	if (statements.size() != 1) {
		throw InvalidInputException("Expected a single statement");
	}

	unordered_set<string> result;
	RunFunctionInTransactionInternal(*lock, [&]() {
		// bind the expressions
		auto binder = Binder::CreateBinder(*this);
		binder->SetBindingMode(BindingMode::EXTRACT_NAMES);
		binder->Bind(*statements[0]);
		result = binder->GetTableNames();
	});
	return result;
}

unique_ptr<PendingQueryResult> ClientContext::PendingQueryInternal(ClientContextLock &lock,
                                                                   const shared_ptr<Relation> &relation,
                                                                   bool allow_stream_result) {
	InitialCleanup(lock);

	string query;
	if (config.query_verification_enabled) {
		// run the ToString method of any relation we run, mostly to ensure it doesn't crash
		relation->ToString();
		relation->GetAlias();
		if (relation->IsReadOnly()) {
			// verify read only statements by running a select statement
			auto select = make_uniq<SelectStatement>();
			select->node = relation->GetQueryNode();
			RunStatementInternal(lock, query, std::move(select), false);
		}
	}

	auto relation_stmt = make_uniq<RelationStatement>(relation);
	PendingQueryParameters parameters;
	parameters.allow_stream_result = allow_stream_result;
	return PendingQueryInternal(lock, std::move(relation_stmt), parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(const shared_ptr<Relation> &relation,
                                                           bool allow_stream_result) {
	auto lock = LockContext();
	return PendingQueryInternal(*lock, relation, allow_stream_result);
}

unique_ptr<QueryResult> ClientContext::Execute(const shared_ptr<Relation> &relation) {
	auto lock = LockContext();
	auto &expected_columns = relation->Columns();
	auto pending = PendingQueryInternal(*lock, relation, false);
	if (!pending->success) {
		return make_uniq<MaterializedQueryResult>(pending->GetErrorObject());
	}

	unique_ptr<QueryResult> result;
	result = ExecutePendingQueryInternal(*lock, *pending);
	if (result->HasError()) {
		return result;
	}
	// verify that the result types and result names of the query match the expected result types/names
	if (result->types.size() == expected_columns.size()) {
		bool mismatch = false;
		for (idx_t i = 0; i < result->types.size(); i++) {
			if (result->types[i] != expected_columns[i].Type() || result->names[i] != expected_columns[i].Name()) {
				mismatch = true;
				break;
			}
		}
		if (!mismatch) {
			// all is as expected: return the result
			return result;
		}
	}
	// result mismatch
	string err_str = "Result mismatch in query!\nExpected the following columns: [";
	for (idx_t i = 0; i < expected_columns.size(); i++) {
		if (i > 0) {
			err_str += ", ";
		}
		err_str += expected_columns[i].Name() + " " + expected_columns[i].Type().ToString();
	}
	err_str += "]\nBut result contained the following: ";
	for (idx_t i = 0; i < result->types.size(); i++) {
		err_str += i == 0 ? "[" : ", ";
		err_str += result->names[i] + " " + result->types[i].ToString();
	}
	err_str += "]";
	return make_uniq<MaterializedQueryResult>(PreservedError(err_str));
}

bool ClientContext::TryGetCurrentSetting(const std::string &key, Value &result) {
	// first check the built-in settings
	auto &db_config = DBConfig::GetConfig(*this);
	auto option = db_config.GetOptionByName(key);
	if (option) {
		result = option->get_setting(*this);
		return true;
	}

	// check the client session values
	const auto &session_config_map = config.set_variables;

	auto session_value = session_config_map.find(key);
	bool found_session_value = session_value != session_config_map.end();
	if (found_session_value) {
		result = session_value->second;
		return true;
	}
	// finally check the global session values
	return db->TryGetCurrentSetting(key, result);
}

ParserOptions ClientContext::GetParserOptions() const {
	auto &client_config = ClientConfig::GetConfig(*this);
	ParserOptions options;
	options.preserve_identifier_case = client_config.preserve_identifier_case;
	options.integer_division = client_config.integer_division;
	options.max_expression_depth = client_config.max_expression_depth;
	options.extensions = &DBConfig::GetConfig(*this).parser_extensions;
	return options;
}

ClientProperties ClientContext::GetClientProperties() const {
	string timezone = "UTC";
	Value result;
	// 1) Check Set Variable
	auto &client_config = ClientConfig::GetConfig(*this);
	auto tz_config = client_config.set_variables.find("timezone");
	if (tz_config == client_config.set_variables.end()) {
		// 2) Check for Default Value
		auto default_value = db->config.extension_parameters.find("timezone");
		if (default_value != db->config.extension_parameters.end()) {
			timezone = default_value->second.default_value.GetValue<string>();
		}
	} else {
		timezone = tz_config->second.GetValue<string>();
	}
	return {timezone, db->config.options.arrow_offset_size};
}

bool ClientContext::ExecutionIsFinished() {
	if (!active_query || !active_query->executor) {
		return false;
	}
	return active_query->executor->ExecutionIsFinished();
}

void ClientContext::SetPbParameters(int sql_mode_input, std::string pb_file_input, unique_ptr<std::vector<string>> parameters) {
    sql_mode = sql_mode_input;
    pb_file = pb_file_input;
    paras = move(parameters);
}

unique_ptr<LogicalGet> getLogicalGet(ClientContext& context, TableCatalogEntry& table, string& alias, idx_t table_index, vector<LogicalType>& table_types) {
    auto &catalog = Catalog::GetSystemCatalog(context);
    unique_ptr<FunctionData> bind_data;
    auto scan_function = table.GetScanFunction(context, bind_data);
    vector<string> table_names;
    vector<TableColumnType> table_categories;
    vector<string> column_name_alias;

    vector<LogicalType> return_types;
    vector<string> return_names;
    for (auto &col : table.GetColumns().Logical()) {
        table_types.push_back(col.Type());
        table_names.push_back(col.Name());
        return_types.push_back(col.Type());
        return_names.push_back(col.Name());
    }
    table_names = BindContext::AliasColumnNames(alias, table_names, column_name_alias);

    auto logical_get = make_uniq<LogicalGet>(table_index, scan_function, std::move(bind_data),
                                             std::move(return_types), std::move(return_names));


   // context.AddBaseTable(table_index, alias, table_names, table_types, logical_get->column_ids,
    //                          logical_get->GetTable().get());

    return logical_get;
}

unique_ptr<PhysicalOperator> ClientContext::GenerateIC11Plan() {
    vector<idx_t> left_projection_map, right_projection_map;

    string table_vertex_person = "person";
    string table_edge_knows = "knows";
    string table_vertex_place = "place";
    idx_t table_index_person1 = 6;
    idx_t table_index_person2 = 8;
    idx_t table_index_knows = 7;
    idx_t table_index_place = 11;


    auto table_or_view_person = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                  table_vertex_person, OnEntryNotFound::RETURN_NULL);
    auto &table_person = table_or_view_person->Cast<TableCatalogEntry>();


    auto table_or_view_knows = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_edge_knows, OnEntryNotFound::RETURN_NULL);
    auto &table_knows = table_or_view_knows->Cast<TableCatalogEntry>();

    auto table_or_view_place = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                        table_vertex_place, OnEntryNotFound::RETURN_NULL);
    auto &table_place = table_or_view_place->Cast<TableCatalogEntry>();

    string p_person_first_name = paras->data()[1];
    Value p_first_name(p_person_first_name);
    vector<idx_t> person2_ids{1, COLUMN_IDENTIFIER_ROW_ID, 9, 0, 2, 4, 5, 3, 7, 6};
    vector<LogicalType> get_person2_types{LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT,
                                          LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::DATE, LogicalType::BIGINT,
                                          LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
    string alias_person2 = "p2";
    vector<LogicalType> table_types_person2;
    vector<unique_ptr<Expression>> filter_person2;
    unique_ptr<LogicalGet> get_op_person2 = move(
            getLogicalGet(*this, table_person, alias_person2, table_index_person2, table_types_person2));
    unique_ptr<TableFilterSet> table_filters_person2 = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter_person2 = duckdb::make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL,
                                                                                           p_first_name);
    table_filters_person2->filters[0] = move(constant_filter_person2);
    unique_ptr<PhysicalTableScan> scan_person2 = make_uniq<PhysicalTableScan>(get_person2_types,
                                                                              get_op_person2->function,
                                                                              get_op_person2->table_index,
                                                                              move(get_op_person2->bind_data),
                                                                              table_types_person2, person2_ids,
                                                                              move(filter_person2), vector<column_t>(),
                                                                              get_op_person2->names,
                                                                              std::move(table_filters_person2),
                                                                              get_op_person2->estimated_cardinality,
                                                                              get_op_person2->extra_info);

    idx_t p_person_id = atoll(paras->data()[0].c_str()); // 933;
    Value p_person = Value::BIGINT(p_person_id);
    vector<idx_t> person1_ids{0, COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person1_types{LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_person1 = "p1";
    vector<LogicalType> table_types_person1;
    unique_ptr<LogicalGet> get_op_person1 = move(
            getLogicalGet(*this, table_person, alias_person1, table_index_person1, table_types_person1));
    vector<unique_ptr<Expression>> filter_person1;
    unique_ptr<TableFilterSet> table_filters_person1 = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter = duckdb::make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL,
                                                                                   p_person);
    table_filters_person1->filters[0] = move(constant_filter);
    unique_ptr<PhysicalTableScan> scan_person1 = make_uniq<PhysicalTableScan>(get_person1_types,
                                                                              get_op_person1->function,
                                                                              get_op_person1->table_index,
                                                                              move(get_op_person1->bind_data),
                                                                              table_types_person1, person1_ids,
                                                                              move(filter_person1), vector<column_t>(),
                                                                              get_op_person1->names,
                                                                              std::move(table_filters_person1),
                                                                              get_op_person1->estimated_cardinality,
                                                                              get_op_person1->extra_info);

    vector<JoinCondition> cond_knows;
    JoinCondition join_condition_knows;
    join_condition_knows.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 1);
    join_condition_knows.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 1);
    join_condition_knows.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows = make_uniq<RAIInfo>();
    rai_info_knows->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows->forward = true;
    rai_info_knows->vertex = &table_person;
    rai_info_knows->vertex_id = table_index_person2;
    rai_info_knows->passing_tables[0] = table_index_person2;
    rai_info_knows->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows.rais.push_back(move(rai_info_knows));
    cond_knows.push_back(move(join_condition_knows));

    LogicalComparisonJoin join_knows_op(JoinType::INNER);
    vector<LogicalType> output_knows_types{LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT,
                                           LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::DATE,
                                           LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                           LogicalType::VARCHAR, LogicalType::BIGINT};
    join_knows_op.types = output_knows_types;
    vector<idx_t> right_projection_map_knows{1};
    vector<idx_t> merge_project_map;
    vector<LogicalType> delim_types;
    auto join_knows = make_uniq<PhysicalMergeSIPJoin>(join_knows_op, move(scan_person2), move(scan_person1),
                                                      move(cond_knows),
                                                      JoinType::INNER, left_projection_map, right_projection_map_knows,
                                                      merge_project_map, delim_types, 0);

    // join place with person-person
    vector<idx_t> place_ids{COLUMN_IDENTIFIER_ROW_ID, 1};
    vector<LogicalType> get_place_types{LogicalType::BIGINT, LogicalType::VARCHAR};
    string alias_place = "pl";
    vector<LogicalType> table_types_place;
    vector<unique_ptr<Expression>> filter_place;
    unique_ptr<LogicalGet> get_op_place = move(
            getLogicalGet(*this, table_place, alias_place, table_index_place, table_types_place));
    unique_ptr<TableFilterSet> table_filters_place = NULL;
    unique_ptr<PhysicalTableScan> scan_place = make_uniq<PhysicalTableScan>(get_place_types, get_op_place->function,
                                                                           get_op_place->table_index,
                                                                           move(get_op_place->bind_data),
                                                                           table_types_place, place_ids,
                                                                           move(filter_place), vector<column_t>(),
                                                                           get_op_place->names,
                                                                           std::move(table_filters_place),
                                                                           get_op_place->estimated_cardinality,
                                                                           get_op_place->extra_info);

    vector<JoinCondition> cond_place;
    JoinCondition join_condition_place;
    join_condition_place.left = make_uniq<BoundReferenceExpression>("place_rowid", LogicalType::BIGINT, 0);
    join_condition_place.right = make_uniq<BoundReferenceExpression>("p_placeid_rowid", LogicalType::BIGINT, 2);
    join_condition_place.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_place = make_uniq<RAIInfo>();
    rai_info_place->rai = table_person.GetStorage().info->rais[0].get();
    rai_info_place->rai_type = RAIType::TARGET_EDGE;
    rai_info_place->forward = true;
    rai_info_place->vertex = &table_place;
    rai_info_place->vertex_id = table_index_place;
    rai_info_place->passing_tables[0] = table_index_place;
    rai_info_place->left_cardinalities[0] = table_place.GetStorage().info->cardinality;
    // rai_info_place->compact_list = &rai_info_place->rai->alist->compact_forward_list;

    join_condition_place.rais.push_back(move(rai_info_place));
    cond_place.push_back(move(join_condition_place));

    LogicalComparisonJoin join_place_op(JoinType::INNER);
    vector<LogicalType> output_place_types{LogicalType::BIGINT, LogicalType::VARCHAR,
                                           LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::DATE,
                                           LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                           LogicalType::VARCHAR};
    join_place_op.types = output_place_types;
    vector<idx_t> right_projection_map_place{3, 4, 5, 6, 7, 8, 9};
    vector<idx_t> merge_project_map_place;
    vector<LogicalType> delim_types_place;
    auto join_place = make_uniq<PhysicalSIPJoin>(join_place_op, move(scan_place), move(join_knows), move(cond_place),
                                                JoinType::INNER, left_projection_map, right_projection_map_place,
                                                delim_types_place, 0);


    // project
    vector<LogicalType> result_types{LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::DATE,
                                     LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                     LogicalType::VARCHAR, LogicalType::VARCHAR};
    vector<unique_ptr<Expression>> select_list;
    auto result_col0 = make_uniq<BoundReferenceExpression>("p_personid", LogicalType::BIGINT, 2);
    auto result_col1 = make_uniq<BoundReferenceExpression>("p_lastname", LogicalType::VARCHAR, 3);
    auto result_col2 = make_uniq<BoundReferenceExpression>("p_birthday", LogicalType::DATE, 4);
    auto result_col3 = make_uniq<BoundReferenceExpression>("p_creationdate", LogicalType::BIGINT, 5);
    auto result_col4 = make_uniq<BoundReferenceExpression>("p_gender", LogicalType::VARCHAR, 6);
    auto result_col5 = make_uniq<BoundReferenceExpression>("p_browserused", LogicalType::VARCHAR, 7);
    auto result_col6 = make_uniq<BoundReferenceExpression>("p_locationip", LogicalType::VARCHAR, 8);
    auto result_col7 = make_uniq<BoundReferenceExpression>("pl_name", LogicalType::VARCHAR, 1);

    select_list.push_back(move(result_col0));
    select_list.push_back(move(result_col1));
    select_list.push_back(move(result_col2));
    select_list.push_back(move(result_col3));
    select_list.push_back(move(result_col4));
    select_list.push_back(move(result_col5));
    select_list.push_back(move(result_col6));
    select_list.push_back(move(result_col7));

    auto projection = make_uniq<PhysicalProjection>(result_types, move(select_list), 0);
    projection->children.push_back(move(join_place));

    return projection;
}

unique_ptr<PhysicalOperator> ClientContext::GenerateIC12Plan() {
    vector<idx_t> left_projection_map, right_projection_map;

    string table_vertex_person = "person";
    string table_edge_knows = "knows";
    string table_vertex_place = "place";
    idx_t table_index_person1 = 6;
    idx_t table_index_person2 = 8;
    idx_t table_index_person_tmp = 10;
    idx_t table_index_knows = 7;
    idx_t table_index_knows2 = 9;
    idx_t table_index_place = 11;


    auto table_or_view_person = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                  table_vertex_person, OnEntryNotFound::RETURN_NULL);
    auto &table_person = table_or_view_person->Cast<TableCatalogEntry>();

    auto table_or_view_knows = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_edge_knows, OnEntryNotFound::RETURN_NULL);
    auto &table_knows = table_or_view_knows->Cast<TableCatalogEntry>();

    auto table_or_view_place = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_vertex_place, OnEntryNotFound::RETURN_NULL);
    auto &table_place = table_or_view_place->Cast<TableCatalogEntry>();


    idx_t p_person_id = atoll(paras->data()[0].c_str()); // 933;
    Value p_person = Value::BIGINT(p_person_id);
    vector<idx_t> person1_ids{0, COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person1_types{LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_person1 = "p1";
    vector<LogicalType> table_types_person1;
    unique_ptr<LogicalGet> get_op_person1 = move(
            getLogicalGet(*this, table_person, alias_person1, table_index_person1, table_types_person1));
    vector<unique_ptr<Expression>> filter_person1;
    unique_ptr<TableFilterSet> table_filters_person1 = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter = duckdb::make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL,
                                                                                   p_person);
    table_filters_person1->filters[0] = move(constant_filter);
    unique_ptr<PhysicalTableScan> scan_person1 = make_uniq<PhysicalTableScan>(get_person1_types,
                                                                              get_op_person1->function,
                                                                              get_op_person1->table_index,
                                                                              move(get_op_person1->bind_data),
                                                                              table_types_person1, person1_ids,
                                                                              move(filter_person1), vector<column_t>(),
                                                                              get_op_person1->names,
                                                                              std::move(table_filters_person1),
                                                                              get_op_person1->estimated_cardinality,
                                                                              get_op_person1->extra_info);

    vector<idx_t> person_tmp_ids{COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person_tmp_types{LogicalType::BIGINT};
    string alias_person_tmp = "ptmp";
    vector<LogicalType> table_types_person_tmp;
    unique_ptr<LogicalGet> get_op_person_tmp = move(
            getLogicalGet(*this, table_person, alias_person_tmp, table_index_person_tmp, table_types_person_tmp));
    vector<unique_ptr<Expression>> filter_person_tmp;
    unique_ptr<TableFilterSet> table_filters_person_tmp = NULL;
    unique_ptr<PhysicalTableScan> scan_person_tmp = make_uniq<PhysicalTableScan>(get_person_tmp_types,
                                                                              get_op_person_tmp->function,
                                                                              get_op_person_tmp->table_index,
                                                                              move(get_op_person_tmp->bind_data),
                                                                              table_types_person_tmp, person_tmp_ids,
                                                                              move(filter_person_tmp), vector<column_t>(),
                                                                              get_op_person_tmp->names,
                                                                              std::move(table_filters_person_tmp),
                                                                              get_op_person_tmp->estimated_cardinality,
                                                                              get_op_person_tmp->extra_info);

    vector<JoinCondition> cond_knows;
    JoinCondition join_condition_knows;
    join_condition_knows.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 1);
    join_condition_knows.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows = make_uniq<RAIInfo>();
    rai_info_knows->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows->forward = true;
    rai_info_knows->vertex = &table_person;
    rai_info_knows->vertex_id = table_index_person_tmp;
    rai_info_knows->passing_tables[0] = table_index_person_tmp;
    rai_info_knows->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows.rais.push_back(move(rai_info_knows));
    cond_knows.push_back(move(join_condition_knows));

    LogicalComparisonJoin join_knows_op(JoinType::INNER);
    vector<LogicalType> output_knows_types{LogicalType::BIGINT, LogicalType::BIGINT};
    join_knows_op.types = output_knows_types;
    vector<idx_t> right_projection_map_knows{1};
    vector<idx_t> merge_project_map;
    vector<LogicalType> delim_types;
    auto join_knows = make_uniq<PhysicalMergeSIPJoin>(join_knows_op, move(scan_person_tmp), move(scan_person1),
                                                      move(cond_knows),
                                                      JoinType::INNER, left_projection_map, right_projection_map_knows,
                                                      merge_project_map, delim_types, 0);


    // join person2 with person-person_tmp

    string p_person_first_name = paras->data()[1];
    Value p_first_name(p_person_first_name);
    vector<idx_t> person2_ids{1, COLUMN_IDENTIFIER_ROW_ID, 9, 0, 2, 4, 5, 3, 7, 6};
    vector<LogicalType> get_person2_types{LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT,
                                          LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::DATE,
                                          LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                          LogicalType::VARCHAR};
    string alias_person2 = "p2";
    vector<LogicalType> table_types_person2;
    vector<unique_ptr<Expression>> filter_person2;
    unique_ptr<LogicalGet> get_op_person2 = move(
            getLogicalGet(*this, table_person, alias_person2, table_index_person2, table_types_person2));
    unique_ptr<TableFilterSet> table_filters_person2 = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter_person2 = duckdb::make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_EQUAL,
            p_first_name);
    table_filters_person2->filters[0] = move(constant_filter_person2);
    unique_ptr<PhysicalTableScan> scan_person2 = make_uniq<PhysicalTableScan>(get_person2_types,
                                                                              get_op_person2->function,
                                                                              get_op_person2->table_index,
                                                                              move(get_op_person2->bind_data),
                                                                              table_types_person2, person2_ids,
                                                                              move(filter_person2), vector<column_t>(),
                                                                              get_op_person2->names,
                                                                              std::move(table_filters_person2),
                                                                              get_op_person2->estimated_cardinality,
                                                                              get_op_person2->extra_info);

    vector<JoinCondition> cond_knows_2;
    JoinCondition join_condition_knows_2;
    join_condition_knows_2.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 1);
    join_condition_knows_2.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows_2.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows_2 = make_uniq<RAIInfo>();
    rai_info_knows_2->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows_2->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows_2->forward = true;
    rai_info_knows_2->vertex = &table_person;
    rai_info_knows_2->vertex_id = table_index_person2;
    rai_info_knows_2->passing_tables[0] = table_index_person2;
    rai_info_knows_2->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows_2.rais.push_back(move(rai_info_knows_2));
    cond_knows_2.push_back(move(join_condition_knows_2));

    LogicalComparisonJoin join_knows_2_op(JoinType::INNER);
    vector<LogicalType> output_knows_2_types{LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT,
                                             LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::DATE,
                                             LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                             LogicalType::VARCHAR, LogicalType::BIGINT};
    join_knows_2_op.types = output_knows_2_types;
    vector<idx_t> right_projection_map_knows_2{0};
    vector<idx_t> merge_project_map_2;
    vector<LogicalType> delim_types_2;
    auto join_knows_2 = make_uniq<PhysicalMergeSIPJoin>(join_knows_2_op, move(scan_person2), move(join_knows),
                                                      move(cond_knows_2),
                                                      JoinType::INNER, left_projection_map, right_projection_map_knows_2,
                                                      merge_project_map_2, delim_types_2, 0);


    // join place with person-person
    vector<idx_t> place_ids{COLUMN_IDENTIFIER_ROW_ID, 1};
    vector<LogicalType> get_place_types{LogicalType::BIGINT, LogicalType::VARCHAR};
    string alias_place = "pl";
    vector<LogicalType> table_types_place;
    vector<unique_ptr<Expression>> filter_place;
    unique_ptr<LogicalGet> get_op_place = move(
            getLogicalGet(*this, table_place, alias_place, table_index_place, table_types_place));
    unique_ptr<TableFilterSet> table_filters_place = NULL;
    unique_ptr<PhysicalTableScan> scan_place = make_uniq<PhysicalTableScan>(get_place_types, get_op_place->function,
                                                                            get_op_place->table_index,
                                                                            move(get_op_place->bind_data),
                                                                            table_types_place, place_ids,
                                                                            move(filter_place), vector<column_t>(),
                                                                            get_op_place->names,
                                                                            std::move(table_filters_place),
                                                                            get_op_place->estimated_cardinality,
                                                                            get_op_place->extra_info);

    vector<JoinCondition> cond_place;
    JoinCondition join_condition_place;
    join_condition_place.left = make_uniq<BoundReferenceExpression>("place_rowid", LogicalType::BIGINT, 0);
    join_condition_place.right = make_uniq<BoundReferenceExpression>("p_placeid_rowid", LogicalType::BIGINT, 2);
    join_condition_place.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_place = make_uniq<RAIInfo>();
    rai_info_place->rai = table_person.GetStorage().info->rais[0].get();
    rai_info_place->rai_type = RAIType::TARGET_EDGE;
    rai_info_place->forward = true;
    rai_info_place->vertex = &table_place;
    rai_info_place->vertex_id = table_index_place;
    rai_info_place->passing_tables[0] = table_index_place;
    rai_info_place->left_cardinalities[0] = table_place.GetStorage().info->cardinality;
    // rai_info_place->compact_list = &rai_info_place->rai->alist->compact_forward_list;

    join_condition_place.rais.push_back(move(rai_info_place));
    cond_place.push_back(move(join_condition_place));

    LogicalComparisonJoin join_place_op(JoinType::INNER);
    vector<LogicalType> output_place_types{LogicalType::BIGINT, LogicalType::VARCHAR,
                                           LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::DATE,
                                           LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                           LogicalType::VARCHAR};
    join_place_op.types = output_place_types;
    vector<idx_t> right_projection_map_place{3, 4, 5, 6, 7, 8, 9};
    vector<idx_t> merge_project_map_place;
    vector<LogicalType> delim_types_place;
    auto join_place = make_uniq<PhysicalSIPJoin>(join_place_op, move(scan_place), move(join_knows_2), move(cond_place),
                                                 JoinType::INNER, left_projection_map, right_projection_map_place,
                                                 delim_types_place, 0);


    // project
    vector<LogicalType> result_types{LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::DATE,
                                     LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                     LogicalType::VARCHAR, LogicalType::VARCHAR};
    vector<unique_ptr<Expression>> select_list;
    auto result_col0 = make_uniq<BoundReferenceExpression>("p_personid", LogicalType::BIGINT, 2);
    auto result_col1 = make_uniq<BoundReferenceExpression>("p_lastname", LogicalType::VARCHAR, 3);
    auto result_col2 = make_uniq<BoundReferenceExpression>("p_birthday", LogicalType::DATE, 4);
    auto result_col3 = make_uniq<BoundReferenceExpression>("p_creationdate", LogicalType::BIGINT, 5);
    auto result_col4 = make_uniq<BoundReferenceExpression>("p_gender", LogicalType::VARCHAR, 6);
    auto result_col5 = make_uniq<BoundReferenceExpression>("p_browserused", LogicalType::VARCHAR, 7);
    auto result_col6 = make_uniq<BoundReferenceExpression>("p_locationip", LogicalType::VARCHAR, 8);
    auto result_col7 = make_uniq<BoundReferenceExpression>("pl_name", LogicalType::VARCHAR, 1);

    select_list.push_back(move(result_col0));
    select_list.push_back(move(result_col1));
    select_list.push_back(move(result_col2));
    select_list.push_back(move(result_col3));
    select_list.push_back(move(result_col4));
    select_list.push_back(move(result_col5));
    select_list.push_back(move(result_col6));
    select_list.push_back(move(result_col7));

    auto projection = make_uniq<PhysicalProjection>(result_types, move(select_list), 0);
    projection->children.push_back(move(join_place));

    return projection;
}

unique_ptr<PhysicalOperator> ClientContext::GenerateIC13Plan() {
    vector<idx_t> left_projection_map, right_projection_map;

    string table_vertex_person = "person";
    string table_edge_knows = "knows";
    string table_vertex_place = "place";
    idx_t table_index_person1 = 6;
    idx_t table_index_person2 = 8;
    idx_t table_index_person_tmp = 10;
    idx_t table_index_person_tmp2 = 12;
    idx_t table_index_knows = 7;
    idx_t table_index_knows2 = 9;
    idx_t table_index_knows3 = 11;
    idx_t table_index_place = 13;


    auto table_or_view_person = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                  table_vertex_person, OnEntryNotFound::RETURN_NULL);
    auto &table_person = table_or_view_person->Cast<TableCatalogEntry>();

    auto table_or_view_knows = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_edge_knows, OnEntryNotFound::RETURN_NULL);
    auto &table_knows = table_or_view_knows->Cast<TableCatalogEntry>();

    auto table_or_view_place = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_vertex_place, OnEntryNotFound::RETURN_NULL);
    auto &table_place = table_or_view_place->Cast<TableCatalogEntry>();


    idx_t p_person_id = atoll(paras->data()[0].c_str()); // 933;
    Value p_person = Value::BIGINT(p_person_id);
    vector<idx_t> person1_ids{0, COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person1_types{LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_person1 = "p1";
    vector<LogicalType> table_types_person1;
    unique_ptr<LogicalGet> get_op_person1 = move(
            getLogicalGet(*this, table_person, alias_person1, table_index_person1, table_types_person1));
    vector<unique_ptr<Expression>> filter_person1;
    unique_ptr<TableFilterSet> table_filters_person1 = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter = duckdb::make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL,
                                                                                   p_person);
    table_filters_person1->filters[0] = move(constant_filter);
    unique_ptr<PhysicalTableScan> scan_person1 = make_uniq<PhysicalTableScan>(get_person1_types,
                                                                              get_op_person1->function,
                                                                              get_op_person1->table_index,
                                                                              move(get_op_person1->bind_data),
                                                                              table_types_person1, person1_ids,
                                                                              move(filter_person1), vector<column_t>(),
                                                                              get_op_person1->names,
                                                                              std::move(table_filters_person1),
                                                                              get_op_person1->estimated_cardinality,
                                                                              get_op_person1->extra_info);

    vector<idx_t> person_tmp_ids{COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person_tmp_types{LogicalType::BIGINT};
    string alias_person_tmp = "ptmp";
    vector<LogicalType> table_types_person_tmp;
    unique_ptr<LogicalGet> get_op_person_tmp = move(
            getLogicalGet(*this, table_person, alias_person_tmp, table_index_person_tmp, table_types_person_tmp));
    vector<unique_ptr<Expression>> filter_person_tmp;
    unique_ptr<TableFilterSet> table_filters_person_tmp = NULL;
    unique_ptr<PhysicalTableScan> scan_person_tmp = make_uniq<PhysicalTableScan>(get_person_tmp_types,
                                                                                 get_op_person_tmp->function,
                                                                                 get_op_person_tmp->table_index,
                                                                                 move(get_op_person_tmp->bind_data),
                                                                                 table_types_person_tmp, person_tmp_ids,
                                                                                 move(filter_person_tmp),
                                                                                 vector<column_t>(),
                                                                                 get_op_person_tmp->names,
                                                                                 std::move(table_filters_person_tmp),
                                                                                 get_op_person_tmp->estimated_cardinality,
                                                                                 get_op_person_tmp->extra_info);

    vector<JoinCondition> cond_knows;
    JoinCondition join_condition_knows;
    join_condition_knows.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 1);
    join_condition_knows.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows = make_uniq<RAIInfo>();
    rai_info_knows->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows->forward = true;
    rai_info_knows->vertex = &table_person;
    rai_info_knows->vertex_id = table_index_person_tmp;
    rai_info_knows->passing_tables[0] = table_index_person_tmp;
    rai_info_knows->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows.rais.push_back(move(rai_info_knows));
    cond_knows.push_back(move(join_condition_knows));

    LogicalComparisonJoin join_knows_op(JoinType::INNER);
    vector<LogicalType> output_knows_types{LogicalType::BIGINT, LogicalType::BIGINT};
    join_knows_op.types = output_knows_types;
    vector<idx_t> right_projection_map_knows{1};
    vector<idx_t> merge_project_map;
    vector<LogicalType> delim_types;
    auto join_knows = make_uniq<PhysicalMergeSIPJoin>(join_knows_op, move(scan_person_tmp), move(scan_person1),
                                                      move(cond_knows),
                                                      JoinType::INNER, left_projection_map, right_projection_map_knows,
                                                      merge_project_map, delim_types, 0);

    // join person_tmp2 with person-person_tmp
    vector<idx_t> person_tmp2_ids{COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person_tmp2_types{LogicalType::BIGINT};
    string alias_person_tmp2 = "ptmp2";
    vector<LogicalType> table_types_person_tmp2;
    unique_ptr<LogicalGet> get_op_person_tmp2 = move(
            getLogicalGet(*this, table_person, alias_person_tmp2, table_index_person_tmp2, table_types_person_tmp2));
    vector<unique_ptr<Expression>> filter_person_tmp2;
    unique_ptr<TableFilterSet> table_filters_person_tmp2 = NULL;
    unique_ptr<PhysicalTableScan> scan_person_tmp2 = make_uniq<PhysicalTableScan>(get_person_tmp2_types,
                                                                                 get_op_person_tmp2->function,
                                                                                 get_op_person_tmp2->table_index,
                                                                                 move(get_op_person_tmp2->bind_data),
                                                                                 table_types_person_tmp2, person_tmp2_ids,
                                                                                 move(filter_person_tmp2),
                                                                                 vector<column_t>(),
                                                                                 get_op_person_tmp2->names,
                                                                                 std::move(table_filters_person_tmp2),
                                                                                 get_op_person_tmp2->estimated_cardinality,
                                                                                 get_op_person_tmp2->extra_info);

    vector<JoinCondition> cond_knows_2;
    JoinCondition join_condition_knows_2;
    join_condition_knows_2.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows_2.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows_2.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows_2 = make_uniq<RAIInfo>();
    rai_info_knows_2->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows_2->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows_2->forward = true;
    rai_info_knows_2->vertex = &table_person;
    rai_info_knows_2->vertex_id = table_index_person_tmp2;
    rai_info_knows_2->passing_tables[0] = table_index_person_tmp2;
    rai_info_knows_2->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows_2.rais.push_back(move(rai_info_knows_2));
    cond_knows_2.push_back(move(join_condition_knows_2));

    LogicalComparisonJoin join_knows_2_op(JoinType::INNER);
    vector<LogicalType> output_knows_2_types{LogicalType::BIGINT, LogicalType::BIGINT};
    join_knows_2_op.types = output_knows_2_types;
    vector<idx_t> right_projection_map_knows_2{0};
    vector<idx_t> merge_project_map_2;
    vector<LogicalType> delim_types_2;
    auto join_knows_2 = make_uniq<PhysicalMergeSIPJoin>(join_knows_2_op, move(scan_person_tmp2), move(join_knows),
                                                      move(cond_knows_2),
                                                      JoinType::INNER, left_projection_map, right_projection_map_knows_2,
                                                      merge_project_map_2, delim_types_2, 0);

    // join person2 with person-person_tmp-person_tmp2
    string p_person_first_name = paras->data()[1];
    Value p_first_name(p_person_first_name);
    vector<idx_t> person2_ids{1, COLUMN_IDENTIFIER_ROW_ID, 9, 0, 2, 4, 5, 3, 7, 6};
    vector<LogicalType> get_person2_types{LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT,
                                          LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::DATE,
                                          LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                          LogicalType::VARCHAR};
    string alias_person2 = "p2";
    vector<LogicalType> table_types_person2;
    vector<unique_ptr<Expression>> filter_person2;
    unique_ptr<LogicalGet> get_op_person2 = move(
            getLogicalGet(*this, table_person, alias_person2, table_index_person2, table_types_person2));
    unique_ptr<TableFilterSet> table_filters_person2 = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter_person2 = duckdb::make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_EQUAL,
            p_first_name);
    table_filters_person2->filters[0] = move(constant_filter_person2);
    unique_ptr<PhysicalTableScan> scan_person2 = make_uniq<PhysicalTableScan>(get_person2_types,
                                                                              get_op_person2->function,
                                                                              get_op_person2->table_index,
                                                                              move(get_op_person2->bind_data),
                                                                              table_types_person2, person2_ids,
                                                                              move(filter_person2), vector<column_t>(),
                                                                              get_op_person2->names,
                                                                              std::move(table_filters_person2),
                                                                              get_op_person2->estimated_cardinality,
                                                                              get_op_person2->extra_info);

    vector<JoinCondition> cond_knows_3;
    JoinCondition join_condition_knows_3;
    join_condition_knows_3.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 1);
    join_condition_knows_3.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows_3.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows_3 = make_uniq<RAIInfo>();
    rai_info_knows_3->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows_3->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows_3->forward = true;
    rai_info_knows_3->vertex = &table_person;
    rai_info_knows_3->vertex_id = table_index_person2;
    rai_info_knows_3->passing_tables[0] = table_index_person2;
    rai_info_knows_3->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows_3.rais.push_back(move(rai_info_knows_3));
    cond_knows_3.push_back(move(join_condition_knows_3));

    LogicalComparisonJoin join_knows_3_op(JoinType::INNER);
    vector<LogicalType> output_knows_3_types{LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT,
                                             LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::DATE,
                                             LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                             LogicalType::VARCHAR, LogicalType::BIGINT};
    join_knows_3_op.types = output_knows_3_types;
    vector<idx_t> right_projection_map_knows_3{0};
    vector<idx_t> merge_project_map_3;
    vector<LogicalType> delim_types_3;
    auto join_knows_3 = make_uniq<PhysicalMergeSIPJoin>(join_knows_3_op, move(scan_person2), move(join_knows_2),
                                                        move(cond_knows_3),
                                                        JoinType::INNER, left_projection_map,
                                                        right_projection_map_knows_3,
                                                        merge_project_map_3, delim_types_3, 0);


    // join place with person-person
    vector<idx_t> place_ids{COLUMN_IDENTIFIER_ROW_ID, 1};
    vector<LogicalType> get_place_types{LogicalType::BIGINT, LogicalType::VARCHAR};
    string alias_place = "pl";
    vector<LogicalType> table_types_place;
    vector<unique_ptr<Expression>> filter_place;
    unique_ptr<LogicalGet> get_op_place = move(
            getLogicalGet(*this, table_place, alias_place, table_index_place, table_types_place));
    unique_ptr<TableFilterSet> table_filters_place = NULL;
    unique_ptr<PhysicalTableScan> scan_place = make_uniq<PhysicalTableScan>(get_place_types, get_op_place->function,
                                                                            get_op_place->table_index,
                                                                            move(get_op_place->bind_data),
                                                                            table_types_place, place_ids,
                                                                            move(filter_place), vector<column_t>(),
                                                                            get_op_place->names,
                                                                            std::move(table_filters_place),
                                                                            get_op_place->estimated_cardinality,
                                                                            get_op_place->extra_info);

    vector<JoinCondition> cond_place;
    JoinCondition join_condition_place;
    join_condition_place.left = make_uniq<BoundReferenceExpression>("place_rowid", LogicalType::BIGINT, 0);
    join_condition_place.right = make_uniq<BoundReferenceExpression>("p_placeid_rowid", LogicalType::BIGINT, 2);
    join_condition_place.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_place = make_uniq<RAIInfo>();
    rai_info_place->rai = table_person.GetStorage().info->rais[0].get();
    rai_info_place->rai_type = RAIType::TARGET_EDGE;
    rai_info_place->forward = true;
    rai_info_place->vertex = &table_place;
    rai_info_place->vertex_id = table_index_place;
    rai_info_place->passing_tables[0] = table_index_place;
    rai_info_place->left_cardinalities[0] = table_place.GetStorage().info->cardinality;
    // rai_info_place->compact_list = &rai_info_place->rai->alist->compact_forward_list;

    join_condition_place.rais.push_back(move(rai_info_place));
    cond_place.push_back(move(join_condition_place));

    LogicalComparisonJoin join_place_op(JoinType::INNER);
    vector<LogicalType> output_place_types{LogicalType::BIGINT, LogicalType::VARCHAR,
                                           LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::DATE,
                                           LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                           LogicalType::VARCHAR};
    join_place_op.types = output_place_types;
    vector<idx_t> right_projection_map_place{3, 4, 5, 6, 7, 8, 9};
    vector<idx_t> merge_project_map_place;
    vector<LogicalType> delim_types_place;
    auto join_place = make_uniq<PhysicalSIPJoin>(join_place_op, move(scan_place), move(join_knows_3), move(cond_place),
                                                 JoinType::INNER, left_projection_map, right_projection_map_place,
                                                 delim_types_place, 0);


    // project
    vector<LogicalType> result_types{LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::DATE,
                                     LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                     LogicalType::VARCHAR, LogicalType::VARCHAR};
    vector<unique_ptr<Expression>> select_list;
    auto result_col0 = make_uniq<BoundReferenceExpression>("p_personid", LogicalType::BIGINT, 2);
    auto result_col1 = make_uniq<BoundReferenceExpression>("p_lastname", LogicalType::VARCHAR, 3);
    auto result_col2 = make_uniq<BoundReferenceExpression>("p_birthday", LogicalType::DATE, 4);
    auto result_col3 = make_uniq<BoundReferenceExpression>("p_creationdate", LogicalType::BIGINT, 5);
    auto result_col4 = make_uniq<BoundReferenceExpression>("p_gender", LogicalType::VARCHAR, 6);
    auto result_col5 = make_uniq<BoundReferenceExpression>("p_browserused", LogicalType::VARCHAR, 7);
    auto result_col6 = make_uniq<BoundReferenceExpression>("p_locationip", LogicalType::VARCHAR, 8);
    auto result_col7 = make_uniq<BoundReferenceExpression>("pl_name", LogicalType::VARCHAR, 1);

    select_list.push_back(move(result_col0));
    select_list.push_back(move(result_col1));
    select_list.push_back(move(result_col2));
    select_list.push_back(move(result_col3));
    select_list.push_back(move(result_col4));
    select_list.push_back(move(result_col5));
    select_list.push_back(move(result_col6));
    select_list.push_back(move(result_col7));

    auto projection = make_uniq<PhysicalProjection>(result_types, move(select_list), 0);
    projection->children.push_back(move(join_place));

    return projection;
}

unique_ptr<PhysicalOperator> ClientContext::GenerateIC21Plan() {
    vector<idx_t> left_projection_map, right_projection_map;

    string table_vertex_person = "person";
    string table_edge_knows = "knows";
    string table_vertex_comment = "comment";
    idx_t table_index_person1 = 6;
    idx_t table_index_person2 = 8;
    idx_t table_index_knows = 7;
    idx_t table_index_comment = 11;


    auto table_or_view_person = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                  table_vertex_person, OnEntryNotFound::RETURN_NULL);
    auto &table_person = table_or_view_person->Cast<TableCatalogEntry>();


    auto table_or_view_knows = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_edge_knows, OnEntryNotFound::RETURN_NULL);
    auto &table_knows = table_or_view_knows->Cast<TableCatalogEntry>();

    auto table_or_view_comment = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_vertex_comment, OnEntryNotFound::RETURN_NULL);
    auto &table_comment = table_or_view_comment->Cast<TableCatalogEntry>();


    vector<idx_t> person2_ids{COLUMN_IDENTIFIER_ROW_ID, 0, 1, 2};
    vector<LogicalType> get_person2_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR,
                                          LogicalType::VARCHAR};
    string alias_person2 = "p2";
    vector<LogicalType> table_types_person2;
    vector<unique_ptr<Expression>> filter_person2;
    unique_ptr<LogicalGet> get_op_person2 = move(
            getLogicalGet(*this, table_person, alias_person2, table_index_person2, table_types_person2));
    unique_ptr<TableFilterSet> table_filters_person2 = NULL;
    unique_ptr<PhysicalTableScan> scan_person2 = make_uniq<PhysicalTableScan>(get_person2_types,
                                                                              get_op_person2->function,
                                                                              get_op_person2->table_index,
                                                                              move(get_op_person2->bind_data),
                                                                              table_types_person2, person2_ids,
                                                                              move(filter_person2), vector<column_t>(),
                                                                              get_op_person2->names,
                                                                              std::move(table_filters_person2),
                                                                              get_op_person2->estimated_cardinality,
                                                                              get_op_person2->extra_info);

    idx_t p_person_id = atoll(paras->data()[0].c_str()); // 933;
    Value p_person = Value::BIGINT(p_person_id);
    vector<idx_t> person1_ids{0, COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person1_types{LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_person1 = "p1";
    vector<LogicalType> table_types_person1;
    unique_ptr<LogicalGet> get_op_person1 = move(
            getLogicalGet(*this, table_person, alias_person1, table_index_person1, table_types_person1));
    vector<unique_ptr<Expression>> filter_person1;
    unique_ptr<TableFilterSet> table_filters_person1 = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter = duckdb::make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL,
                                                                                   p_person);
    table_filters_person1->filters[0] = move(constant_filter);
    unique_ptr<PhysicalTableScan> scan_person1 = make_uniq<PhysicalTableScan>(get_person1_types,
                                                                              get_op_person1->function,
                                                                              get_op_person1->table_index,
                                                                              move(get_op_person1->bind_data),
                                                                              table_types_person1, person1_ids,
                                                                              move(filter_person1), vector<column_t>(),
                                                                              get_op_person1->names,
                                                                              std::move(table_filters_person1),
                                                                              get_op_person1->estimated_cardinality,
                                                                              get_op_person1->extra_info);

    vector<JoinCondition> cond_knows;
    JoinCondition join_condition_knows;
    join_condition_knows.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 1);
    join_condition_knows.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows = make_uniq<RAIInfo>();
    rai_info_knows->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows->forward = true;
    rai_info_knows->vertex = &table_person;
    rai_info_knows->vertex_id = table_index_person2;
    rai_info_knows->passing_tables[0] = table_index_person2;
    rai_info_knows->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows.rais.push_back(move(rai_info_knows));
    cond_knows.push_back(move(join_condition_knows));

    LogicalComparisonJoin join_knows_op(JoinType::INNER);
    vector<LogicalType> output_knows_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR,
                                           LogicalType::VARCHAR, LogicalType::BIGINT};
    join_knows_op.types = output_knows_types;
    vector<idx_t> right_projection_map_knows{1};
    vector<idx_t> merge_project_map;
    vector<LogicalType> delim_types;
    auto join_knows = make_uniq<PhysicalMergeSIPJoin>(join_knows_op, move(scan_person2), move(scan_person1),
                                                      move(cond_knows),
                                                      JoinType::INNER, left_projection_map, right_projection_map_knows,
                                                      merge_project_map, delim_types, 0);

    // join comment with person-person
    idx_t p_comment_date = atoll(paras->data()[1].c_str());
    Value p_comment = Value::BIGINT(p_comment_date);
    vector<idx_t> comment_ids{10, 1, 0, 4};
    vector<LogicalType> get_comment_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                                          LogicalType::VARCHAR};
    string alias_comment = "c";
    vector<LogicalType> table_types_comment;
    vector<unique_ptr<Expression>> filter_comment;
    unique_ptr<LogicalGet> get_op_comment = move(
            getLogicalGet(*this, table_comment, alias_comment, table_index_comment, table_types_comment));
    unique_ptr<TableFilterSet> table_filters_comment = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter_comment = duckdb::make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHAN,
                                                                                   p_comment);
    table_filters_comment->filters[1] = move(constant_filter_comment);
    unique_ptr<PhysicalTableScan> scan_comment = make_uniq<PhysicalTableScan>(get_comment_types, get_op_comment->function,
                                                                            get_op_comment->table_index,
                                                                            move(get_op_comment->bind_data),
                                                                            table_types_comment, comment_ids,
                                                                            move(filter_comment), vector<column_t>(),
                                                                            get_op_comment->names,
                                                                            std::move(table_filters_comment),
                                                                            get_op_comment->estimated_cardinality,
                                                                            get_op_comment->extra_info);

    vector<JoinCondition> cond_comment;
    JoinCondition join_condition_comment;
    join_condition_comment.left = make_uniq<BoundReferenceExpression>("m_creatorid_rowid", LogicalType::BIGINT, 0);
    join_condition_comment.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_comment.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_comment = make_uniq<RAIInfo>();
    rai_info_comment->rai = table_comment.GetStorage().info->rais[0].get();
    rai_info_comment->rai_type = RAIType::EDGE_SOURCE;
    rai_info_comment->forward = true;
    rai_info_comment->vertex = &table_person;
    rai_info_comment->vertex_id = table_index_person2;
    rai_info_comment->passing_tables[0] = table_index_comment;
    rai_info_comment->left_cardinalities[0] = table_comment.GetStorage().info->cardinality;
    rai_info_comment->compact_list = &rai_info_comment->rai->alist->compact_forward_list;

    join_condition_comment.rais.push_back(move(rai_info_comment));
    cond_comment.push_back(move(join_condition_comment));

    LogicalComparisonJoin join_comment_op(JoinType::INNER);
    vector<LogicalType> output_comment_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                                             LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR,
                                             LogicalType::VARCHAR};
    join_comment_op.types = output_comment_types;
    vector<idx_t> right_projection_map_comment{1, 2, 3};
    vector<idx_t> merge_project_map_comment;
    vector<LogicalType> delim_types_comment;
    auto join_place = make_uniq<PhysicalSIPJoin>(join_comment_op, move(scan_comment), move(join_knows), move(cond_comment),
                                                 JoinType::INNER, left_projection_map, right_projection_map_comment,
                                                 delim_types_comment, 0);


    // project
    vector<LogicalType> result_types{LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                     LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::BIGINT};
    vector<unique_ptr<Expression>> select_list;
    auto result_col0 = make_uniq<BoundReferenceExpression>("p_personid", LogicalType::BIGINT, 4);
    auto result_col1 = make_uniq<BoundReferenceExpression>("p_firstname", LogicalType::VARCHAR, 5);
    auto result_col2 = make_uniq<BoundReferenceExpression>("p_lastname", LogicalType::VARCHAR, 6);
    auto result_col3 = make_uniq<BoundReferenceExpression>("m_messageid", LogicalType::BIGINT, 2);
    auto result_col4 = make_uniq<BoundReferenceExpression>("m_content", LogicalType::VARCHAR, 3);
    auto result_col5 = make_uniq<BoundReferenceExpression>("m_creationdate", LogicalType::BIGINT, 1);

    select_list.push_back(move(result_col0));
    select_list.push_back(move(result_col1));
    select_list.push_back(move(result_col2));
    select_list.push_back(move(result_col3));
    select_list.push_back(move(result_col4));
    select_list.push_back(move(result_col5));

    auto projection = make_uniq<PhysicalProjection>(result_types, move(select_list), 0);
    projection->children.push_back(move(join_place));

    return projection;
}

unique_ptr<PhysicalOperator> ClientContext::GenerateIC31Plan() {
    vector<idx_t> left_projection_map, right_projection_map;

    string table_vertex_person = "person";
    string table_edge_knows = "knows";
    string table_vertex_comment = "comment";
    string table_vertex_place = "place";
    idx_t table_index_person1 = 6;
    idx_t table_index_person2 = 8;
    idx_t table_index_knows = 7;
    idx_t table_index_comment1 = 11;
    idx_t table_index_comment2 = 12;
    idx_t table_index_place1 = 13;
    idx_t table_index_place2 = 14;


    auto table_or_view_person = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                  table_vertex_person, OnEntryNotFound::RETURN_NULL);
    auto &table_person = table_or_view_person->Cast<TableCatalogEntry>();


    auto table_or_view_knows = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_edge_knows, OnEntryNotFound::RETURN_NULL);
    auto &table_knows = table_or_view_knows->Cast<TableCatalogEntry>();

    auto table_or_view_comment = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                   table_vertex_comment, OnEntryNotFound::RETURN_NULL);
    auto &table_comment = table_or_view_comment->Cast<TableCatalogEntry>();

    auto table_or_view_place = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                   table_vertex_place, OnEntryNotFound::RETURN_NULL);
    auto &table_place = table_or_view_place->Cast<TableCatalogEntry>();


    vector<idx_t> person2_ids{COLUMN_IDENTIFIER_ROW_ID, 0, 1, 2};
    vector<LogicalType> get_person2_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR,
                                          LogicalType::VARCHAR};
    string alias_person2 = "p2";
    vector<LogicalType> table_types_person2;
    vector<unique_ptr<Expression>> filter_person2;
    unique_ptr<LogicalGet> get_op_person2 = move(
            getLogicalGet(*this, table_person, alias_person2, table_index_person2, table_types_person2));
    unique_ptr<TableFilterSet> table_filters_person2 = NULL;
    unique_ptr<PhysicalTableScan> scan_person2 = make_uniq<PhysicalTableScan>(get_person2_types,
                                                                              get_op_person2->function,
                                                                              get_op_person2->table_index,
                                                                              move(get_op_person2->bind_data),
                                                                              table_types_person2, person2_ids,
                                                                              move(filter_person2), vector<column_t>(),
                                                                              get_op_person2->names,
                                                                              std::move(table_filters_person2),
                                                                              get_op_person2->estimated_cardinality,
                                                                              get_op_person2->extra_info);

    idx_t p_person_id = atoll(paras->data()[0].c_str()); // 933;
    Value p_person = Value::BIGINT(p_person_id);
    vector<idx_t> person1_ids{0, COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person1_types{LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_person1 = "p1";
    vector<LogicalType> table_types_person1;
    unique_ptr<LogicalGet> get_op_person1 = move(
            getLogicalGet(*this, table_person, alias_person1, table_index_person1, table_types_person1));
    vector<unique_ptr<Expression>> filter_person1;
    unique_ptr<TableFilterSet> table_filters_person1 = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter = duckdb::make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL,
                                                                                   p_person);
    table_filters_person1->filters[0] = move(constant_filter);
    unique_ptr<PhysicalTableScan> scan_person1 = make_uniq<PhysicalTableScan>(get_person1_types,
                                                                              get_op_person1->function,
                                                                              get_op_person1->table_index,
                                                                              move(get_op_person1->bind_data),
                                                                              table_types_person1, person1_ids,
                                                                              move(filter_person1), vector<column_t>(),
                                                                              get_op_person1->names,
                                                                              std::move(table_filters_person1),
                                                                              get_op_person1->estimated_cardinality,
                                                                              get_op_person1->extra_info);

    vector<JoinCondition> cond_knows;
    JoinCondition join_condition_knows;
    join_condition_knows.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 1);
    join_condition_knows.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows = make_uniq<RAIInfo>();
    rai_info_knows->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows->forward = true;
    rai_info_knows->vertex = &table_person;
    rai_info_knows->vertex_id = table_index_person2;
    rai_info_knows->passing_tables[0] = table_index_person2;
    rai_info_knows->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows.rais.push_back(move(rai_info_knows));
    cond_knows.push_back(move(join_condition_knows));

    LogicalComparisonJoin join_knows_op(JoinType::INNER);
    vector<LogicalType> output_knows_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR,
                                           LogicalType::VARCHAR, LogicalType::BIGINT};
    join_knows_op.types = output_knows_types;
    vector<idx_t> right_projection_map_knows{1};
    vector<idx_t> merge_project_map;
    vector<LogicalType> delim_types;
    auto join_knows = make_uniq<PhysicalMergeSIPJoin>(join_knows_op, move(scan_person2), move(scan_person1),
                                                      move(cond_knows),
                                                      JoinType::INNER, left_projection_map, right_projection_map_knows,
                                                      merge_project_map, delim_types, 0);

    // join comment with person-person
    idx_t p_comment1_start = atoll(paras->data()[1].c_str());
    idx_t p_comment1_end = atoll(paras->data()[2].c_str());
    Value p_comment1_start_time = Value::BIGINT(p_comment1_start);
    Value p_comment1_end_time = Value::BIGINT(p_comment1_end);
    vector<idx_t> comment1_ids{10, 1, 11};
    vector<LogicalType> get_comment1_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_comment1 = "c1";
    vector<LogicalType> table_types_comment1;
    vector<unique_ptr<Expression>> filter_comment1;
    unique_ptr<LogicalGet> get_op_comment1 = move(
            getLogicalGet(*this, table_comment, alias_comment1, table_index_comment1, table_types_comment1));
    unique_ptr<TableFilterSet> table_filters_comment1 = make_uniq<TableFilterSet>();
    unique_ptr<ConjunctionAndFilter> and_filter_comment1 = duckdb::make_uniq<ConjunctionAndFilter>();
    unique_ptr<ConstantFilter> constant_filter_comment1_start = duckdb::make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_GREATERTHANOREQUALTO, p_comment1_start_time);
    unique_ptr<ConstantFilter> constant_filter_comment1_end = duckdb::make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_LESSTHAN, p_comment1_end_time);
    and_filter_comment1->child_filters.push_back(move(constant_filter_comment1_start));
    and_filter_comment1->child_filters.push_back(move(constant_filter_comment1_end));
    table_filters_comment1->filters[1] = move(and_filter_comment1);
    unique_ptr<PhysicalTableScan> scan_comment1 = make_uniq<PhysicalTableScan>(get_comment1_types,
                                                                              get_op_comment1->function,
                                                                              get_op_comment1->table_index,
                                                                              move(get_op_comment1->bind_data),
                                                                              table_types_comment1, comment1_ids,
                                                                              move(filter_comment1), vector<column_t>(),
                                                                              get_op_comment1->names,
                                                                              std::move(table_filters_comment1),
                                                                              get_op_comment1->estimated_cardinality,
                                                                              get_op_comment1->extra_info);

    vector<JoinCondition> cond_comment1;
    JoinCondition join_condition_comment1;
    join_condition_comment1.left = make_uniq<BoundReferenceExpression>("m_creatorid_rowid", LogicalType::BIGINT, 0);
    join_condition_comment1.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_comment1.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_comment = make_uniq<RAIInfo>();
    rai_info_comment->rai = table_comment.GetStorage().info->rais[0].get();
    rai_info_comment->rai_type = RAIType::EDGE_SOURCE;
    rai_info_comment->forward = true;
    rai_info_comment->vertex = &table_person;
    rai_info_comment->vertex_id = table_index_person2;
    rai_info_comment->passing_tables[0] = table_index_comment1;
    rai_info_comment->left_cardinalities[0] = table_comment.GetStorage().info->cardinality;
    rai_info_comment->compact_list = &rai_info_comment->rai->alist->compact_forward_list;

    join_condition_comment1.rais.push_back(move(rai_info_comment));
    cond_comment1.push_back(move(join_condition_comment1));

    LogicalComparisonJoin join_comment1_op(JoinType::INNER);
    vector<LogicalType> output_comment1_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                                             LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR,
                                             LogicalType::VARCHAR};
    join_comment1_op.types = output_comment1_types;
    vector<idx_t> right_projection_map_comment1{0, 1, 2, 3};
    vector<idx_t> merge_project_map_comment1;
    vector<LogicalType> delim_types_comment1;
    auto join_comment1 = make_uniq<PhysicalSIPJoin>(join_comment1_op, move(scan_comment1), move(join_knows),
                                                 move(cond_comment1),
                                                 JoinType::INNER, left_projection_map, right_projection_map_comment1,
                                                 delim_types_comment1, 0);

    // join place1 with person-person-comment
    string p_place1 = paras->data()[3];
    Value p_place1_name = Value(p_place1);
    vector<idx_t> place1_ids{COLUMN_IDENTIFIER_ROW_ID, 1};
    vector<LogicalType> get_place1_types{LogicalType::BIGINT, LogicalType::VARCHAR};
    string alias_place1 = "pl1";
    vector<LogicalType> table_types_place1;
    vector<unique_ptr<Expression>> filter_place1;
    unique_ptr<LogicalGet> get_op_place1 = move(
            getLogicalGet(*this, table_place, alias_place1, table_index_place1, table_types_place1));
    unique_ptr<TableFilterSet> table_filters_place1 = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter_place1_name = duckdb::make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_EQUAL, p_place1_name);
    table_filters_place1->filters[1] = move(constant_filter_place1_name);
    unique_ptr<PhysicalTableScan> scan_place1 = make_uniq<PhysicalTableScan>(get_place1_types, get_op_place1->function,
                                                                            get_op_place1->table_index,
                                                                            move(get_op_place1->bind_data),
                                                                            table_types_place1, place1_ids,
                                                                            move(filter_place1), vector<column_t>(),
                                                                            get_op_place1->names,
                                                                            std::move(table_filters_place1),
                                                                            get_op_place1->estimated_cardinality,
                                                                            get_op_place1->extra_info);

    vector<JoinCondition> cond_place1;
    JoinCondition join_condition_place1;
    join_condition_place1.left = make_uniq<BoundReferenceExpression>("place_rowid", LogicalType::BIGINT, 0);
    join_condition_place1.right = make_uniq<BoundReferenceExpression>("m_locationid_rowid", LogicalType::BIGINT, 2);
    join_condition_place1.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_place1 = make_uniq<RAIInfo>();
    rai_info_place1->rai = table_comment.GetStorage().info->rais[1].get();
    rai_info_place1->rai_type = RAIType::TARGET_EDGE;
    rai_info_place1->forward = true;
    rai_info_place1->vertex = &table_place;
    rai_info_place1->vertex_id = table_index_place1;
    rai_info_place1->passing_tables[0] = table_index_place1;
    rai_info_place1->left_cardinalities[0] = table_place.GetStorage().info->cardinality;
    // rai_info_place->compact_list = &rai_info_place->rai->alist->compact_forward_list;

    join_condition_place1.rais.push_back(move(rai_info_place1));
    cond_place1.push_back(move(join_condition_place1));

    LogicalComparisonJoin join_place1_op(JoinType::INNER);
    vector<LogicalType> output_place1_types{LogicalType::BIGINT, LogicalType::VARCHAR,
                                           LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR,
                                           LogicalType::VARCHAR};
    join_place1_op.types = output_place1_types;
    vector<idx_t> right_projection_map_place1{3, 4, 5, 6};
    vector<idx_t> merge_project_map_place1;
    vector<LogicalType> delim_types_place1;
    auto join_place1 = make_uniq<PhysicalSIPJoin>(join_place1_op, move(scan_place1), move(join_comment1), move(cond_place1),
                                                 JoinType::INNER, left_projection_map, right_projection_map_place1,
                                                 delim_types_place1, 0);

    // comment2 and place2
    idx_t p_comment2_start = atoll(paras->data()[1].c_str());
    idx_t p_comment2_end = atoll(paras->data()[2].c_str());
    Value p_comment2_start_time = Value::BIGINT(p_comment2_start);
    Value p_comment2_end_time = Value::BIGINT(p_comment2_end);
    vector<idx_t> comment2_ids{10, 1, 11};
    vector<LogicalType> get_comment2_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_comment2 = "c1";
    vector<LogicalType> table_types_comment2;
    vector<unique_ptr<Expression>> filter_comment2;
    unique_ptr<LogicalGet> get_op_comment2 = move(
            getLogicalGet(*this, table_comment, alias_comment2, table_index_comment2, table_types_comment2));
    unique_ptr<TableFilterSet> table_filters_comment2 = make_uniq<TableFilterSet>();
    unique_ptr<ConjunctionAndFilter> and_filter_comment2 = duckdb::make_uniq<ConjunctionAndFilter>();
    unique_ptr<ConstantFilter> constant_filter_comment2_start = duckdb::make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_GREATERTHANOREQUALTO, p_comment2_start_time);
    unique_ptr<ConstantFilter> constant_filter_comment2_end = duckdb::make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_LESSTHAN, p_comment2_end_time);
    and_filter_comment2->child_filters.push_back(move(constant_filter_comment2_start));
    and_filter_comment2->child_filters.push_back(move(constant_filter_comment2_end));
    table_filters_comment2->filters[1] = move(and_filter_comment2);
    unique_ptr<PhysicalTableScan> scan_comment2 = make_uniq<PhysicalTableScan>(get_comment2_types,
                                                                               get_op_comment2->function,
                                                                               get_op_comment2->table_index,
                                                                               move(get_op_comment2->bind_data),
                                                                               table_types_comment2, comment2_ids,
                                                                               move(filter_comment2), vector<column_t>(),
                                                                               get_op_comment2->names,
                                                                               std::move(table_filters_comment2),
                                                                               get_op_comment2->estimated_cardinality,
                                                                               get_op_comment2->extra_info);

    vector<JoinCondition> cond_comment2;
    JoinCondition join_condition_comment2;
    join_condition_comment2.left = make_uniq<BoundReferenceExpression>("m_creatorid_rowid", LogicalType::BIGINT, 0);
    join_condition_comment2.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 2);
    join_condition_comment2.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_comment2 = make_uniq<RAIInfo>();
    rai_info_comment2->rai = table_comment.GetStorage().info->rais[0].get();
    rai_info_comment2->rai_type = RAIType::EDGE_SOURCE;
    rai_info_comment2->forward = true;
    rai_info_comment2->vertex = &table_person;
    rai_info_comment2->vertex_id = table_index_person2;
    rai_info_comment2->passing_tables[0] = table_index_comment2;
    rai_info_comment2->left_cardinalities[0] = table_comment.GetStorage().info->cardinality;
    rai_info_comment2->compact_list = &rai_info_comment2->rai->alist->compact_forward_list;

    join_condition_comment2.rais.push_back(move(rai_info_comment2));
    cond_comment2.push_back(move(join_condition_comment2));

    LogicalComparisonJoin join_comment2_op(JoinType::INNER);
    vector<LogicalType> output_comment2_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                                              LogicalType::BIGINT, LogicalType::VARCHAR,
                                              LogicalType::VARCHAR};
    join_comment2_op.types = output_comment2_types;
    vector<idx_t> right_projection_map_comment2{3, 4, 5};
    vector<idx_t> merge_project_map_comment2;
    vector<LogicalType> delim_types_comment2;
    auto join_comment2 = make_uniq<PhysicalSIPJoin>(join_comment2_op, move(scan_comment2), move(join_place1),
                                                    move(cond_comment2),
                                                    JoinType::INNER, left_projection_map, right_projection_map_comment2,
                                                    delim_types_comment2, 0);

    // join place2 with person-person-comment
    string p_place2 = paras->data()[4];
    Value p_place2_name = Value(p_place2);
    vector<idx_t> place2_ids{COLUMN_IDENTIFIER_ROW_ID, 1};
    vector<LogicalType> get_place2_types{LogicalType::BIGINT, LogicalType::VARCHAR};
    string alias_place2 = "pl2";
    vector<LogicalType> table_types_place2;
    vector<unique_ptr<Expression>> filter_place2;
    unique_ptr<LogicalGet> get_op_place2 = move(
            getLogicalGet(*this, table_place, alias_place2, table_index_place2, table_types_place2));
    unique_ptr<TableFilterSet> table_filters_place2 = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter_place2_name = duckdb::make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_EQUAL, p_place2_name);
    table_filters_place2->filters[1] = move(constant_filter_place2_name);
    unique_ptr<PhysicalTableScan> scan_place2 = make_uniq<PhysicalTableScan>(get_place2_types, get_op_place2->function,
                                                                             get_op_place2->table_index,
                                                                             move(get_op_place2->bind_data),
                                                                             table_types_place2, place2_ids,
                                                                             move(filter_place2), vector<column_t>(),
                                                                             get_op_place2->names,
                                                                             std::move(table_filters_place2),
                                                                             get_op_place2->estimated_cardinality,
                                                                             get_op_place2->extra_info);

    vector<JoinCondition> cond_place2;
    JoinCondition join_condition_place2;
    join_condition_place2.left = make_uniq<BoundReferenceExpression>("place_rowid", LogicalType::BIGINT, 0);
    join_condition_place2.right = make_uniq<BoundReferenceExpression>("m_locationid_rowid", LogicalType::BIGINT, 2);
    join_condition_place2.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_place2 = make_uniq<RAIInfo>();
    rai_info_place2->rai = table_comment.GetStorage().info->rais[1].get();
    rai_info_place2->rai_type = RAIType::TARGET_EDGE;
    rai_info_place2->forward = true;
    rai_info_place2->vertex = &table_place;
    rai_info_place2->vertex_id = table_index_place2;
    rai_info_place2->passing_tables[0] = table_index_place2;
    rai_info_place2->left_cardinalities[0] = table_place.GetStorage().info->cardinality;
    // rai_info_place->compact_list = &rai_info_place->rai->alist->compact_forward_list;

    join_condition_place2.rais.push_back(move(rai_info_place2));
    cond_place2.push_back(move(join_condition_place2));

    LogicalComparisonJoin join_place2_op(JoinType::INNER);
    vector<LogicalType> output_place2_types{LogicalType::BIGINT, LogicalType::VARCHAR,
                                            LogicalType::BIGINT, LogicalType::VARCHAR,
                                            LogicalType::VARCHAR};
    join_place2_op.types = output_place2_types;
    vector<idx_t> right_projection_map_place2{3, 4, 5};
    vector<idx_t> merge_project_map_place2;
    vector<LogicalType> delim_types_place2;
    auto join_place2 = make_uniq<PhysicalSIPJoin>(join_place2_op, move(scan_place2), move(join_comment2), move(cond_place2),
                                                  JoinType::INNER, left_projection_map, right_projection_map_place2,
                                                  delim_types_place2, 0);

    // project
    vector<LogicalType> result_types{LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR};
    vector<unique_ptr<Expression>> select_list;
    auto result_col0 = make_uniq<BoundReferenceExpression>("p_personid", LogicalType::BIGINT, 2);
    auto result_col1 = make_uniq<BoundReferenceExpression>("p_firstname", LogicalType::VARCHAR, 3);
    auto result_col2 = make_uniq<BoundReferenceExpression>("p_lastname", LogicalType::VARCHAR, 4);

    select_list.push_back(move(result_col0));
    select_list.push_back(move(result_col1));
    select_list.push_back(move(result_col2));

    auto projection = make_uniq<PhysicalProjection>(result_types, move(select_list), 0);
    projection->children.push_back(move(join_place2));

    return projection;
}

unique_ptr<PhysicalOperator> ClientContext::GenerateIC51Plan() {
    vector<idx_t> left_projection_map, right_projection_map;

    string table_vertex_person = "person";
    string table_vertex_forum = "forum";
    string table_vertex_post = "post";
    string table_edge_knows = "knows";
    string table_edge_forum_person = "forum_person";
    idx_t table_index_person1 = 6;
    idx_t table_index_forum = 10;
    idx_t table_index_person2 = 8;
    idx_t table_index_post = 12;
    idx_t table_index_knows = 7;
    idx_t table_index_forum_person = 13;


    auto table_or_view_person = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                      table_vertex_person, OnEntryNotFound::RETURN_NULL);
    auto& table_person = table_or_view_person->Cast<TableCatalogEntry>();

    auto table_or_view_forum = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                  table_vertex_forum, OnEntryNotFound::RETURN_NULL);
    auto& table_forum = table_or_view_forum->Cast<TableCatalogEntry>();

    auto table_or_view_post = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_vertex_post, OnEntryNotFound::RETURN_NULL);
    auto& table_post = table_or_view_post->Cast<TableCatalogEntry>();

    auto table_or_view_knows = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_edge_knows, OnEntryNotFound::RETURN_NULL);
    auto& table_knows = table_or_view_knows->Cast<TableCatalogEntry>();

    auto table_or_view_forum_person = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_edge_forum_person, OnEntryNotFound::RETURN_NULL);
    auto& table_forum_person = table_or_view_forum_person->Cast<TableCatalogEntry>();


    vector<idx_t> person2_ids{COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person2_types{LogicalType::BIGINT};
    string alias_person2 = "p2";
    vector<LogicalType> table_types_person2;
    vector<unique_ptr<Expression>> filter_person2;
    unique_ptr<LogicalGet> get_op_person2 = move(getLogicalGet(*this, table_person, alias_person2, table_index_person2, table_types_person2));
    unique_ptr<TableFilterSet> table_filters_person2 = NULL;
    unique_ptr<PhysicalTableScan> scan_person2 = make_uniq<PhysicalTableScan>(get_person2_types, get_op_person2->function, get_op_person2->table_index,
                                                                              move(get_op_person2->bind_data), table_types_person2, person2_ids,
                                                                              move(filter_person2), vector<column_t>(), get_op_person2->names, std::move(table_filters_person2),
                                                                              get_op_person2->estimated_cardinality, get_op_person2->extra_info);

    idx_t p_person_id = atoll(paras->data()[0].c_str()); // 933;
    Value p_person = Value::BIGINT(p_person_id);
    vector<idx_t> person1_ids{0, COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person1_types{LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_person1 = "p1";
    vector<LogicalType> table_types_person1;
    unique_ptr<LogicalGet> get_op_person1 = move(getLogicalGet(*this, table_person, alias_person1, table_index_person1, table_types_person1));
    vector<unique_ptr<Expression>> filter_person1;
    unique_ptr<TableFilterSet> table_filters_person1 = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter = duckdb::make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, p_person);
    table_filters_person1->filters[0] = move(constant_filter);
    unique_ptr<PhysicalTableScan> scan_person1 = make_uniq<PhysicalTableScan>(get_person1_types, get_op_person1->function, get_op_person1->table_index,
                                                                              move(get_op_person1->bind_data), table_types_person1, person1_ids,
                                                                              move(filter_person1), vector<column_t>(), get_op_person1->names, std::move(table_filters_person1),
                                                                              get_op_person1->estimated_cardinality, get_op_person1->extra_info);

    vector<JoinCondition> cond_knows;
    JoinCondition join_condition_knows;
    join_condition_knows.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 1);
    join_condition_knows.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows = make_uniq<RAIInfo>();
    rai_info_knows->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows->forward = true;
    rai_info_knows->vertex = &table_person;
    rai_info_knows->vertex_id = table_index_person2;
    rai_info_knows->passing_tables[0] = table_index_person2;
    rai_info_knows->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows.rais.push_back(move(rai_info_knows));
    cond_knows.push_back(move(join_condition_knows));

    LogicalComparisonJoin join_knows_op(JoinType::INNER);
    vector<LogicalType> output_knows_types{LogicalType::BIGINT, LogicalType::BIGINT};
    join_knows_op.types = output_knows_types;
    vector<idx_t> right_projection_map_knows{1};
    vector<idx_t> merge_project_map;
    vector<LogicalType> delim_types;
    auto join_knows = make_uniq<PhysicalMergeSIPJoin>(join_knows_op, move(scan_person2), move(scan_person1), move(cond_knows),
                                                      JoinType::INNER, left_projection_map, right_projection_map_knows,
                                                      merge_project_map, delim_types, 0);

    // join post with person-person
    vector<idx_t> post_ids{11, COLUMN_IDENTIFIER_ROW_ID, 13};
    vector<LogicalType> get_post_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_post = "m";
    vector<LogicalType> table_types_post;
    vector<unique_ptr<Expression>> filter_post;
    unique_ptr<LogicalGet> get_op_post = move(getLogicalGet(*this, table_post, alias_post, table_index_post, table_types_post));
    unique_ptr<TableFilterSet> table_filters_post = NULL;
    unique_ptr<PhysicalTableScan> scan_post = make_uniq<PhysicalTableScan>(get_post_types, get_op_post->function, get_op_post->table_index,
                                                                           move(get_op_post->bind_data), table_types_post, post_ids,
                                                                           move(filter_post), vector<column_t>(), get_op_post->names, std::move(table_filters_post),
                                                                           get_op_post->estimated_cardinality, get_op_post->extra_info);

    vector<JoinCondition> cond_post;
    JoinCondition join_condition_post;
    join_condition_post.left = make_uniq<BoundReferenceExpression>("m_creatorid_rowid", LogicalType::BIGINT, 0);
    join_condition_post.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_post.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_post = make_uniq<RAIInfo>();
    rai_info_post->rai = table_post.GetStorage().info->rais[0].get();
    rai_info_post->rai_type = RAIType::EDGE_SOURCE;
    rai_info_post->forward = true;
    rai_info_post->vertex = &table_person;
    rai_info_post->vertex_id = table_index_person2;
    rai_info_post->passing_tables[0] = table_index_post;
    rai_info_post->left_cardinalities[0] = table_post.GetStorage().info->cardinality;
    rai_info_post->compact_list = &rai_info_post->rai->alist->compact_forward_list;

    join_condition_post.rais.push_back(move(rai_info_post));
    cond_post.push_back(move(join_condition_post));

    LogicalComparisonJoin join_post_op(JoinType::INNER);
    vector<LogicalType> output_post_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
    join_post_op.types = output_post_types;
    vector<idx_t> right_projection_map_post{0};
    vector<idx_t> merge_project_map_post;
    vector<LogicalType> delim_types_post;
    auto join_post = make_uniq<PhysicalSIPJoin>(join_post_op, move(scan_post), move(join_knows), move(cond_post),
                                                JoinType::INNER, left_projection_map, right_projection_map_post,
                                                delim_types_post, 0);

    // join forum with person-person-post
    vector<idx_t> forum_ids{1, COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_forum_types{LogicalType::VARCHAR, LogicalType::BIGINT};
    string alias_forum = "f";
    vector<LogicalType> table_types_forum;
    vector<unique_ptr<Expression>> filter_forum;
    unique_ptr<LogicalGet> get_op_forum = move(getLogicalGet(*this, table_forum, alias_forum, table_index_forum, table_types_forum));
    unique_ptr<TableFilterSet> table_filters_forum = NULL;
    unique_ptr<PhysicalTableScan> scan_forum = make_uniq<PhysicalTableScan>(get_forum_types, get_op_forum->function, get_op_forum->table_index,
                                                                           move(get_op_forum->bind_data), table_types_forum, forum_ids,
                                                                           move(filter_forum), vector<column_t>(), get_op_forum->names, std::move(table_filters_forum),
                                                                           get_op_forum->estimated_cardinality, get_op_forum->extra_info);

    vector<JoinCondition> cond_forum;
    JoinCondition join_condition_forum;
    join_condition_forum.left = make_uniq<BoundReferenceExpression>("forum_rowid", LogicalType::BIGINT, 1);
    join_condition_forum.right = make_uniq<BoundReferenceExpression>("m_ps_forumid_rowid", LogicalType::BIGINT, 2);
    join_condition_forum.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_forum = make_uniq<RAIInfo>();
    rai_info_forum->rai = table_post.GetStorage().info->rais[2].get();
    rai_info_forum->rai_type = RAIType::TARGET_EDGE;
    rai_info_forum->forward = true;
    rai_info_forum->vertex = &table_forum;
    rai_info_forum->vertex_id = table_index_forum;
    rai_info_forum->passing_tables[0] = table_index_forum;
    rai_info_forum->left_cardinalities[0] = table_forum.GetStorage().info->cardinality;
    // rai_info_forum->compact_list = &rai_info_forum->rai->alist->compact_forward_list;

    join_condition_forum.rais.push_back(move(rai_info_forum));
    cond_forum.push_back(move(join_condition_forum));

    LogicalComparisonJoin join_forum_op(JoinType::INNER);
    vector<LogicalType> output_forum_types{LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT};
    join_forum_op.types = output_forum_types;
    vector<idx_t> right_projection_map_forum{3};
    vector<idx_t> merge_project_map_forum;
    vector<LogicalType> delim_types_forum;
    auto join_forum = make_uniq<PhysicalSIPJoin>(join_forum_op, move(scan_forum), move(join_post), move(cond_forum),
                                                 JoinType::INNER, left_projection_map, right_projection_map_forum,
                                                 delim_types_forum, 0);


    // join person_forum with person-person-post-forum
    idx_t p_forum_person_joindate = atoll(paras->data()[1].c_str());
    Value p_joindate = Value::BIGINT(p_forum_person_joindate);
    vector<idx_t> forum_person_ids{4, 3, 2};
    vector<LogicalType> get_forum_person_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_forum_person = "fp";
    vector<LogicalType> table_types_forum_person;
    vector<unique_ptr<Expression>> filter_forum_person;
    unique_ptr<LogicalGet> get_op_forum_person = move(getLogicalGet(*this, table_forum_person, alias_forum_person, table_index_forum_person, table_types_forum_person));
    unique_ptr<TableFilterSet> table_filters_forum_person = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter_forum_person = duckdb::make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, p_joindate);
    table_filters_forum_person->filters[2] = move(constant_filter_forum_person);
    unique_ptr<PhysicalTableScan> scan_forum_person = make_uniq<PhysicalTableScan>(get_forum_person_types, get_op_forum_person->function, get_op_forum_person->table_index,
                                                                            move(get_op_forum_person->bind_data), table_types_forum_person, forum_person_ids,
                                                                            move(filter_forum_person), vector<column_t>(), get_op_forum_person->names, std::move(table_filters_forum_person),
                                                                            get_op_forum_person->estimated_cardinality, get_op_forum_person->extra_info);

    vector<JoinCondition> cond_forum_person;
    JoinCondition join_condition_forum_person, join_condition_forum_person_2;
    join_condition_forum_person.left = make_uniq<BoundReferenceExpression>("fp_forumid_rowid", LogicalType::BIGINT, 0);
    join_condition_forum_person.right = make_uniq<BoundReferenceExpression>("forum_rowid", LogicalType::BIGINT, 1);
    join_condition_forum_person.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_forum_person = make_uniq<RAIInfo>();
    rai_info_forum_person->rai = table_forum_person.GetStorage().info->rais[0].get();
    rai_info_forum_person->rai_type = RAIType::EDGE_TARGET;
    rai_info_forum_person->forward = false;
    rai_info_forum_person->vertex = &table_forum;
    rai_info_forum_person->vertex_id = table_index_forum;
    rai_info_forum_person->passing_tables[0] = table_index_forum_person;
    rai_info_forum_person->left_cardinalities[0] = table_forum_person.GetStorage().info->cardinality;
    rai_info_forum_person->compact_list = &rai_info_forum_person->rai->alist->compact_backward_list;

    join_condition_forum_person.rais.push_back(move(rai_info_forum_person));

    join_condition_forum_person_2.left = make_uniq<BoundReferenceExpression>("fp_personid_rowid", LogicalType::BIGINT, 1);
    join_condition_forum_person_2.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 2);
    join_condition_forum_person_2.comparison = ExpressionType::COMPARE_EQUAL;

    cond_forum_person.push_back(move(join_condition_forum_person));
    cond_forum_person.push_back(move(join_condition_forum_person_2));

    LogicalComparisonJoin join_forum_person_op(JoinType::INNER);
    vector<LogicalType> output_forum_person_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR};
    join_forum_person_op.types = output_forum_person_types;
    vector<idx_t> right_projection_map_forum_person{0};
    vector<idx_t> merge_project_map_forum_person;
    vector<LogicalType> delim_types_forum_person;
    auto join_forum_person = make_uniq<PhysicalSIPJoin>(join_forum_person_op, move(scan_forum_person), move(join_forum), move(cond_forum_person),
                                                        JoinType::INNER, left_projection_map, right_projection_map_forum_person,
                                                        delim_types_forum_person, 0);


    // project
    vector<LogicalType> result_types{LogicalType::VARCHAR};
    vector<unique_ptr<Expression>> select_list;
    auto result_col0 = make_uniq<BoundReferenceExpression>("f_title", LogicalType::VARCHAR, 3);
    select_list.push_back(move(result_col0));
    auto projection = make_uniq<PhysicalProjection>(result_types, move(select_list), 0);
    projection->children.push_back(move(join_forum_person));

    return projection;
}

unique_ptr<PhysicalOperator> ClientContext::GenerateIC52Plan() {
    vector<idx_t> left_projection_map, right_projection_map;

    string table_vertex_person = "person";
    string table_vertex_forum = "forum";
    string table_vertex_post = "post";
    string table_edge_knows = "knows";
    string table_edge_forum_person = "forum_person";
    idx_t table_index_person1 = 6;
    idx_t table_index_forum = 10;
    idx_t table_index_person2 = 8;
    idx_t table_index_person3 = 9;
    idx_t table_index_post = 12;
    idx_t table_index_knows = 7;
    idx_t table_index_forum_person = 13;


    auto table_or_view_person = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                  table_vertex_person, OnEntryNotFound::RETURN_NULL);
    auto &table_person = table_or_view_person->Cast<TableCatalogEntry>();

    auto table_or_view_forum = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_vertex_forum, OnEntryNotFound::RETURN_NULL);
    auto &table_forum = table_or_view_forum->Cast<TableCatalogEntry>();

    auto table_or_view_post = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                table_vertex_post, OnEntryNotFound::RETURN_NULL);
    auto &table_post = table_or_view_post->Cast<TableCatalogEntry>();

    auto table_or_view_knows = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_edge_knows, OnEntryNotFound::RETURN_NULL);
    auto &table_knows = table_or_view_knows->Cast<TableCatalogEntry>();

    auto table_or_view_forum_person = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                        table_edge_forum_person, OnEntryNotFound::RETURN_NULL);
    auto &table_forum_person = table_or_view_forum_person->Cast<TableCatalogEntry>();


    vector<idx_t> person2_ids{COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person2_types{LogicalType::BIGINT};
    string alias_person2 = "p2";
    vector<LogicalType> table_types_person2;
    vector<unique_ptr<Expression>> filter_person2;
    unique_ptr<LogicalGet> get_op_person2 = move(
            getLogicalGet(*this, table_person, alias_person2, table_index_person2, table_types_person2));
    unique_ptr<TableFilterSet> table_filters_person2 = NULL;
    unique_ptr<PhysicalTableScan> scan_person2 = make_uniq<PhysicalTableScan>(get_person2_types,
                                                                              get_op_person2->function,
                                                                              get_op_person2->table_index,
                                                                              move(get_op_person2->bind_data),
                                                                              table_types_person2, person2_ids,
                                                                              move(filter_person2), vector<column_t>(),
                                                                              get_op_person2->names,
                                                                              std::move(table_filters_person2),
                                                                              get_op_person2->estimated_cardinality,
                                                                              get_op_person2->extra_info);

    idx_t p_person_id = atoll(paras->data()[0].c_str()); // 933;
    Value p_person = Value::BIGINT(p_person_id);
    vector<idx_t> person1_ids{0, COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person1_types{LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_person1 = "p1";
    vector<LogicalType> table_types_person1;
    unique_ptr<LogicalGet> get_op_person1 = move(
            getLogicalGet(*this, table_person, alias_person1, table_index_person1, table_types_person1));
    vector<unique_ptr<Expression>> filter_person1;
    unique_ptr<TableFilterSet> table_filters_person1 = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter = duckdb::make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL,
                                                                                   p_person);
    table_filters_person1->filters[0] = move(constant_filter);
    unique_ptr<PhysicalTableScan> scan_person1 = make_uniq<PhysicalTableScan>(get_person1_types,
                                                                              get_op_person1->function,
                                                                              get_op_person1->table_index,
                                                                              move(get_op_person1->bind_data),
                                                                              table_types_person1, person1_ids,
                                                                              move(filter_person1), vector<column_t>(),
                                                                              get_op_person1->names,
                                                                              std::move(table_filters_person1),
                                                                              get_op_person1->estimated_cardinality,
                                                                              get_op_person1->extra_info);

    vector<JoinCondition> cond_knows;
    JoinCondition join_condition_knows;
    join_condition_knows.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 1);
    join_condition_knows.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows = make_uniq<RAIInfo>();
    rai_info_knows->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows->forward = true;
    rai_info_knows->vertex = &table_person;
    rai_info_knows->vertex_id = table_index_person2;
    rai_info_knows->passing_tables[0] = table_index_person2;
    rai_info_knows->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows.rais.push_back(move(rai_info_knows));
    cond_knows.push_back(move(join_condition_knows));

    LogicalComparisonJoin join_knows_op(JoinType::INNER);
    vector<LogicalType> output_knows_types{LogicalType::BIGINT, LogicalType::BIGINT};
    join_knows_op.types = output_knows_types;
    vector<idx_t> right_projection_map_knows{1};
    vector<idx_t> merge_project_map;
    vector<LogicalType> delim_types;
    auto join_knows = make_uniq<PhysicalMergeSIPJoin>(join_knows_op, move(scan_person2), move(scan_person1),
                                                      move(cond_knows),
                                                      JoinType::INNER, left_projection_map, right_projection_map_knows,
                                                      merge_project_map, delim_types, 0);

    // join the 2-hop neighbors
    vector<idx_t> person3_ids{COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person3_types{LogicalType::BIGINT};
    string alias_person3 = "p3";
    vector<LogicalType> table_types_person3;
    vector<unique_ptr<Expression>> filter_person3;
    unique_ptr<LogicalGet> get_op_person3 = move(
            getLogicalGet(*this, table_person, alias_person3, table_index_person3, table_types_person3));
    unique_ptr<TableFilterSet> table_filters_person3 = NULL;
    unique_ptr<PhysicalTableScan> scan_person3 = make_uniq<PhysicalTableScan>(get_person3_types,
                                                                              get_op_person3->function,
                                                                              get_op_person3->table_index,
                                                                              move(get_op_person3->bind_data),
                                                                              table_types_person3, person3_ids,
                                                                              move(filter_person3), vector<column_t>(),
                                                                              get_op_person3->names,
                                                                              std::move(table_filters_person3),
                                                                              get_op_person3->estimated_cardinality,
                                                                              get_op_person3->extra_info);

    vector<JoinCondition> cond_knows_2;
    JoinCondition join_condition_knows_2;
    join_condition_knows_2.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows_2.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows_2.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows_2 = make_uniq<RAIInfo>();
    rai_info_knows_2->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows_2->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows_2->forward = true;
    rai_info_knows_2->vertex = &table_person;
    rai_info_knows_2->vertex_id = table_index_person3;
    rai_info_knows_2->passing_tables[0] = table_index_person3;
    rai_info_knows_2->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows_2.rais.push_back(move(rai_info_knows_2));
    cond_knows_2.push_back(move(join_condition_knows_2));

    LogicalComparisonJoin join_knows_op_2(JoinType::INNER);
    vector<LogicalType> output_knows_types_2{LogicalType::BIGINT, LogicalType::BIGINT};
    join_knows_op_2.types = output_knows_types_2;
    vector<idx_t> right_projection_map_knows_2{0};
    vector<idx_t> merge_project_map_2;
    vector<LogicalType> delim_types_2;
    auto join_knows_2 = make_uniq<PhysicalMergeSIPJoin>(join_knows_op_2, move(scan_person3), move(join_knows),
                                                      move(cond_knows_2),
                                                      JoinType::INNER, left_projection_map, right_projection_map_knows_2,
                                                      merge_project_map_2, delim_types_2, 0);

    // join post with person-person
    vector<idx_t> post_ids{11, COLUMN_IDENTIFIER_ROW_ID, 13};
    vector<LogicalType> get_post_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_post = "m";
    vector<LogicalType> table_types_post;
    vector<unique_ptr<Expression>> filter_post;
    unique_ptr<LogicalGet> get_op_post = move(
            getLogicalGet(*this, table_post, alias_post, table_index_post, table_types_post));
    unique_ptr<TableFilterSet> table_filters_post = NULL;
    unique_ptr<PhysicalTableScan> scan_post = make_uniq<PhysicalTableScan>(get_post_types, get_op_post->function,
                                                                           get_op_post->table_index,
                                                                           move(get_op_post->bind_data),
                                                                           table_types_post, post_ids,
                                                                           move(filter_post), vector<column_t>(),
                                                                           get_op_post->names,
                                                                           std::move(table_filters_post),
                                                                           get_op_post->estimated_cardinality,
                                                                           get_op_post->extra_info);

    vector<JoinCondition> cond_post;
    JoinCondition join_condition_post;
    join_condition_post.left = make_uniq<BoundReferenceExpression>("m_creatorid_rowid", LogicalType::BIGINT, 0);
    join_condition_post.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_post.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_post = make_uniq<RAIInfo>();
    rai_info_post->rai = table_post.GetStorage().info->rais[0].get();
    rai_info_post->rai_type = RAIType::EDGE_SOURCE;
    rai_info_post->forward = true;
    rai_info_post->vertex = &table_person;
    rai_info_post->vertex_id = table_index_person2;
    rai_info_post->passing_tables[0] = table_index_post;
    rai_info_post->left_cardinalities[0] = table_post.GetStorage().info->cardinality;
    rai_info_post->compact_list = &rai_info_post->rai->alist->compact_forward_list;

    join_condition_post.rais.push_back(move(rai_info_post));
    cond_post.push_back(move(join_condition_post));

    LogicalComparisonJoin join_post_op(JoinType::INNER);
    vector<LogicalType> output_post_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                                          LogicalType::BIGINT};
    join_post_op.types = output_post_types;
    vector<idx_t> right_projection_map_post{0};
    vector<idx_t> merge_project_map_post;
    vector<LogicalType> delim_types_post;
    auto join_post = make_uniq<PhysicalSIPJoin>(join_post_op, move(scan_post), move(join_knows_2), move(cond_post),
                                                JoinType::INNER, left_projection_map, right_projection_map_post,
                                                delim_types_post, 0);

    // join forum with person-person-post
    vector<idx_t> forum_ids{1, COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_forum_types{LogicalType::VARCHAR, LogicalType::BIGINT};
    string alias_forum = "f";
    vector<LogicalType> table_types_forum;
    vector<unique_ptr<Expression>> filter_forum;
    unique_ptr<LogicalGet> get_op_forum = move(
            getLogicalGet(*this, table_forum, alias_forum, table_index_forum, table_types_forum));
    unique_ptr<TableFilterSet> table_filters_forum = NULL;
    unique_ptr<PhysicalTableScan> scan_forum = make_uniq<PhysicalTableScan>(get_forum_types, get_op_forum->function,
                                                                            get_op_forum->table_index,
                                                                            move(get_op_forum->bind_data),
                                                                            table_types_forum, forum_ids,
                                                                            move(filter_forum), vector<column_t>(),
                                                                            get_op_forum->names,
                                                                            std::move(table_filters_forum),
                                                                            get_op_forum->estimated_cardinality,
                                                                            get_op_forum->extra_info);

    vector<JoinCondition> cond_forum;
    JoinCondition join_condition_forum;
    join_condition_forum.left = make_uniq<BoundReferenceExpression>("forum_rowid", LogicalType::BIGINT, 1);
    join_condition_forum.right = make_uniq<BoundReferenceExpression>("m_ps_forumid_rowid", LogicalType::BIGINT, 2);
    join_condition_forum.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_forum = make_uniq<RAIInfo>();
    rai_info_forum->rai = table_post.GetStorage().info->rais[2].get();
    rai_info_forum->rai_type = RAIType::TARGET_EDGE;
    rai_info_forum->forward = true;
    rai_info_forum->vertex = &table_forum;
    rai_info_forum->vertex_id = table_index_forum;
    rai_info_forum->passing_tables[0] = table_index_forum;
    rai_info_forum->left_cardinalities[0] = table_forum.GetStorage().info->cardinality;
    // rai_info_forum->compact_list = &rai_info_forum->rai->alist->compact_forward_list;

    join_condition_forum.rais.push_back(move(rai_info_forum));
    cond_forum.push_back(move(join_condition_forum));

    LogicalComparisonJoin join_forum_op(JoinType::INNER);
    vector<LogicalType> output_forum_types{LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT};
    join_forum_op.types = output_forum_types;
    vector<idx_t> right_projection_map_forum{3};
    vector<idx_t> merge_project_map_forum;
    vector<LogicalType> delim_types_forum;
    auto join_forum = make_uniq<PhysicalSIPJoin>(join_forum_op, move(scan_forum), move(join_post), move(cond_forum),
                                                 JoinType::INNER, left_projection_map, right_projection_map_forum,
                                                 delim_types_forum, 0);


    // join person_forum with person-person-post-forum
    idx_t p_forum_person_joindate = atoll(paras->data()[1].c_str());
    Value p_joindate = Value::BIGINT(p_forum_person_joindate);
    vector<idx_t> forum_person_ids{4, 3, 2};
    vector<LogicalType> get_forum_person_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_forum_person = "fp";
    vector<LogicalType> table_types_forum_person;
    vector<unique_ptr<Expression>> filter_forum_person;
    unique_ptr<LogicalGet> get_op_forum_person = move(
            getLogicalGet(*this, table_forum_person, alias_forum_person, table_index_forum_person,
                          table_types_forum_person));
    unique_ptr<TableFilterSet> table_filters_forum_person = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter_forum_person = duckdb::make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_GREATERTHANOREQUALTO, p_joindate);
    table_filters_forum_person->filters[2] = move(constant_filter_forum_person);
    unique_ptr<PhysicalTableScan> scan_forum_person = make_uniq<PhysicalTableScan>(get_forum_person_types,
                                                                                   get_op_forum_person->function,
                                                                                   get_op_forum_person->table_index,
                                                                                   move(get_op_forum_person->bind_data),
                                                                                   table_types_forum_person,
                                                                                   forum_person_ids,
                                                                                   move(filter_forum_person),
                                                                                   vector<column_t>(),
                                                                                   get_op_forum_person->names,
                                                                                   std::move(
                                                                                           table_filters_forum_person),
                                                                                   get_op_forum_person->estimated_cardinality,
                                                                                   get_op_forum_person->extra_info);

    vector<JoinCondition> cond_forum_person;
    JoinCondition join_condition_forum_person, join_condition_forum_person_2;
    join_condition_forum_person.left = make_uniq<BoundReferenceExpression>("fp_forumid_rowid", LogicalType::BIGINT, 0);
    join_condition_forum_person.right = make_uniq<BoundReferenceExpression>("forum_rowid", LogicalType::BIGINT, 1);
    join_condition_forum_person.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_forum_person = make_uniq<RAIInfo>();
    rai_info_forum_person->rai = table_forum_person.GetStorage().info->rais[0].get();
    rai_info_forum_person->rai_type = RAIType::EDGE_TARGET;
    rai_info_forum_person->forward = false;
    rai_info_forum_person->vertex = &table_forum;
    rai_info_forum_person->vertex_id = table_index_forum;
    rai_info_forum_person->passing_tables[0] = table_index_forum_person;
    rai_info_forum_person->left_cardinalities[0] = table_forum_person.GetStorage().info->cardinality;
    rai_info_forum_person->compact_list = &rai_info_forum_person->rai->alist->compact_backward_list;

    join_condition_forum_person.rais.push_back(move(rai_info_forum_person));

    join_condition_forum_person_2.left = make_uniq<BoundReferenceExpression>("fp_personid_rowid", LogicalType::BIGINT,
                                                                             1);
    join_condition_forum_person_2.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 2);
    join_condition_forum_person_2.comparison = ExpressionType::COMPARE_EQUAL;

    cond_forum_person.push_back(move(join_condition_forum_person));
    cond_forum_person.push_back(move(join_condition_forum_person_2));

    LogicalComparisonJoin join_forum_person_op(JoinType::INNER);
    vector<LogicalType> output_forum_person_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                                                  LogicalType::VARCHAR};
    join_forum_person_op.types = output_forum_person_types;
    vector<idx_t> right_projection_map_forum_person{0};
    vector<idx_t> merge_project_map_forum_person;
    vector<LogicalType> delim_types_forum_person;
    auto join_forum_person = make_uniq<PhysicalSIPJoin>(join_forum_person_op, move(scan_forum_person), move(join_forum),
                                                        move(cond_forum_person),
                                                        JoinType::INNER, left_projection_map,
                                                        right_projection_map_forum_person,
                                                        delim_types_forum_person, 0);


    // project
    vector<LogicalType> result_types{LogicalType::VARCHAR};
    vector<unique_ptr<Expression>> select_list;
    auto result_col0 = make_uniq<BoundReferenceExpression>("f_title", LogicalType::VARCHAR, 3);
    select_list.push_back(move(result_col0));
    auto projection = make_uniq<PhysicalProjection>(result_types, move(select_list), 0);
    projection->children.push_back(move(join_forum_person));

    return projection;
}

unique_ptr<PhysicalOperator> ClientContext::GenerateIC52PlanSelf() {
    vector<idx_t> left_projection_map, right_projection_map;

    string table_vertex_person = "person";
    string table_vertex_forum = "forum";
    string table_vertex_post = "post";
    string table_edge_knows = "knows";
    string table_edge_forum_person = "forum_person";
    idx_t table_index_person1 = 6;
    idx_t table_index_forum = 10;
    idx_t table_index_person2 = 8;
    idx_t table_index_person3 = 9;
    idx_t table_index_post = 12;
    idx_t table_index_knows = 7;
    idx_t table_index_knows2 = 14;
    idx_t table_index_forum_person = 13;


    auto table_or_view_person = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                  table_vertex_person, OnEntryNotFound::RETURN_NULL);
    auto &table_person = table_or_view_person->Cast<TableCatalogEntry>();

    auto table_or_view_forum = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_vertex_forum, OnEntryNotFound::RETURN_NULL);
    auto &table_forum = table_or_view_forum->Cast<TableCatalogEntry>();

    auto table_or_view_post = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                table_vertex_post, OnEntryNotFound::RETURN_NULL);
    auto &table_post = table_or_view_post->Cast<TableCatalogEntry>();

    auto table_or_view_knows = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_edge_knows, OnEntryNotFound::RETURN_NULL);
    auto &table_knows = table_or_view_knows->Cast<TableCatalogEntry>();

    auto table_or_view_forum_person = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                        table_edge_forum_person, OnEntryNotFound::RETURN_NULL);
    auto &table_forum_person = table_or_view_forum_person->Cast<TableCatalogEntry>();


    vector<idx_t> knows_ids{3, 4};
    vector<LogicalType> get_knows_types{LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_knows = "k1";
    vector<LogicalType> table_types_knows;
    vector<unique_ptr<Expression>> filter_knows;
    unique_ptr<LogicalGet> get_op_knows = move(
            getLogicalGet(*this, table_knows, alias_knows, table_index_knows, table_types_knows));
    unique_ptr<TableFilterSet> table_filters_knows = NULL;
    unique_ptr<PhysicalTableScan> scan_knows = make_uniq<PhysicalTableScan>(get_knows_types,
                                                                              get_op_knows->function,
                                                                              get_op_knows->table_index,
                                                                              move(get_op_knows->bind_data),
                                                                              table_types_knows, knows_ids,
                                                                              move(filter_knows), vector<column_t>(),
                                                                              get_op_knows->names,
                                                                              std::move(table_filters_knows),
                                                                              get_op_knows->estimated_cardinality,
                                                                              get_op_knows->extra_info);

    idx_t p_person_id = atoll(paras->data()[0].c_str()); // 933;
    Value p_person = Value::BIGINT(p_person_id);
    vector<idx_t> person1_ids{0, COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person1_types{LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_person1 = "p1";
    vector<LogicalType> table_types_person1;
    unique_ptr<LogicalGet> get_op_person1 = move(
            getLogicalGet(*this, table_person, alias_person1, table_index_person1, table_types_person1));
    vector<unique_ptr<Expression>> filter_person1;
    unique_ptr<TableFilterSet> table_filters_person1 = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter = duckdb::make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL,
                                                                                   p_person);
    table_filters_person1->filters[0] = move(constant_filter);
    unique_ptr<PhysicalTableScan> scan_person1 = make_uniq<PhysicalTableScan>(get_person1_types,
                                                                              get_op_person1->function,
                                                                              get_op_person1->table_index,
                                                                              move(get_op_person1->bind_data),
                                                                              table_types_person1, person1_ids,
                                                                              move(filter_person1), vector<column_t>(),
                                                                              get_op_person1->names,
                                                                              std::move(table_filters_person1),
                                                                              get_op_person1->estimated_cardinality,
                                                                              get_op_person1->extra_info);

    vector<JoinCondition> cond_knows;
    JoinCondition join_condition_knows;
    join_condition_knows.left = make_uniq<BoundReferenceExpression>("k_person1id_rowid", LogicalType::BIGINT, 0);
    join_condition_knows.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 1);
    join_condition_knows.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows = make_uniq<RAIInfo>();
    rai_info_knows->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows->rai_type = RAIType::EDGE_SOURCE;
    rai_info_knows->forward = true;
    rai_info_knows->vertex = &table_person;
    rai_info_knows->vertex_id = table_index_person1;
    rai_info_knows->passing_tables[0] = table_index_person1;
    rai_info_knows->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_forward_list;

    join_condition_knows.rais.push_back(move(rai_info_knows));
    cond_knows.push_back(move(join_condition_knows));

    LogicalComparisonJoin join_knows_op(JoinType::INNER);
    vector<LogicalType> output_knows_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
    join_knows_op.types = output_knows_types;
    vector<idx_t> right_projection_map_knows{1};
    vector<idx_t> merge_project_map;
    vector<LogicalType> delim_types;
    auto join_knows = make_uniq<PhysicalSIPJoin>(join_knows_op, move(scan_knows), move(scan_person1),
                                                      move(cond_knows),
                                                      JoinType::INNER, left_projection_map, right_projection_map_knows,
                                                      delim_types, 0);

    // join the two knows
    vector<idx_t> knows2_ids{3, 4};
    vector<LogicalType> get_knows2_types{LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_knows2 = "k2";
    vector<LogicalType> table_types_knows2;
    vector<unique_ptr<Expression>> filter_knows2;
    unique_ptr<LogicalGet> get_op_knows2 = move(
            getLogicalGet(*this, table_knows, alias_knows2, table_index_knows2, table_types_knows2));
    unique_ptr<TableFilterSet> table_filters_knows2 = NULL;
    unique_ptr<PhysicalTableScan> scan_knows2 = make_uniq<PhysicalTableScan>(get_knows2_types,
                                                                              get_op_knows2->function,
                                                                              get_op_knows2->table_index,
                                                                              move(get_op_knows2->bind_data),
                                                                              table_types_knows2, knows2_ids,
                                                                              move(filter_knows2), vector<column_t>(),
                                                                              get_op_knows2->names,
                                                                              std::move(table_filters_knows2),
                                                                              get_op_knows2->estimated_cardinality,
                                                                              get_op_knows2->extra_info);

    vector<JoinCondition> cond_knows_2;
    JoinCondition join_condition_knows_2;
    join_condition_knows_2.left = make_uniq<BoundReferenceExpression>("k_person1id_rowid", LogicalType::BIGINT, 0);
    join_condition_knows_2.right = make_uniq<BoundReferenceExpression>("k_person2id_rowid", LogicalType::BIGINT, 1);
    join_condition_knows_2.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows_2 = make_uniq<RAIInfo>();
    rai_info_knows_2->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows_2->rai_type = RAIType::SELF;
    rai_info_knows_2->forward = true;
    rai_info_knows_2->vertex = &table_person;
    rai_info_knows_2->vertex_id = table_index_knows2;
    rai_info_knows_2->passing_tables[0] = table_index_knows2;
    rai_info_knows_2->left_cardinalities[0] = table_knows.GetStorage().info->cardinality;
    rai_info_knows_2->compact_list = &rai_info_knows_2->rai->alist->compact_forward_list;

    join_condition_knows_2.rais.push_back(move(rai_info_knows_2));
    cond_knows_2.push_back(move(join_condition_knows_2));

    LogicalComparisonJoin join_knows_op_2(JoinType::INNER);
    vector<LogicalType> output_knows_types_2{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
    join_knows_op_2.types = output_knows_types_2;
    vector<idx_t> right_projection_map_knows_2{0};
    vector<idx_t> merge_project_map_2;
    vector<LogicalType> delim_types_2;
    auto join_knows_2 = make_uniq<PhysicalSIPJoin>(join_knows_op_2, move(scan_knows2), move(join_knows),
                                                        move(cond_knows_2),
                                                        JoinType::INNER, left_projection_map,
                                                        right_projection_map_knows_2,
                                                        delim_types_2, 0);


    vector<idx_t> person2_ids{COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person2_types{LogicalType::BIGINT};
    string alias_person2 = "p2";
    vector<LogicalType> table_types_person2;
    unique_ptr<LogicalGet> get_op_person2 = move(
            getLogicalGet(*this, table_person, alias_person2, table_index_person2, table_types_person2));
    vector<unique_ptr<Expression>> filter_person2;
    unique_ptr<TableFilterSet> table_filters_person2 = NULL;
    unique_ptr<PhysicalTableScan> scan_person2 = make_uniq<PhysicalTableScan>(get_person2_types,
                                                                              get_op_person2->function,
                                                                              get_op_person2->table_index,
                                                                              move(get_op_person2->bind_data),
                                                                              table_types_person2, person2_ids,
                                                                              move(filter_person2), vector<column_t>(),
                                                                              get_op_person2->names,
                                                                              std::move(table_filters_person2),
                                                                              get_op_person2->estimated_cardinality,
                                                                              get_op_person2->extra_info);

    vector<JoinCondition> cond_knows_3;
    JoinCondition join_condition_knows_3;
    join_condition_knows_3.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows_3.right = make_uniq<BoundReferenceExpression>("k_person2id_rowid", LogicalType::BIGINT, 1);
    join_condition_knows_3.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows_3 = make_uniq<RAIInfo>();
    rai_info_knows_3->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows_3->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows_3->forward = true;
    rai_info_knows_3->vertex = &table_person;
    rai_info_knows_3->vertex_id = table_index_person2;
    rai_info_knows_3->passing_tables[0] = table_index_person2;
    rai_info_knows_3->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows_3.rais.push_back(move(rai_info_knows_3));
    cond_knows_3.push_back(move(join_condition_knows_3));

    LogicalComparisonJoin join_knows_op_3(JoinType::INNER);
    vector<LogicalType> output_knows_types_3{LogicalType::BIGINT, LogicalType::BIGINT};
    join_knows_op_3.types = output_knows_types_3;
    vector<idx_t> right_projection_map_knows_3{0};
    vector<idx_t> merge_project_map_3;
    vector<LogicalType> delim_types_3;
    auto join_knows_3 = make_uniq<PhysicalSIPJoin>(join_knows_op_3, move(scan_person2), move(join_knows_2),
                                                        move(cond_knows_3),
                                                        JoinType::INNER, left_projection_map,
                                                        right_projection_map_knows_3,
                                                        delim_types_3, 0);

    // join post with person-person
    vector<idx_t> post_ids{11, COLUMN_IDENTIFIER_ROW_ID, 13};
    vector<LogicalType> get_post_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_post = "m";
    vector<LogicalType> table_types_post;
    vector<unique_ptr<Expression>> filter_post;
    unique_ptr<LogicalGet> get_op_post = move(
            getLogicalGet(*this, table_post, alias_post, table_index_post, table_types_post));
    unique_ptr<TableFilterSet> table_filters_post = NULL;
    unique_ptr<PhysicalTableScan> scan_post = make_uniq<PhysicalTableScan>(get_post_types, get_op_post->function,
                                                                           get_op_post->table_index,
                                                                           move(get_op_post->bind_data),
                                                                           table_types_post, post_ids,
                                                                           move(filter_post), vector<column_t>(),
                                                                           get_op_post->names,
                                                                           std::move(table_filters_post),
                                                                           get_op_post->estimated_cardinality,
                                                                           get_op_post->extra_info);

    vector<JoinCondition> cond_post;
    JoinCondition join_condition_post;
    join_condition_post.left = make_uniq<BoundReferenceExpression>("m_creatorid_rowid", LogicalType::BIGINT, 0);
    join_condition_post.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_post.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_post = make_uniq<RAIInfo>();
    rai_info_post->rai = table_post.GetStorage().info->rais[0].get();
    rai_info_post->rai_type = RAIType::EDGE_SOURCE;
    rai_info_post->forward = true;
    rai_info_post->vertex = &table_person;
    rai_info_post->vertex_id = table_index_person2;
    rai_info_post->passing_tables[0] = table_index_post;
    rai_info_post->left_cardinalities[0] = table_post.GetStorage().info->cardinality;
    rai_info_post->compact_list = &rai_info_post->rai->alist->compact_forward_list;

    join_condition_post.rais.push_back(move(rai_info_post));
    cond_post.push_back(move(join_condition_post));

    LogicalComparisonJoin join_post_op(JoinType::INNER);
    vector<LogicalType> output_post_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                                          LogicalType::BIGINT};
    join_post_op.types = output_post_types;
    vector<idx_t> right_projection_map_post{0};
    vector<idx_t> merge_project_map_post;
    vector<LogicalType> delim_types_post;
    auto join_post = make_uniq<PhysicalSIPJoin>(join_post_op, move(scan_post), move(join_knows_3), move(cond_post),
                                                JoinType::INNER, left_projection_map, right_projection_map_post,
                                                delim_types_post, 0);

    // join forum with person-person-post
    vector<idx_t> forum_ids{1, COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_forum_types{LogicalType::VARCHAR, LogicalType::BIGINT};
    string alias_forum = "f";
    vector<LogicalType> table_types_forum;
    vector<unique_ptr<Expression>> filter_forum;
    unique_ptr<LogicalGet> get_op_forum = move(
            getLogicalGet(*this, table_forum, alias_forum, table_index_forum, table_types_forum));
    unique_ptr<TableFilterSet> table_filters_forum = NULL;
    unique_ptr<PhysicalTableScan> scan_forum = make_uniq<PhysicalTableScan>(get_forum_types, get_op_forum->function,
                                                                            get_op_forum->table_index,
                                                                            move(get_op_forum->bind_data),
                                                                            table_types_forum, forum_ids,
                                                                            move(filter_forum), vector<column_t>(),
                                                                            get_op_forum->names,
                                                                            std::move(table_filters_forum),
                                                                            get_op_forum->estimated_cardinality,
                                                                            get_op_forum->extra_info);

    vector<JoinCondition> cond_forum;
    JoinCondition join_condition_forum;
    join_condition_forum.left = make_uniq<BoundReferenceExpression>("forum_rowid", LogicalType::BIGINT, 1);
    join_condition_forum.right = make_uniq<BoundReferenceExpression>("m_ps_forumid_rowid", LogicalType::BIGINT, 2);
    join_condition_forum.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_forum = make_uniq<RAIInfo>();
    rai_info_forum->rai = table_post.GetStorage().info->rais[2].get();
    rai_info_forum->rai_type = RAIType::TARGET_EDGE;
    rai_info_forum->forward = true;
    rai_info_forum->vertex = &table_forum;
    rai_info_forum->vertex_id = table_index_forum;
    rai_info_forum->passing_tables[0] = table_index_forum;
    rai_info_forum->left_cardinalities[0] = table_forum.GetStorage().info->cardinality;
    // rai_info_forum->compact_list = &rai_info_forum->rai->alist->compact_forward_list;

    join_condition_forum.rais.push_back(move(rai_info_forum));
    cond_forum.push_back(move(join_condition_forum));

    LogicalComparisonJoin join_forum_op(JoinType::INNER);
    vector<LogicalType> output_forum_types{LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT};
    join_forum_op.types = output_forum_types;
    vector<idx_t> right_projection_map_forum{3};
    vector<idx_t> merge_project_map_forum;
    vector<LogicalType> delim_types_forum;
    auto join_forum = make_uniq<PhysicalSIPJoin>(join_forum_op, move(scan_forum), move(join_post), move(cond_forum),
                                                 JoinType::INNER, left_projection_map, right_projection_map_forum,
                                                 delim_types_forum, 0);


    // join person_forum with person-person-post-forum
    idx_t p_forum_person_joindate = atoll(paras->data()[1].c_str());
    Value p_joindate = Value::BIGINT(p_forum_person_joindate);
    vector<idx_t> forum_person_ids{4, 3, 2};
    vector<LogicalType> get_forum_person_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_forum_person = "fp";
    vector<LogicalType> table_types_forum_person;
    vector<unique_ptr<Expression>> filter_forum_person;
    unique_ptr<LogicalGet> get_op_forum_person = move(
            getLogicalGet(*this, table_forum_person, alias_forum_person, table_index_forum_person,
                          table_types_forum_person));
    unique_ptr<TableFilterSet> table_filters_forum_person = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter_forum_person = duckdb::make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_GREATERTHANOREQUALTO, p_joindate);
    table_filters_forum_person->filters[2] = move(constant_filter_forum_person);
    unique_ptr<PhysicalTableScan> scan_forum_person = make_uniq<PhysicalTableScan>(get_forum_person_types,
                                                                                   get_op_forum_person->function,
                                                                                   get_op_forum_person->table_index,
                                                                                   move(get_op_forum_person->bind_data),
                                                                                   table_types_forum_person,
                                                                                   forum_person_ids,
                                                                                   move(filter_forum_person),
                                                                                   vector<column_t>(),
                                                                                   get_op_forum_person->names,
                                                                                   std::move(
                                                                                           table_filters_forum_person),
                                                                                   get_op_forum_person->estimated_cardinality,
                                                                                   get_op_forum_person->extra_info);

    vector<JoinCondition> cond_forum_person;
    JoinCondition join_condition_forum_person, join_condition_forum_person_2;
    join_condition_forum_person.left = make_uniq<BoundReferenceExpression>("fp_forumid_rowid", LogicalType::BIGINT, 0);
    join_condition_forum_person.right = make_uniq<BoundReferenceExpression>("forum_rowid", LogicalType::BIGINT, 1);
    join_condition_forum_person.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_forum_person = make_uniq<RAIInfo>();
    rai_info_forum_person->rai = table_forum_person.GetStorage().info->rais[0].get();
    rai_info_forum_person->rai_type = RAIType::EDGE_TARGET;
    rai_info_forum_person->forward = false;
    rai_info_forum_person->vertex = &table_forum;
    rai_info_forum_person->vertex_id = table_index_forum;
    rai_info_forum_person->passing_tables[0] = table_index_forum_person;
    rai_info_forum_person->left_cardinalities[0] = table_forum_person.GetStorage().info->cardinality;
    rai_info_forum_person->compact_list = &rai_info_forum_person->rai->alist->compact_backward_list;

    join_condition_forum_person.rais.push_back(move(rai_info_forum_person));

    join_condition_forum_person_2.left = make_uniq<BoundReferenceExpression>("fp_personid_rowid", LogicalType::BIGINT,
                                                                             1);
    join_condition_forum_person_2.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 2);
    join_condition_forum_person_2.comparison = ExpressionType::COMPARE_EQUAL;

    cond_forum_person.push_back(move(join_condition_forum_person));
    cond_forum_person.push_back(move(join_condition_forum_person_2));

    LogicalComparisonJoin join_forum_person_op(JoinType::INNER);
    vector<LogicalType> output_forum_person_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                                                  LogicalType::VARCHAR};
    join_forum_person_op.types = output_forum_person_types;
    vector<idx_t> right_projection_map_forum_person{0};
    vector<idx_t> merge_project_map_forum_person;
    vector<LogicalType> delim_types_forum_person;
    auto join_forum_person = make_uniq<PhysicalSIPJoin>(join_forum_person_op, move(scan_forum_person), move(join_forum),
                                                        move(cond_forum_person),
                                                        JoinType::INNER, left_projection_map,
                                                        right_projection_map_forum_person,
                                                        delim_types_forum_person, 0);


    // project
    vector<LogicalType> result_types{LogicalType::VARCHAR};
    vector<unique_ptr<Expression>> select_list;
    auto result_col0 = make_uniq<BoundReferenceExpression>("f_title", LogicalType::VARCHAR, 3);
    select_list.push_back(move(result_col0));
    auto projection = make_uniq<PhysicalProjection>(result_types, move(select_list), 0);
    projection->children.push_back(move(join_forum_person));

    return projection;
}

unique_ptr<PhysicalOperator> ClientContext::GenerateIC52PlanByPass() {
    vector<idx_t> left_projection_map, right_projection_map;

    string table_vertex_person = "person";
    string table_vertex_forum = "forum";
    string table_vertex_post = "post";
    string table_edge_knows = "knows";
    string table_edge_forum_person = "forum_person";
    idx_t table_index_person1 = 6;
    idx_t table_index_forum = 10;
    idx_t table_index_person2 = 8;
    idx_t table_index_person3 = 9;
    idx_t table_index_post = 12;
    idx_t table_index_knows = 7;
    idx_t table_index_forum_person = 13;


    auto table_or_view_person = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                  table_vertex_person, OnEntryNotFound::RETURN_NULL);
    auto &table_person = table_or_view_person->Cast<TableCatalogEntry>();

    auto table_or_view_forum = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_vertex_forum, OnEntryNotFound::RETURN_NULL);
    auto &table_forum = table_or_view_forum->Cast<TableCatalogEntry>();

    auto table_or_view_post = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                table_vertex_post, OnEntryNotFound::RETURN_NULL);
    auto &table_post = table_or_view_post->Cast<TableCatalogEntry>();

    auto table_or_view_knows = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_edge_knows, OnEntryNotFound::RETURN_NULL);
    auto &table_knows = table_or_view_knows->Cast<TableCatalogEntry>();

    auto table_or_view_forum_person = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                        table_edge_forum_person, OnEntryNotFound::RETURN_NULL);
    auto &table_forum_person = table_or_view_forum_person->Cast<TableCatalogEntry>();


    vector<idx_t> person2_ids{COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person2_types{LogicalType::BIGINT};
    string alias_person2 = "p2";
    vector<LogicalType> table_types_person2;
    vector<unique_ptr<Expression>> filter_person2;
    unique_ptr<LogicalGet> get_op_person2 = move(
            getLogicalGet(*this, table_person, alias_person2, table_index_person2, table_types_person2));
    unique_ptr<TableFilterSet> table_filters_person2 = NULL;
    unique_ptr<PhysicalTableScan> scan_person2 = make_uniq<PhysicalTableScan>(get_person2_types,
                                                                              get_op_person2->function,
                                                                              get_op_person2->table_index,
                                                                              move(get_op_person2->bind_data),
                                                                              table_types_person2, person2_ids,
                                                                              move(filter_person2), vector<column_t>(),
                                                                              get_op_person2->names,
                                                                              std::move(table_filters_person2),
                                                                              get_op_person2->estimated_cardinality,
                                                                              get_op_person2->extra_info);

    idx_t p_person_id = atoll(paras->data()[0].c_str()); // 933;
    Value p_person = Value::BIGINT(p_person_id);
    vector<idx_t> person1_ids{0, COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person1_types{LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_person1 = "p1";
    vector<LogicalType> table_types_person1;
    unique_ptr<LogicalGet> get_op_person1 = move(
            getLogicalGet(*this, table_person, alias_person1, table_index_person1, table_types_person1));
    vector<unique_ptr<Expression>> filter_person1;
    unique_ptr<TableFilterSet> table_filters_person1 = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter = duckdb::make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL,
                                                                                   p_person);
    table_filters_person1->filters[0] = move(constant_filter);
    unique_ptr<PhysicalTableScan> scan_person1 = make_uniq<PhysicalTableScan>(get_person1_types,
                                                                              get_op_person1->function,
                                                                              get_op_person1->table_index,
                                                                              move(get_op_person1->bind_data),
                                                                              table_types_person1, person1_ids,
                                                                              move(filter_person1), vector<column_t>(),
                                                                              get_op_person1->names,
                                                                              std::move(table_filters_person1),
                                                                              get_op_person1->estimated_cardinality,
                                                                              get_op_person1->extra_info);

    vector<JoinCondition> cond_knows;
    JoinCondition join_condition_knows;
    join_condition_knows.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 1);
    join_condition_knows.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows = make_uniq<RAIInfo>();
    rai_info_knows->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows->forward = true;
    rai_info_knows->vertex = &table_person;
    rai_info_knows->vertex_id = table_index_person2;
    rai_info_knows->passing_tables[0] = table_index_person2;
    rai_info_knows->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows.rais.push_back(move(rai_info_knows));
    cond_knows.push_back(move(join_condition_knows));

    LogicalComparisonJoin join_knows_op(JoinType::INNER);
    vector<LogicalType> output_knows_types{LogicalType::BIGINT, LogicalType::BIGINT};
    join_knows_op.types = output_knows_types;
    vector<idx_t> right_projection_map_knows{1};
    vector<idx_t> merge_project_map;
    vector<LogicalType> delim_types;
    auto join_knows = make_uniq<PhysicalMergeSIPJoin>(join_knows_op, move(scan_person2), move(scan_person1),
                                                      move(cond_knows),
                                                      JoinType::INNER, left_projection_map, right_projection_map_knows,
                                                      merge_project_map, delim_types, 0);

    // join the 2-hop neighbors
    vector<idx_t> person3_ids{COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person3_types{LogicalType::BIGINT};
    string alias_person3 = "p3";
    vector<LogicalType> table_types_person3;
    vector<unique_ptr<Expression>> filter_person3;
    unique_ptr<LogicalGet> get_op_person3 = move(
            getLogicalGet(*this, table_person, alias_person3, table_index_person3, table_types_person3));
    unique_ptr<TableFilterSet> table_filters_person3 = NULL;
    unique_ptr<PhysicalTableScan> scan_person3 = make_uniq<PhysicalTableScan>(get_person3_types,
                                                                              get_op_person3->function,
                                                                              get_op_person3->table_index,
                                                                              move(get_op_person3->bind_data),
                                                                              table_types_person3, person3_ids,
                                                                              move(filter_person3), vector<column_t>(),
                                                                              get_op_person3->names,
                                                                              std::move(table_filters_person3),
                                                                              get_op_person3->estimated_cardinality,
                                                                              get_op_person3->extra_info);

    vector<JoinCondition> cond_knows_2;
    JoinCondition join_condition_knows_2;
    join_condition_knows_2.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows_2.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows_2.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows_2 = make_uniq<RAIInfo>();
    rai_info_knows_2->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows_2->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows_2->forward = true;
    rai_info_knows_2->vertex = &table_person;
    rai_info_knows_2->vertex_id = table_index_person3;
    rai_info_knows_2->passing_tables[0] = table_index_person3;
    rai_info_knows_2->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows_2.rais.push_back(move(rai_info_knows_2));
    cond_knows_2.push_back(move(join_condition_knows_2));

    LogicalComparisonJoin join_knows_op_2(JoinType::INNER);
    vector<LogicalType> output_knows_types_2{LogicalType::BIGINT, LogicalType::BIGINT};
    join_knows_op_2.types = output_knows_types_2;
    vector<idx_t> right_projection_map_knows_2{0};
    vector<idx_t> merge_project_map_2;
    vector<LogicalType> delim_types_2;
    auto join_knows_2 = make_uniq<PhysicalMergeSIPJoin>(join_knows_op_2, move(scan_person3), move(join_knows),
                                                        move(cond_knows_2),
                                                        JoinType::INNER, left_projection_map,
                                                        right_projection_map_knows_2,
                                                        merge_project_map_2, delim_types_2, 0);

    // join person_forum with person-person
    idx_t p_forum_person_joindate = atoll(paras->data()[1].c_str());
    Value p_joindate = Value::BIGINT(p_forum_person_joindate);
    vector<idx_t> forum_person_ids{4, 3, 2};
    vector<LogicalType> get_forum_person_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_forum_person = "fp";
    vector<LogicalType> table_types_forum_person;
    vector<unique_ptr<Expression>> filter_forum_person;
    unique_ptr<LogicalGet> get_op_forum_person = move(
            getLogicalGet(*this, table_forum_person, alias_forum_person, table_index_forum_person,
                          table_types_forum_person));
    unique_ptr<TableFilterSet> table_filters_forum_person = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter_forum_person = duckdb::make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_GREATERTHANOREQUALTO, p_joindate);
    table_filters_forum_person->filters[2] = move(constant_filter_forum_person);
    unique_ptr<PhysicalTableScan> scan_forum_person = make_uniq<PhysicalTableScan>(get_forum_person_types,
                                                                                   get_op_forum_person->function,
                                                                                   get_op_forum_person->table_index,
                                                                                   move(get_op_forum_person->bind_data),
                                                                                   table_types_forum_person,
                                                                                   forum_person_ids,
                                                                                   move(filter_forum_person),
                                                                                   vector<column_t>(),
                                                                                   get_op_forum_person->names,
                                                                                   std::move(
                                                                                           table_filters_forum_person),
                                                                                   get_op_forum_person->estimated_cardinality,
                                                                                   get_op_forum_person->extra_info);

    vector<JoinCondition> cond_forum_person;
    JoinCondition join_condition_forum_person;
    join_condition_forum_person.left = make_uniq<BoundReferenceExpression>("fp_personid_rowid", LogicalType::BIGINT, 1);
    join_condition_forum_person.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_forum_person.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_forum_person = make_uniq<RAIInfo>();
    rai_info_forum_person->rai = table_forum_person.GetStorage().info->rais[0].get();
    rai_info_forum_person->rai_type = RAIType::EDGE_SOURCE;
    rai_info_forum_person->forward = true;
    rai_info_forum_person->vertex = &table_person;
    rai_info_forum_person->vertex_id = table_index_person3;
    rai_info_forum_person->passing_tables[0] = table_index_forum_person;
    rai_info_forum_person->left_cardinalities[0] = table_forum_person.GetStorage().info->cardinality;
    rai_info_forum_person->compact_list = &rai_info_forum_person->rai->alist->compact_forward_list;

    join_condition_forum_person.rais.push_back(move(rai_info_forum_person));

    cond_forum_person.push_back(move(join_condition_forum_person));

    LogicalComparisonJoin join_forum_person_op(JoinType::INNER);
    vector<LogicalType> output_forum_person_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                                                  LogicalType::BIGINT};
    join_forum_person_op.types = output_forum_person_types;
    vector<idx_t> right_projection_map_forum_person{0};
    vector<idx_t> merge_project_map_forum_person;
    vector<LogicalType> delim_types_forum_person;
    auto join_forum_person = make_uniq<PhysicalSIPJoin>(join_forum_person_op, move(scan_forum_person), move(join_knows_2),
                                                        move(cond_forum_person),
                                                        JoinType::INNER, left_projection_map,
                                                        right_projection_map_forum_person,
                                                        delim_types_forum_person, 0);

    // join forum with person-person-fp
    vector<idx_t> forum_ids{1, COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_forum_types{LogicalType::VARCHAR, LogicalType::BIGINT};
    string alias_forum = "f";
    vector<LogicalType> table_types_forum;
    vector<unique_ptr<Expression>> filter_forum;
    unique_ptr<LogicalGet> get_op_forum = move(
            getLogicalGet(*this, table_forum, alias_forum, table_index_forum, table_types_forum));
    unique_ptr<TableFilterSet> table_filters_forum = NULL;
    unique_ptr<PhysicalTableScan> scan_forum = make_uniq<PhysicalTableScan>(get_forum_types, get_op_forum->function,
                                                                            get_op_forum->table_index,
                                                                            move(get_op_forum->bind_data),
                                                                            table_types_forum, forum_ids,
                                                                            move(filter_forum), vector<column_t>(),
                                                                            get_op_forum->names,
                                                                            std::move(table_filters_forum),
                                                                            get_op_forum->estimated_cardinality,
                                                                            get_op_forum->extra_info);

    vector<JoinCondition> cond_forum;
    JoinCondition join_condition_forum;
    join_condition_forum.left = make_uniq<BoundReferenceExpression>("forum_rowid", LogicalType::BIGINT, 1);
    join_condition_forum.right = make_uniq<BoundReferenceExpression>("fp_forumid_rowid", LogicalType::BIGINT, 0);
    join_condition_forum.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_forum = make_uniq<RAIInfo>();
    rai_info_forum->rai = table_forum_person.GetStorage().info->rais[0].get();
    rai_info_forum->rai_type = RAIType::TARGET_EDGE;
    rai_info_forum->forward = true;
    rai_info_forum->vertex = &table_forum;
    rai_info_forum->vertex_id = table_index_forum;
    rai_info_forum->passing_tables[0] = table_index_forum;
    rai_info_forum->left_cardinalities[0] = table_forum.GetStorage().info->cardinality;
    // rai_info_forum->compact_list = &rai_info_forum->rai->alist->compact_forward_list;

    join_condition_forum.rais.push_back(move(rai_info_forum));
    cond_forum.push_back(move(join_condition_forum));

    LogicalComparisonJoin join_forum_op(JoinType::INNER);
    vector<LogicalType> output_forum_types{LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT};
    join_forum_op.types = output_forum_types;
    vector<idx_t> right_projection_map_forum{3};
    vector<idx_t> merge_project_map_forum;
    vector<LogicalType> delim_types_forum;
    auto join_forum = make_uniq<PhysicalSIPJoin>(join_forum_op, move(scan_forum), move(join_forum_person), move(cond_forum),
                                                 JoinType::INNER, left_projection_map, right_projection_map_forum,
                                                 delim_types_forum, 0);

    // join post with person-person-forum
    vector<idx_t> post_ids{11, COLUMN_IDENTIFIER_ROW_ID, 13};
    vector<LogicalType> get_post_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_post = "m";
    vector<LogicalType> table_types_post;
    vector<unique_ptr<Expression>> filter_post;
    unique_ptr<LogicalGet> get_op_post = move(
            getLogicalGet(*this, table_post, alias_post, table_index_post, table_types_post));
    unique_ptr<TableFilterSet> table_filters_post = NULL;
    unique_ptr<PhysicalTableScan> scan_post = make_uniq<PhysicalTableScan>(get_post_types, get_op_post->function,
                                                                           get_op_post->table_index,
                                                                           move(get_op_post->bind_data),
                                                                           table_types_post, post_ids,
                                                                           move(filter_post), vector<column_t>(),
                                                                           get_op_post->names,
                                                                           std::move(table_filters_post),
                                                                           get_op_post->estimated_cardinality,
                                                                           get_op_post->extra_info);

    vector<JoinCondition> cond_post;
    JoinCondition join_condition_post, join_condition_post_2;
    join_condition_post.left = make_uniq<BoundReferenceExpression>("m_ps_forumid_rowid", LogicalType::BIGINT, 2);
    join_condition_post.right = make_uniq<BoundReferenceExpression>("forum_rowid", LogicalType::BIGINT, 1);
    join_condition_post.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_post = make_uniq<RAIInfo>();
    rai_info_post->rai = table_post.GetStorage().info->rais[2].get();
    rai_info_post->rai_type = RAIType::EDGE_SOURCE;
    rai_info_post->forward = true;
    rai_info_post->vertex = &table_forum;
    rai_info_post->vertex_id = table_index_forum;
    rai_info_post->passing_tables[0] = table_index_post;
    rai_info_post->left_cardinalities[0] = table_post.GetStorage().info->cardinality;
    rai_info_post->compact_list = &rai_info_post->rai->alist->compact_forward_list;

    join_condition_post_2.left = make_uniq<BoundReferenceExpression>("m_creatorid_rowid", LogicalType::BIGINT, 0);
    join_condition_post_2.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 2);
    join_condition_post_2.comparison = ExpressionType::COMPARE_EQUAL;

    join_condition_post.rais.push_back(move(rai_info_post));
    cond_post.push_back(move(join_condition_post));
    cond_post.push_back(move(join_condition_post_2));

    LogicalComparisonJoin join_post_op(JoinType::INNER);
    vector<LogicalType> output_post_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                                          LogicalType::VARCHAR};
    join_post_op.types = output_post_types;
    vector<idx_t> right_projection_map_post{0};
    vector<idx_t> merge_project_map_post;
    vector<LogicalType> delim_types_post;
    auto join_post = make_uniq<PhysicalSIPJoin>(join_post_op, move(scan_post), move(join_forum), move(cond_post),
                                                JoinType::INNER, left_projection_map, right_projection_map_post,
                                                delim_types_post, 0);



    // project
    vector<LogicalType> result_types{LogicalType::VARCHAR};
    vector<unique_ptr<Expression>> select_list;
    auto result_col0 = make_uniq<BoundReferenceExpression>("f_title", LogicalType::VARCHAR, 3);
    select_list.push_back(move(result_col0));
    auto projection = make_uniq<PhysicalProjection>(result_types, move(select_list), 0);
    projection->children.push_back(move(join_post));

    return projection;
}

unique_ptr<PhysicalOperator> ClientContext::GenerateIC52PlanByPassFromPerson() {
    vector<idx_t> left_projection_map, right_projection_map;

    string table_vertex_person = "person";
    string table_vertex_forum = "forum";
    string table_vertex_post = "post";
    string table_edge_knows = "knows";
    string table_edge_forum_person = "forum_person";
    idx_t table_index_person1 = 6;
    idx_t table_index_forum = 10;
    idx_t table_index_person2 = 8;
    idx_t table_index_person3 = 9;
    idx_t table_index_post = 12;
    idx_t table_index_knows = 7;
    idx_t table_index_forum_person = 13;


    auto table_or_view_person = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                  table_vertex_person, OnEntryNotFound::RETURN_NULL);
    auto &table_person = table_or_view_person->Cast<TableCatalogEntry>();

    auto table_or_view_forum = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_vertex_forum, OnEntryNotFound::RETURN_NULL);
    auto &table_forum = table_or_view_forum->Cast<TableCatalogEntry>();

    auto table_or_view_post = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                table_vertex_post, OnEntryNotFound::RETURN_NULL);
    auto &table_post = table_or_view_post->Cast<TableCatalogEntry>();

    auto table_or_view_knows = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                 table_edge_knows, OnEntryNotFound::RETURN_NULL);
    auto &table_knows = table_or_view_knows->Cast<TableCatalogEntry>();

    auto table_or_view_forum_person = Catalog::GetEntry(*this, CatalogType::TABLE_ENTRY, "", "",
                                                        table_edge_forum_person, OnEntryNotFound::RETURN_NULL);
    auto &table_forum_person = table_or_view_forum_person->Cast<TableCatalogEntry>();


    vector<idx_t> person2_ids{COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person2_types{LogicalType::BIGINT};
    string alias_person2 = "p2";
    vector<LogicalType> table_types_person2;
    vector<unique_ptr<Expression>> filter_person2;
    unique_ptr<LogicalGet> get_op_person2 = move(
            getLogicalGet(*this, table_person, alias_person2, table_index_person2, table_types_person2));
    unique_ptr<TableFilterSet> table_filters_person2 = NULL;
    unique_ptr<PhysicalTableScan> scan_person2 = make_uniq<PhysicalTableScan>(get_person2_types,
                                                                              get_op_person2->function,
                                                                              get_op_person2->table_index,
                                                                              move(get_op_person2->bind_data),
                                                                              table_types_person2, person2_ids,
                                                                              move(filter_person2), vector<column_t>(),
                                                                              get_op_person2->names,
                                                                              std::move(table_filters_person2),
                                                                              get_op_person2->estimated_cardinality,
                                                                              get_op_person2->extra_info);

    idx_t p_person_id = atoll(paras->data()[0].c_str()); // 933;
    Value p_person = Value::BIGINT(p_person_id);
    vector<idx_t> person1_ids{0, COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person1_types{LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_person1 = "p1";
    vector<LogicalType> table_types_person1;
    unique_ptr<LogicalGet> get_op_person1 = move(
            getLogicalGet(*this, table_person, alias_person1, table_index_person1, table_types_person1));
    vector<unique_ptr<Expression>> filter_person1;
    unique_ptr<TableFilterSet> table_filters_person1 = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter = duckdb::make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL,
                                                                                   p_person);
    table_filters_person1->filters[0] = move(constant_filter);
    unique_ptr<PhysicalTableScan> scan_person1 = make_uniq<PhysicalTableScan>(get_person1_types,
                                                                              get_op_person1->function,
                                                                              get_op_person1->table_index,
                                                                              move(get_op_person1->bind_data),
                                                                              table_types_person1, person1_ids,
                                                                              move(filter_person1), vector<column_t>(),
                                                                              get_op_person1->names,
                                                                              std::move(table_filters_person1),
                                                                              get_op_person1->estimated_cardinality,
                                                                              get_op_person1->extra_info);

    vector<JoinCondition> cond_knows;
    JoinCondition join_condition_knows;
    join_condition_knows.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 1);
    join_condition_knows.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows = make_uniq<RAIInfo>();
    rai_info_knows->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows->forward = true;
    rai_info_knows->vertex = &table_person;
    rai_info_knows->vertex_id = table_index_person2;
    rai_info_knows->passing_tables[0] = table_index_person2;
    rai_info_knows->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows.rais.push_back(move(rai_info_knows));
    cond_knows.push_back(move(join_condition_knows));

    LogicalComparisonJoin join_knows_op(JoinType::INNER);
    vector<LogicalType> output_knows_types{LogicalType::BIGINT, LogicalType::BIGINT};
    join_knows_op.types = output_knows_types;
    vector<idx_t> right_projection_map_knows{1};
    vector<idx_t> merge_project_map;
    vector<LogicalType> delim_types;
    auto join_knows = make_uniq<PhysicalMergeSIPJoin>(join_knows_op, move(scan_person2), move(scan_person1),
                                                      move(cond_knows),
                                                      JoinType::INNER, left_projection_map, right_projection_map_knows,
                                                      merge_project_map, delim_types, 0);

    // join the 2-hop neighbors
    vector<idx_t> person3_ids{COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_person3_types{LogicalType::BIGINT};
    string alias_person3 = "p3";
    vector<LogicalType> table_types_person3;
    vector<unique_ptr<Expression>> filter_person3;
    unique_ptr<LogicalGet> get_op_person3 = move(
            getLogicalGet(*this, table_person, alias_person3, table_index_person3, table_types_person3));
    unique_ptr<TableFilterSet> table_filters_person3 = NULL;
    unique_ptr<PhysicalTableScan> scan_person3 = make_uniq<PhysicalTableScan>(get_person3_types,
                                                                              get_op_person3->function,
                                                                              get_op_person3->table_index,
                                                                              move(get_op_person3->bind_data),
                                                                              table_types_person3, person3_ids,
                                                                              move(filter_person3), vector<column_t>(),
                                                                              get_op_person3->names,
                                                                              std::move(table_filters_person3),
                                                                              get_op_person3->estimated_cardinality,
                                                                              get_op_person3->extra_info);

    vector<JoinCondition> cond_knows_2;
    JoinCondition join_condition_knows_2;
    join_condition_knows_2.left = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows_2.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_knows_2.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_knows_2 = make_uniq<RAIInfo>();
    rai_info_knows_2->rai = table_knows.GetStorage().info->rais[0].get();
    rai_info_knows_2->rai_type = RAIType::TARGET_EDGE;
    rai_info_knows_2->forward = true;
    rai_info_knows_2->vertex = &table_person;
    rai_info_knows_2->vertex_id = table_index_person3;
    rai_info_knows_2->passing_tables[0] = table_index_person3;
    rai_info_knows_2->left_cardinalities[0] = table_person.GetStorage().info->cardinality;
    // rai_info_knows->compact_list = &rai_info_knows->rai->alist->compact_backward_list;

    join_condition_knows_2.rais.push_back(move(rai_info_knows_2));
    cond_knows_2.push_back(move(join_condition_knows_2));

    LogicalComparisonJoin join_knows_op_2(JoinType::INNER);
    vector<LogicalType> output_knows_types_2{LogicalType::BIGINT, LogicalType::BIGINT};
    join_knows_op_2.types = output_knows_types_2;
    vector<idx_t> right_projection_map_knows_2{0};
    vector<idx_t> merge_project_map_2;
    vector<LogicalType> delim_types_2;
    auto join_knows_2 = make_uniq<PhysicalMergeSIPJoin>(join_knows_op_2, move(scan_person3), move(join_knows),
                                                        move(cond_knows_2),
                                                        JoinType::INNER, left_projection_map,
                                                        right_projection_map_knows_2,
                                                        merge_project_map_2, delim_types_2, 0);

    // join person_forum with person-person
    idx_t p_forum_person_joindate = atoll(paras->data()[1].c_str());
    Value p_joindate = Value::BIGINT(p_forum_person_joindate);
    vector<idx_t> forum_person_ids{4, 3, 2};
    vector<LogicalType> get_forum_person_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_forum_person = "fp";
    vector<LogicalType> table_types_forum_person;
    vector<unique_ptr<Expression>> filter_forum_person;
    unique_ptr<LogicalGet> get_op_forum_person = move(
            getLogicalGet(*this, table_forum_person, alias_forum_person, table_index_forum_person,
                          table_types_forum_person));
    unique_ptr<TableFilterSet> table_filters_forum_person = make_uniq<TableFilterSet>();
    unique_ptr<ConstantFilter> constant_filter_forum_person = duckdb::make_uniq<ConstantFilter>(
            ExpressionType::COMPARE_GREATERTHANOREQUALTO, p_joindate);
    table_filters_forum_person->filters[2] = move(constant_filter_forum_person);
    unique_ptr<PhysicalTableScan> scan_forum_person = make_uniq<PhysicalTableScan>(get_forum_person_types,
                                                                                   get_op_forum_person->function,
                                                                                   get_op_forum_person->table_index,
                                                                                   move(get_op_forum_person->bind_data),
                                                                                   table_types_forum_person,
                                                                                   forum_person_ids,
                                                                                   move(filter_forum_person),
                                                                                   vector<column_t>(),
                                                                                   get_op_forum_person->names,
                                                                                   std::move(
                                                                                           table_filters_forum_person),
                                                                                   get_op_forum_person->estimated_cardinality,
                                                                                   get_op_forum_person->extra_info);

    vector<JoinCondition> cond_forum_person;
    JoinCondition join_condition_forum_person;
    join_condition_forum_person.left = make_uniq<BoundReferenceExpression>("fp_personid_rowid", LogicalType::BIGINT, 1);
    join_condition_forum_person.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 0);
    join_condition_forum_person.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_forum_person = make_uniq<RAIInfo>();
    rai_info_forum_person->rai = table_forum_person.GetStorage().info->rais[0].get();
    rai_info_forum_person->rai_type = RAIType::EDGE_SOURCE;
    rai_info_forum_person->forward = true;
    rai_info_forum_person->vertex = &table_person;
    rai_info_forum_person->vertex_id = table_index_person3;
    rai_info_forum_person->passing_tables[0] = table_index_forum_person;
    rai_info_forum_person->left_cardinalities[0] = table_forum_person.GetStorage().info->cardinality;
    rai_info_forum_person->compact_list = &rai_info_forum_person->rai->alist->compact_forward_list;

    join_condition_forum_person.rais.push_back(move(rai_info_forum_person));

    cond_forum_person.push_back(move(join_condition_forum_person));

    LogicalComparisonJoin join_forum_person_op(JoinType::INNER);
    vector<LogicalType> output_forum_person_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                                                  LogicalType::BIGINT};
    join_forum_person_op.types = output_forum_person_types;
    vector<idx_t> right_projection_map_forum_person{0};
    vector<idx_t> merge_project_map_forum_person;
    vector<LogicalType> delim_types_forum_person;
    auto join_forum_person = make_uniq<PhysicalSIPJoin>(join_forum_person_op, move(scan_forum_person),
                                                        move(join_knows_2),
                                                        move(cond_forum_person),
                                                        JoinType::INNER, left_projection_map,
                                                        right_projection_map_forum_person,
                                                        delim_types_forum_person, 0);

    // join forum with person-person-fp
    vector<idx_t> forum_ids{1, COLUMN_IDENTIFIER_ROW_ID};
    vector<LogicalType> get_forum_types{LogicalType::VARCHAR, LogicalType::BIGINT};
    string alias_forum = "f";
    vector<LogicalType> table_types_forum;
    vector<unique_ptr<Expression>> filter_forum;
    unique_ptr<LogicalGet> get_op_forum = move(
            getLogicalGet(*this, table_forum, alias_forum, table_index_forum, table_types_forum));
    unique_ptr<TableFilterSet> table_filters_forum = NULL;
    unique_ptr<PhysicalTableScan> scan_forum = make_uniq<PhysicalTableScan>(get_forum_types, get_op_forum->function,
                                                                            get_op_forum->table_index,
                                                                            move(get_op_forum->bind_data),
                                                                            table_types_forum, forum_ids,
                                                                            move(filter_forum), vector<column_t>(),
                                                                            get_op_forum->names,
                                                                            std::move(table_filters_forum),
                                                                            get_op_forum->estimated_cardinality,
                                                                            get_op_forum->extra_info);

    vector<JoinCondition> cond_forum;
    JoinCondition join_condition_forum;
    join_condition_forum.left = make_uniq<BoundReferenceExpression>("forum_rowid", LogicalType::BIGINT, 1);
    join_condition_forum.right = make_uniq<BoundReferenceExpression>("fp_forumid_rowid", LogicalType::BIGINT, 0);
    join_condition_forum.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_forum = make_uniq<RAIInfo>();
    rai_info_forum->rai = table_forum_person.GetStorage().info->rais[0].get();
    rai_info_forum->rai_type = RAIType::TARGET_EDGE;
    rai_info_forum->forward = true;
    rai_info_forum->vertex = &table_forum;
    rai_info_forum->vertex_id = table_index_forum;
    rai_info_forum->passing_tables[0] = table_index_forum;
    rai_info_forum->left_cardinalities[0] = table_forum.GetStorage().info->cardinality;
    // rai_info_forum->compact_list = &rai_info_forum->rai->alist->compact_forward_list;

    join_condition_forum.rais.push_back(move(rai_info_forum));
    cond_forum.push_back(move(join_condition_forum));

    LogicalComparisonJoin join_forum_op(JoinType::INNER);
    vector<LogicalType> output_forum_types{LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT};
    join_forum_op.types = output_forum_types;
    vector<idx_t> right_projection_map_forum{3};
    vector<idx_t> merge_project_map_forum;
    vector<LogicalType> delim_types_forum;
    auto join_forum = make_uniq<PhysicalSIPJoin>(join_forum_op, move(scan_forum), move(join_forum_person),
                                                 move(cond_forum),
                                                 JoinType::INNER, left_projection_map, right_projection_map_forum,
                                                 delim_types_forum, 0);

    // join post with person-person-forum
    vector<idx_t> post_ids{11, COLUMN_IDENTIFIER_ROW_ID, 13};
    vector<LogicalType> get_post_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
    string alias_post = "m";
    vector<LogicalType> table_types_post;
    vector<unique_ptr<Expression>> filter_post;
    unique_ptr<LogicalGet> get_op_post = move(
            getLogicalGet(*this, table_post, alias_post, table_index_post, table_types_post));
    unique_ptr<TableFilterSet> table_filters_post = NULL;
    unique_ptr<PhysicalTableScan> scan_post = make_uniq<PhysicalTableScan>(get_post_types, get_op_post->function,
                                                                           get_op_post->table_index,
                                                                           move(get_op_post->bind_data),
                                                                           table_types_post, post_ids,
                                                                           move(filter_post), vector<column_t>(),
                                                                           get_op_post->names,
                                                                           std::move(table_filters_post),
                                                                           get_op_post->estimated_cardinality,
                                                                           get_op_post->extra_info);

    vector<JoinCondition> cond_post;
    JoinCondition join_condition_post, join_condition_post_2;
    join_condition_post.left = make_uniq<BoundReferenceExpression>("m_creatorid_rowid", LogicalType::BIGINT, 0);
    join_condition_post.right = make_uniq<BoundReferenceExpression>("person_rowid", LogicalType::BIGINT, 2);
    join_condition_post.comparison = ExpressionType::COMPARE_EQUAL;

    auto rai_info_post = make_uniq<RAIInfo>();
    rai_info_post->rai = table_post.GetStorage().info->rais[0].get();
    rai_info_post->rai_type = RAIType::EDGE_SOURCE;
    rai_info_post->forward = true;
    rai_info_post->vertex = &table_person;
    rai_info_post->vertex_id = table_index_person3;
    rai_info_post->passing_tables[0] = table_index_post;
    rai_info_post->left_cardinalities[0] = table_post.GetStorage().info->cardinality;
    rai_info_post->compact_list = &rai_info_post->rai->alist->compact_forward_list;

    join_condition_post_2.left = make_uniq<BoundReferenceExpression>("m_ps_forumid_rowid", LogicalType::BIGINT, 2);
    join_condition_post_2.right = make_uniq<BoundReferenceExpression>("forum_rowid", LogicalType::BIGINT, 1);
    join_condition_post_2.comparison = ExpressionType::COMPARE_EQUAL;

    join_condition_post.rais.push_back(move(rai_info_post));
    cond_post.push_back(move(join_condition_post));
    cond_post.push_back(move(join_condition_post_2));

    LogicalComparisonJoin join_post_op(JoinType::INNER);
    vector<LogicalType> output_post_types{LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                                          LogicalType::VARCHAR};
    join_post_op.types = output_post_types;
    vector<idx_t> right_projection_map_post{0};
    vector<idx_t> merge_project_map_post;
    vector<LogicalType> delim_types_post;
    auto join_post = make_uniq<PhysicalSIPJoin>(join_post_op, move(scan_post), move(join_forum), move(cond_post),
                                                JoinType::INNER, left_projection_map, right_projection_map_post,
                                                delim_types_post, 0);



    // project
    vector<LogicalType> result_types{LogicalType::VARCHAR};
    vector<unique_ptr<Expression>> select_list;
    auto result_col0 = make_uniq<BoundReferenceExpression>("f_title", LogicalType::VARCHAR, 3);
    select_list.push_back(move(result_col0));
    auto projection = make_uniq<PhysicalProjection>(result_types, move(select_list), 0);
    projection->children.push_back(move(join_post));

    return projection;
}

} // namespace duckdb
