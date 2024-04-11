//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/common/enums/pending_execution_result.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/table_description.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/external_dependencies.hpp"
#include "duckdb/common/preserved_error.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/main/protobuf_serializer.hpp"

namespace duckdb {
class Appender;
class Catalog;
class CatalogSearchPath;
class ColumnDataCollection;
class DatabaseInstance;
class FileOpener;
class LogicalOperator;
class PreparedStatementData;
class Relation;
class BufferedFileWriter;
class QueryProfiler;
class ClientContextLock;
struct CreateScalarFunctionInfo;
class ScalarFunctionCatalogEntry;
struct ActiveQueryContext;
struct ParserOptions;
struct ClientData;

struct PendingQueryParameters {
	//! Prepared statement parameters (if any)
	optional_ptr<case_insensitive_map_t<Value>> parameters;
	//! Whether or not a stream result should be allowed
	bool allow_stream_result = false;
};

//! ClientContextState is virtual base class for ClientContext-local (or Query-Local, using QueryEnd callback) state
//! e.g. caches that need to live as long as a ClientContext or Query.
class ClientContextState {
public:
	virtual ~ClientContextState() {};
	virtual void QueryEnd() = 0;
};

//! The ClientContext holds information relevant to the current client session
//! during execution
class ClientContext : public std::enable_shared_from_this<ClientContext> {
	friend class PendingQueryResult;
	friend class StreamQueryResult;
	friend class DuckTransactionManager;

public:
	DUCKDB_API explicit ClientContext(shared_ptr<DatabaseInstance> db, int sql_mode_input=0, string pb_file_input="pb_output.log");
	DUCKDB_API ~ClientContext();

    //! Protobuf Converter
    PbSerializer pb_serializer;
	//! The database that this client is connected to
	shared_ptr<DatabaseInstance> db;
	//! Whether or not the query is interrupted
	atomic<bool> interrupted;
	//! External Objects (e.g., Python objects) that views depend of
	unordered_map<string, vector<shared_ptr<ExternalDependency>>> external_dependencies;
	//! Set of optional states (e.g. Caches) that can be held by the ClientContext
	unordered_map<string, shared_ptr<ClientContextState>> registered_state;
	//! The client configuration
	ClientConfig config;
	//! The set of client-specific data
	unique_ptr<ClientData> client_data;
	//! Data for the currently running transaction
	TransactionContext transaction;

public:
	MetaTransaction &ActiveTransaction() {
		return transaction.ActiveTransaction();
	}

	//! Interrupt execution of a query
	DUCKDB_API void Interrupt();
	//! Enable query profiling
	DUCKDB_API void EnableProfiling();
	//! Disable query profiling
	DUCKDB_API void DisableProfiling();

	//! Issue a query, returning a QueryResult. The QueryResult can be either a StreamQueryResult or a
	//! MaterializedQueryResult. The StreamQueryResult will only be returned in the case of a successful SELECT
	//! statement.
	DUCKDB_API unique_ptr<QueryResult> Query(const string &query, bool allow_stream_result);
	DUCKDB_API unique_ptr<QueryResult> Query(unique_ptr<SQLStatement> statement, bool allow_stream_result);

	//! Issues a query to the database and returns a Pending Query Result. Note that "query" may only contain
	//! a single statement.
	DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(const string &query, bool allow_stream_result);
	//! Issues a query to the database and returns a Pending Query Result
	DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(unique_ptr<SQLStatement> statement,
	                                                       bool allow_stream_result);

	//! Destroy the client context
	DUCKDB_API void Destroy();

	//! Get the table info of a specific table, or nullptr if it cannot be found
	DUCKDB_API unique_ptr<TableDescription> TableInfo(const string &schema_name, const string &table_name);
	//! Appends a DataChunk to the specified table. Returns whether or not the append was successful.
	DUCKDB_API void Append(TableDescription &description, ColumnDataCollection &collection);
	//! Try to bind a relation in the current client context; either throws an exception or fills the result_columns
	//! list with the set of returned columns
	DUCKDB_API void TryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns);

	//! Execute a relation
	DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(const shared_ptr<Relation> &relation,
	                                                       bool allow_stream_result);
	DUCKDB_API unique_ptr<QueryResult> Execute(const shared_ptr<Relation> &relation);

	//! Prepare a query
	DUCKDB_API unique_ptr<PreparedStatement> Prepare(const string &query);
	//! Directly prepare a SQL statement
	DUCKDB_API unique_ptr<PreparedStatement> Prepare(unique_ptr<SQLStatement> statement);

	//! Create a pending query result from a prepared statement with the given name and set of parameters
	//! It is possible that the prepared statement will be re-bound. This will generally happen if the catalog is
	//! modified in between the prepared statement being bound and the prepared statement being run.
	DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(const string &query,
	                                                       shared_ptr<PreparedStatementData> &prepared,
	                                                       const PendingQueryParameters &parameters);

	//! Execute a prepared statement with the given name and set of parameters
	//! It is possible that the prepared statement will be re-bound. This will generally happen if the catalog is
	//! modified in between the prepared statement being bound and the prepared statement being run.
	DUCKDB_API unique_ptr<QueryResult> Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
	                                           case_insensitive_map_t<Value> &values, bool allow_stream_result = true);
	DUCKDB_API unique_ptr<QueryResult> Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
	                                           const PendingQueryParameters &parameters);

	//! Gets current percentage of the query's progress, returns 0 in case the progress bar is disabled.
	DUCKDB_API double GetProgress();

	//! Register function in the temporary schema
	DUCKDB_API void RegisterFunction(CreateFunctionInfo &info);

	//! Parse statements from a query
	DUCKDB_API vector<unique_ptr<SQLStatement>> ParseStatements(const string &query);

	//! Extract the logical plan of a query
	DUCKDB_API unique_ptr<LogicalOperator> ExtractPlan(const string &query);
	DUCKDB_API void HandlePragmaStatements(vector<unique_ptr<SQLStatement>> &statements);

	//! Runs a function with a valid transaction context, potentially starting a transaction if the context is in auto
	//! commit mode.
	DUCKDB_API void RunFunctionInTransaction(const std::function<void(void)> &fun,
	                                         bool requires_valid_transaction = true);
	//! Same as RunFunctionInTransaction, but does not obtain a lock on the client context or check for validation
	DUCKDB_API void RunFunctionInTransactionInternal(ClientContextLock &lock, const std::function<void(void)> &fun,
	                                                 bool requires_valid_transaction = true);

	//! Equivalent to CURRENT_SETTING(key) SQL function.
	DUCKDB_API bool TryGetCurrentSetting(const std::string &key, Value &result);

	//! Returns the parser options for this client context
	DUCKDB_API ParserOptions GetParserOptions() const;

	DUCKDB_API unique_ptr<DataChunk> Fetch(ClientContextLock &lock, StreamQueryResult &result);

	//! Whether or not the given result object (streaming query result or pending query result) is active
	DUCKDB_API bool IsActiveResult(ClientContextLock &lock, BaseQueryResult *result);

	//! Returns the current executor
	Executor &GetExecutor();

	//! Returns the current query string (if any)
	const string &GetCurrentQuery();

	//! Fetch a list of table names that are required for a given query
	DUCKDB_API unordered_set<string> GetTableNames(const string &query);

	DUCKDB_API ClientProperties GetClientProperties() const;

	//! Returns true if execution of the current query is finished
	DUCKDB_API bool ExecutionIsFinished();

    DUCKDB_API void SetPbParameters(int sql_mode_input=0, string pb_file_input="pb_output.log", unique_ptr<std::vector<string>> paras = NULL);

public:
// ic query
    unique_ptr<PhysicalOperator> GenerateIC11Plan();
    unique_ptr<PhysicalOperator> GenerateIC11PlanGLogue();
    unique_ptr<PhysicalOperator> GenerateIC11PlanCalcite();
    unique_ptr<PhysicalOperator> GenerateIC12Plan();
    unique_ptr<PhysicalOperator> GenerateIC12PlanGLogue();
    unique_ptr<PhysicalOperator> GenerateIC13PlanGLogue();
    unique_ptr<PhysicalOperator> GenerateIC13PlanCalcite();

    unique_ptr<PhysicalOperator> GenerateIC21PlanGLogue();
    unique_ptr<PhysicalOperator> GenerateIC21PlanCalcite();

    unique_ptr<PhysicalOperator> GenerateIC31PlanGLogue();
    unique_ptr<PhysicalOperator> GenerateIC31PlanCalcite();
    unique_ptr<PhysicalOperator> GenerateIC32PlanGLogue();

    unique_ptr<PhysicalOperator> GenerateIC41PlanGLogue();
    unique_ptr<PhysicalOperator> GenerateIC41PlanCalcite();

    unique_ptr<PhysicalOperator> GenerateIC51PlanGLogue();
    unique_ptr<PhysicalOperator> GenerateIC51PlanGLogueEI();
    unique_ptr<PhysicalOperator> GenerateIC51PlanAnother();
    unique_ptr<PhysicalOperator> GenerateIC52PlanGLogue();
    unique_ptr<PhysicalOperator> GenerateIC52PlanGLogueEI();
    unique_ptr<PhysicalOperator> GenerateIC52PlanSelf();
    unique_ptr<PhysicalOperator> GenerateIC52PlanByPass();
    unique_ptr<PhysicalOperator> GenerateIC52PlanByPassFromPerson();

    unique_ptr<PhysicalOperator> GenerateIC61PlanGLogue();
    unique_ptr<PhysicalOperator> GenerateIC62PlanGLogue();

    unique_ptr<PhysicalOperator> GenerateIC71PlanGLogue();
    unique_ptr<PhysicalOperator> GenerateIC71PlanGLogueNoIntersect();

    unique_ptr<PhysicalOperator> GenerateIC81PlanGLogue();

    unique_ptr<PhysicalOperator> GenerateIC91PlanGLogue();
    unique_ptr<PhysicalOperator> GenerateIC92PlanGLogue();

    unique_ptr<PhysicalOperator> GenerateIC111PlanGLogue();
    unique_ptr<PhysicalOperator> GenerateIC112PlanGLogue();

    unique_ptr<PhysicalOperator> GenerateIC121PlanGLogue();

// filter push down
    unique_ptr<PhysicalOperator> GenerateIC21PlanPPFilter();
    unique_ptr<PhysicalOperator> GenerateIC61PlanPPFilter();
    unique_ptr<PhysicalOperator> GenerateIC62PlanPPFilter();
    unique_ptr<PhysicalOperator> GenerateIC91PlanPPFilter();
    unique_ptr<PhysicalOperator> GenerateIC92PlanPPFilter();
    unique_ptr<PhysicalOperator> GenerateIC111PlanPPFilter();
    unique_ptr<PhysicalOperator> GenerateIC112PlanPPFilter();

// job query
    unique_ptr<PhysicalOperator> GenerateJOB1aPlan(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB1aPlanSIP(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB1aPlanMerge(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB1aPlanMergeTest(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB2aPlan(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB2aPlanNewDirect(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB3aPlanMerge(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB4aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB5aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB6aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB7aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB8aPlan(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB8aPlanNewDirect(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB9aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB10aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB11aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB12aPlan(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB12aPlanNewDirect(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB13aPlan(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB13aPlanNewDirect(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB14aPlan(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB14aPlanNewDirect(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB15aPlan(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB15aPlanNewDirect(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB16aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB17aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB18aPlan(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB18aPlanNewDirect(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB19aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB20aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB21aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB22aPlan(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB22aPlanNewDirect(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB23aPlan(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB23aPlanNewDirect(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB23aPlanNewDirectMore(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB24aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB25aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB26aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB27aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB28aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB29aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB30aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB31aPlan(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB32aPlan(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB32aPlanNewDirect(ClientContext& context);

    unique_ptr<PhysicalOperator> GenerateJOB33aPlan(ClientContext& context);

// back to hash join
    unique_ptr<PhysicalOperator> GenerateJOB1aPlanHash(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB2aPlanHash(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB3aPlanHash(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB4aPlanHash(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB5aPlanHash(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB6aPlanHash(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB6aPlanHashPure(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB7aPlanHash(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB8aPlanHash(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB9aPlanHash(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateJOB10aPlanHash(ClientContext& context);

// triangle query (person - forum - post)
    unique_ptr<PhysicalOperator> GenerateTriangle(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateTriangleWOEI(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateTriangleWOPara(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateTriangleWOParaWOEI(ClientContext& context);
// butterfly query (person - person - forum - post)
    unique_ptr<PhysicalOperator> GenerateButterfly(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateButterflyWOEI(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateButterflyWOEINew(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateButterflyWOEIHash(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateButterflyPara(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateButterflyWOEIPara(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateButterflyMOD(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateButterflyMODWOEI(ClientContext& context);
// clique query (person - person - person - person)
    unique_ptr<PhysicalOperator> GenerateClique(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateCliqueWOEI(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateCliquePara(ClientContext& context);
    unique_ptr<PhysicalOperator> GenerateCliqueWOEIPara(ClientContext& context);
// path query (person - knows - person - forum_person - forum)
    unique_ptr<PhysicalOperator> GeneratePath(ClientContext& context);
// path query person-knows-person
// with merge
    unique_ptr<PhysicalOperator> GeneratePathMergeSIP(ClientContext& context);
// with sip only
    unique_ptr<PhysicalOperator> GeneratePathSIP(ClientContext& context);

private:
	//! Parse statements and resolve pragmas from a query
	bool ParseStatements(ClientContextLock &lock, const string &query, vector<unique_ptr<SQLStatement>> &result,
	                     PreservedError &error);
	//! Issues a query to the database and returns a Pending Query Result
	unique_ptr<PendingQueryResult> PendingQueryInternal(ClientContextLock &lock, unique_ptr<SQLStatement> statement,
	                                                    const PendingQueryParameters &parameters, bool verify = true);
	unique_ptr<QueryResult> ExecutePendingQueryInternal(ClientContextLock &lock, PendingQueryResult &query);

	//! Parse statements from a query
	vector<unique_ptr<SQLStatement>> ParseStatementsInternal(ClientContextLock &lock, const string &query);
	//! Perform aggressive query verification of a SELECT statement. Only called when query_verification_enabled is
	//! true.
	PreservedError VerifyQuery(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement);

	void InitialCleanup(ClientContextLock &lock);
	//! Internal clean up, does not lock. Caller must hold the context_lock.
	void CleanupInternal(ClientContextLock &lock, BaseQueryResult *result = nullptr,
	                     bool invalidate_transaction = false);
	unique_ptr<PendingQueryResult> PendingStatementOrPreparedStatement(ClientContextLock &lock, const string &query,
	                                                                   unique_ptr<SQLStatement> statement,
	                                                                   shared_ptr<PreparedStatementData> &prepared,
	                                                                   const PendingQueryParameters &parameters);
	unique_ptr<PendingQueryResult> PendingPreparedStatement(ClientContextLock &lock,
	                                                        shared_ptr<PreparedStatementData> statement_p,
	                                                        const PendingQueryParameters &parameters);

	//! Internally prepare a SQL statement. Caller must hold the context_lock.
	shared_ptr<PreparedStatementData>
	CreatePreparedStatement(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
	                        optional_ptr<case_insensitive_map_t<Value>> values = nullptr);
	unique_ptr<PendingQueryResult> PendingStatementInternal(ClientContextLock &lock, const string &query,
	                                                        unique_ptr<SQLStatement> statement,
	                                                        const PendingQueryParameters &parameters);
	unique_ptr<QueryResult> RunStatementInternal(ClientContextLock &lock, const string &query,
	                                             unique_ptr<SQLStatement> statement, bool allow_stream_result,
	                                             bool verify = true);
	unique_ptr<PreparedStatement> PrepareInternal(ClientContextLock &lock, unique_ptr<SQLStatement> statement);
	void LogQueryInternal(ClientContextLock &lock, const string &query);

	unique_ptr<QueryResult> FetchResultInternal(ClientContextLock &lock, PendingQueryResult &pending);
	unique_ptr<DataChunk> FetchInternal(ClientContextLock &lock, Executor &executor, BaseQueryResult &result);

	unique_ptr<ClientContextLock> LockContext();

	void BeginTransactionInternal(ClientContextLock &lock, bool requires_valid_transaction);
	void BeginQueryInternal(ClientContextLock &lock, const string &query);
	PreservedError EndQueryInternal(ClientContextLock &lock, bool success, bool invalidate_transaction);

	PendingExecutionResult ExecuteTaskInternal(ClientContextLock &lock, PendingQueryResult &result);

	unique_ptr<PendingQueryResult> PendingStatementOrPreparedStatementInternal(
	    ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
	    shared_ptr<PreparedStatementData> &prepared, const PendingQueryParameters &parameters);

	unique_ptr<PendingQueryResult> PendingQueryPreparedInternal(ClientContextLock &lock, const string &query,
	                                                            shared_ptr<PreparedStatementData> &prepared,
	                                                            const PendingQueryParameters &parameters);

	unique_ptr<PendingQueryResult> PendingQueryInternal(ClientContextLock &, const shared_ptr<Relation> &relation,
	                                                    bool allow_stream_result);

private:
	//! Lock on using the ClientContext in parallel
	mutex context_lock;
	//! The currently active query context
	unique_ptr<ActiveQueryContext> active_query;
	//! The current query progress
	atomic<double> query_progress;
    //! The input is for sql query (0) or pb_file generate (1) or pb_file execute (2)
    int sql_mode;
    //! The pb_file related to the pb_file generate and pb_file execute mode
    string pb_file;
    //! The parameters used in physical plan
    unique_ptr<std::vector<string>> paras;
public:
    // start and end time of optimization
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> start_time;
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> end_time;
};

class ClientContextLock {
public:
	explicit ClientContextLock(mutex &context_lock) : client_guard(context_lock) {
	}

	~ClientContextLock() {
	}

private:
	lock_guard<mutex> client_guard;
};

class ClientContextWrapper {
public:
	explicit ClientContextWrapper(const shared_ptr<ClientContext> &context)
	    : client_context(context) {

	      };
	shared_ptr<ClientContext> GetContext() {
		auto actual_context = client_context.lock();
		if (!actual_context) {
			throw ConnectionException("Connection has already been closed");
		}
		return actual_context;
	}

private:
	std::weak_ptr<ClientContext> client_context;
};

} // namespace duckdb
