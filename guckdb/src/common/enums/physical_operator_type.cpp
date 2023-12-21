#include "duckdb/common/enums/physical_operator_type.hpp"

namespace duckdb {

// LCOV_EXCL_START
string PhysicalOperatorToString(PhysicalOperatorType type) {
	switch (type) {
	case PhysicalOperatorType::TABLE_SCAN:
		return "TABLE_SCAN";
	case PhysicalOperatorType::DUMMY_SCAN:
		return "DUMMY_SCAN";
	case PhysicalOperatorType::CHUNK_SCAN:
		return "CHUNK_SCAN";
	case PhysicalOperatorType::COLUMN_DATA_SCAN:
		return "COLUMN_DATA_SCAN";
	case PhysicalOperatorType::DELIM_SCAN:
		return "DELIM_SCAN";
	case PhysicalOperatorType::ORDER_BY:
		return "ORDER_BY";
	case PhysicalOperatorType::LIMIT:
		return "LIMIT";
	case PhysicalOperatorType::LIMIT_PERCENT:
		return "LIMIT_PERCENT";
	case PhysicalOperatorType::STREAMING_LIMIT:
		return "STREAMING_LIMIT";
	case PhysicalOperatorType::RESERVOIR_SAMPLE:
		return "RESERVOIR_SAMPLE";
	case PhysicalOperatorType::STREAMING_SAMPLE:
		return "STREAMING_SAMPLE";
	case PhysicalOperatorType::TOP_N:
		return "TOP_N";
	case PhysicalOperatorType::WINDOW:
		return "WINDOW";
	case PhysicalOperatorType::STREAMING_WINDOW:
		return "STREAMING_WINDOW";
	case PhysicalOperatorType::UNNEST:
		return "UNNEST";
	case PhysicalOperatorType::UNGROUPED_AGGREGATE:
		return "UNGROUPED_AGGREGATE";
	case PhysicalOperatorType::HASH_GROUP_BY:
		return "HASH_GROUP_BY";
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
		return "PERFECT_HASH_GROUP_BY";
	case PhysicalOperatorType::FILTER:
		return "FILTER";
	case PhysicalOperatorType::PROJECTION:
		return "PROJECTION";
	case PhysicalOperatorType::COPY_TO_FILE:
		return "COPY_TO_FILE";
	case PhysicalOperatorType::BATCH_COPY_TO_FILE:
		return "BATCH_COPY_TO_FILE";
	case PhysicalOperatorType::FIXED_BATCH_COPY_TO_FILE:
		return "FIXED_BATCH_COPY_TO_FILE";
	case PhysicalOperatorType::DELIM_JOIN:
		return "DELIM_JOIN";
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
		return "BLOCKWISE_NL_JOIN";
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
		return "NESTED_LOOP_JOIN";
	case PhysicalOperatorType::HASH_JOIN:
		return "HASH_JOIN";
    case PhysicalOperatorType::SIP_JOIN:
        return "SIP_JOIN";
    case PhysicalOperatorType::MERGE_SIP_JOIN:
        return "MERGE_SIP_JOIN";
	case PhysicalOperatorType::INDEX_JOIN:
		return "INDEX_JOIN";
    case PhysicalOperatorType::LOOKUP:
        return "LOOKUP";
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		return "PIECEWISE_MERGE_JOIN";
	case PhysicalOperatorType::IE_JOIN:
		return "IE_JOIN";
	case PhysicalOperatorType::ASOF_JOIN:
		return "ASOF_JOIN";
	case PhysicalOperatorType::CROSS_PRODUCT:
		return "CROSS_PRODUCT";
	case PhysicalOperatorType::POSITIONAL_JOIN:
		return "POSITIONAL_JOIN";
	case PhysicalOperatorType::POSITIONAL_SCAN:
		return "POSITIONAL_SCAN";
	case PhysicalOperatorType::UNION:
		return "UNION";
	case PhysicalOperatorType::INSERT:
		return "INSERT";
	case PhysicalOperatorType::BATCH_INSERT:
		return "BATCH_INSERT";
	case PhysicalOperatorType::DELETE_OPERATOR:
		return "DELETE";
	case PhysicalOperatorType::UPDATE:
		return "UPDATE";
	case PhysicalOperatorType::EMPTY_RESULT:
		return "EMPTY_RESULT";
	case PhysicalOperatorType::CREATE_TABLE:
		return "CREATE_TABLE";
	case PhysicalOperatorType::CREATE_TABLE_AS:
		return "CREATE_TABLE_AS";
	case PhysicalOperatorType::BATCH_CREATE_TABLE_AS:
		return "BATCH_CREATE_TABLE_AS";
	case PhysicalOperatorType::CREATE_INDEX:
		return "CREATE_INDEX";
	case PhysicalOperatorType::EXPLAIN:
		return "EXPLAIN";
	case PhysicalOperatorType::EXPLAIN_ANALYZE:
		return "EXPLAIN_ANALYZE";
	case PhysicalOperatorType::EXECUTE:
		return "EXECUTE";
	case PhysicalOperatorType::VACUUM:
		return "VACUUM";
	case PhysicalOperatorType::RECURSIVE_CTE:
		return "REC_CTE";
	case PhysicalOperatorType::CTE:
		return "CTE";
	case PhysicalOperatorType::RECURSIVE_CTE_SCAN:
		return "REC_CTE_SCAN";
	case PhysicalOperatorType::CTE_SCAN:
		return "CTE_SCAN";
	case PhysicalOperatorType::EXPRESSION_SCAN:
		return "EXPRESSION_SCAN";
	case PhysicalOperatorType::ALTER:
		return "ALTER";
	case PhysicalOperatorType::CREATE_SEQUENCE:
		return "CREATE_SEQUENCE";
	case PhysicalOperatorType::CREATE_VIEW:
		return "CREATE_VIEW";
	case PhysicalOperatorType::CREATE_SCHEMA:
		return "CREATE_SCHEMA";
	case PhysicalOperatorType::CREATE_MACRO:
		return "CREATE_MACRO";
	case PhysicalOperatorType::DROP:
		return "DROP";
	case PhysicalOperatorType::PRAGMA:
		return "PRAGMA";
	case PhysicalOperatorType::TRANSACTION:
		return "TRANSACTION";
	case PhysicalOperatorType::PREPARE:
		return "PREPARE";
	case PhysicalOperatorType::EXPORT:
		return "EXPORT";
	case PhysicalOperatorType::SET:
		return "SET";
	case PhysicalOperatorType::RESET:
		return "RESET";
	case PhysicalOperatorType::LOAD:
		return "LOAD";
	case PhysicalOperatorType::INOUT_FUNCTION:
		return "INOUT_FUNCTION";
	case PhysicalOperatorType::CREATE_TYPE:
		return "CREATE_TYPE";
	case PhysicalOperatorType::ATTACH:
		return "ATTACH";
	case PhysicalOperatorType::DETACH:
		return "DETACH";
	case PhysicalOperatorType::RESULT_COLLECTOR:
		return "RESULT_COLLECTOR";
	case PhysicalOperatorType::EXTENSION:
		return "EXTENSION";
	case PhysicalOperatorType::PIVOT:
		return "PIVOT";
	case PhysicalOperatorType::INVALID:
		break;
	}
	return "INVALID";
}
// LCOV_EXCL_STOP

} // namespace duckdb
