#include "duckdb/common/enums/physical_operator_type.hpp"

using namespace std;

namespace duckdb {

string PhysicalOperatorToString(PhysicalOperatorType type) {
	switch (type) {
	case PhysicalOperatorType::LEAF:
		return "LEAF";
	case PhysicalOperatorType::DUMMY_SCAN:
		return "DUMMY_SCAN";
	case PhysicalOperatorType::SEQ_SCAN:
		return "SCAN";
	case PhysicalOperatorType::INDEX_SCAN:
		return "INDEX_SCAN";
	case PhysicalOperatorType::LOOKUP:
		return "LOOKUP";
	case PhysicalOperatorType::CHUNK_SCAN:
		return "CHUNK_SCAN";
	case PhysicalOperatorType::DELIM_SCAN:
		return "DELIM_SCAN";
	case PhysicalOperatorType::EXTERNAL_FILE_SCAN:
		return "EXTERNAL_FILE_SCAN";
	case PhysicalOperatorType::QUERY_DERIVED_SCAN:
		return "QUERY_DERIVED_SCAN";
	case PhysicalOperatorType::ORDER_BY:
		return "ORDER_BY";
	case PhysicalOperatorType::LIMIT:
		return "LIMIT";
	case PhysicalOperatorType::TOP_N:
		return "TOP_N";
	case PhysicalOperatorType::AGGREGATE:
		return "AGGREGATE";
	case PhysicalOperatorType::WINDOW:
		return "WINDOW";
	case PhysicalOperatorType::UNNEST:
		return "UNNEST";
	case PhysicalOperatorType::DISTINCT:
		return "DISTINCT";
	case PhysicalOperatorType::SIMPLE_AGGREGATE:
		return "SIMPLE_AGGREGATE";
	case PhysicalOperatorType::HASH_GROUP_BY:
		return "HASH_GROUP_BY";
	case PhysicalOperatorType::SORT_GROUP_BY:
		return "SORT_GROUP_BY";
	case PhysicalOperatorType::FILTER:
		return "FILTER";
	case PhysicalOperatorType::PROJECTION:
		return "PROJECTION";
	case PhysicalOperatorType::COPY_FROM_FILE:
		return "COPY_FROM_FILE";
	case PhysicalOperatorType::COPY_TO_FILE:
		return "COPY_TO_FILE";
	case PhysicalOperatorType::DELIM_JOIN:
		return "DELIM_JOIN";
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
		return "BLOCKWISE_NL_JOIN";
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
		return "NESTED_LOOP_JOIN";
	case PhysicalOperatorType::HASH_JOIN:
		return "HASH_JOIN";
	case PhysicalOperatorType::ADJACENCY_JOIN:
		return "ADJACENCY_JOIN";
	case PhysicalOperatorType::RAI_JOIN:
		return "RAI_JOIN";
	case PhysicalOperatorType::SIP_JOIN:
		return "SIP_JOIN";
	case PhysicalOperatorType::MERGE_RAI_JOIN:
		return "M_RAI_JOIN";
	case PhysicalOperatorType::MERGE_SIP_JOIN:
		return "M_SIP_JOIN";
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		return "PIECEWISE_MERGE_JOIN";
	case PhysicalOperatorType::CROSS_PRODUCT:
		return "CROSS_PRODUCT";
	case PhysicalOperatorType::UNION:
		return "UNION";
	case PhysicalOperatorType::INSERT:
		return "INSERT";
	case PhysicalOperatorType::INSERT_SELECT:
		return "INSERT_SELECT";
	case PhysicalOperatorType::DELETE:
		return "DELETE";
	case PhysicalOperatorType::UPDATE:
		return "UPDATE";
	case PhysicalOperatorType::EXPORT_EXTERNAL_FILE:
		return "EXPORT_EXTERNAL_FILE";
	case PhysicalOperatorType::EMPTY_RESULT:
		return "EMPTY_RESULT";
	case PhysicalOperatorType::TABLE_FUNCTION:
		return "TABLE_FUNCTION";
	case PhysicalOperatorType::CREATE:
		return "CREATE";
	case PhysicalOperatorType::CREATE_INDEX:
		return "CREATE_INDEX";
	case PhysicalOperatorType::CREATE_RAI:
		return "CREATE_RAI";
	case PhysicalOperatorType::EXPLAIN:
		return "EXPLAIN";
	case PhysicalOperatorType::EXECUTE:
		return "EXECUTE";
	case PhysicalOperatorType::VACUUM:
		return "VACUUM";
	case PhysicalOperatorType::RECURSIVE_CTE:
		return "REC_CTE";
	case PhysicalOperatorType::INVALID:
	default:
		return "INVALID";
	}
}

PhysicalOperatorType PhysicalOperatorFromString(string type) {
    if (type == "LEAF")
        return PhysicalOperatorType::LEAF;
    else if (type == "DUMMY_SCAN")
        return PhysicalOperatorType::DUMMY_SCAN;
    else if (type == "SCAN")
        return PhysicalOperatorType::SEQ_SCAN;
    else if (type == "INDEX_SCAN")
        return PhysicalOperatorType::INDEX_SCAN;
    else if (type == "LOOKUP")
        return PhysicalOperatorType::LOOKUP;
    else if (type == "CHUNK_SCAN")
        return PhysicalOperatorType::CHUNK_SCAN;
    else if (type == "DELIM_SCAN")
        return PhysicalOperatorType::DELIM_SCAN;
    else if (type == "EXTERNAL_FILE_SCAN")
        return PhysicalOperatorType::EXTERNAL_FILE_SCAN;
    else if (type == "QUERY_DERIVED_SCAN")
        return PhysicalOperatorType::QUERY_DERIVED_SCAN;
    else if (type == "ORDER_BY")
        return PhysicalOperatorType::ORDER_BY;
    else if (type == "LIMIT")
        return PhysicalOperatorType::LIMIT;
    else if (type == "TOP_N")
        return PhysicalOperatorType::TOP_N;
    else if (type == "AGGREGATE")
        return PhysicalOperatorType::AGGREGATE;
    else if (type == "WINDOW")
        return PhysicalOperatorType::WINDOW;
    else if (type == "UNNEST")
        return PhysicalOperatorType::UNNEST;
    else if (type == "DISTINCT")
        return PhysicalOperatorType::DISTINCT;
    else if (type == "SIMPLE_AGGREGATE")
        return PhysicalOperatorType::SIMPLE_AGGREGATE;
    else if (type == "HASH_GROUP_BY")
        return PhysicalOperatorType::HASH_GROUP_BY;
    else if (type == "FILTER")
        return PhysicalOperatorType::FILTER;
    else if (type == "PROJECTION")
        return PhysicalOperatorType::PROJECTION;
    else if (type == "DELIM_JOIN")
        return PhysicalOperatorType::DELIM_JOIN;
    else if (type == "HASH_JOIN")
        return PhysicalOperatorType::HASH_JOIN;
    else if (type == "RAI_JOIN")
        return PhysicalOperatorType::RAI_JOIN;
    else if (type == "SIP_JOIN")
        return PhysicalOperatorType::SIP_JOIN;
    else if (type == "M_RAI_JOIN")
        return PhysicalOperatorType::MERGE_RAI_JOIN;
    else if (type == "M_SIP_JOIN")
        return PhysicalOperatorType::MERGE_SIP_JOIN;
    else if (type == "UNION")
        return PhysicalOperatorType::UNION;
    else
        return PhysicalOperatorType::INVALID;
}

} // namespace duckdb
