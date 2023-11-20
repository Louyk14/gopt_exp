#include "duckdb/common/enums/join_type.hpp"

using namespace std;

namespace duckdb {

string JoinTypeToString(JoinType type) {
	switch (type) {
	case JoinType::LEFT:
		return "LEFT";
	case JoinType::RIGHT:
		return "RIGHT";
	case JoinType::INNER:
		return "INNER";
	case JoinType::OUTER:
		return "OUTER";
	case JoinType::SEMI:
		return "SEMI";
	case JoinType::ANTI:
		return "ANTI";
	case JoinType::SINGLE:
		return "SINGLE";
	case JoinType::MARK:
		return "MARK";
	case JoinType::INVALID:
	default:
		return "INVALID";
	}
}

substrait::HashJoinRel_JoinType JoinTypeToSubstraitHashJoinType(JoinType type) {
    if (type == JoinType::INNER)
        return substrait::HashJoinRel_JoinType_JOIN_TYPE_INNER;
    else if (type == JoinType::SINGLE)
        return substrait::HashJoinRel_JoinType_JOIN_TYPE_SINGLE;
    else if (type == JoinType::ANTI)
        return substrait::HashJoinRel_JoinType_JOIN_TYPE_LEFT_ANTI;
    else if (type == JoinType::LEFT)
        return substrait::HashJoinRel_JoinType_JOIN_TYPE_LEFT;
    else if (type == JoinType::RIGHT)
        return substrait::HashJoinRel_JoinType_JOIN_TYPE_RIGHT;
    else if (type == JoinType::SEMI)
        return substrait::HashJoinRel_JoinType_JOIN_TYPE_LEFT_SEMI;
    else if (type == JoinType::MARK)
        return substrait::HashJoinRel_JoinType_JOIN_TYPE_MARK;
    else {
        std::cout << "unsupported join type" << std::endl;
    }
}

JoinType SubstraitHashJoinTypeToJoinType(substrait::HashJoinRel_JoinType type) {
    if (type == substrait::HashJoinRel_JoinType_JOIN_TYPE_INNER)
        return JoinType::INNER;
    else if (type == substrait::HashJoinRel_JoinType_JOIN_TYPE_SINGLE)
        return JoinType::SINGLE;
    else if (type == substrait::HashJoinRel_JoinType_JOIN_TYPE_LEFT_ANTI)
        return JoinType::ANTI;
    else if (type == substrait::HashJoinRel_JoinType_JOIN_TYPE_LEFT)
        return JoinType::LEFT;
    else if (type == substrait::HashJoinRel_JoinType_JOIN_TYPE_RIGHT)
        return JoinType::RIGHT;
    else if (type == substrait::HashJoinRel_JoinType_JOIN_TYPE_LEFT_SEMI)
        return JoinType::SEMI;
    else if (type == substrait::HashJoinRel_JoinType_JOIN_TYPE_MARK)
        return JoinType::MARK;
    else
        std::cout << "unsuppored hash join type in protobuf_serializer.cpp" << std::endl;

    return JoinType::INVALID;
}

} // namespace duckdb
