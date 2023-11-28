//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/join_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/protocode/algebra.pb.h"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Join Types
//===--------------------------------------------------------------------===//
enum class JoinType : uint8_t {
	INVALID = 0, // invalid join type
	LEFT = 1,    // left
	RIGHT = 2,   // right
	INNER = 3,   // inner
	OUTER = 4,   // outer
	SEMI = 5,    // SEMI join returns left side row ONLY if it has a join partner, no duplicates
	ANTI = 6,    // ANTI join returns left side row ONLY if it has NO join partner, no duplicates
	MARK = 7,    // MARK join returns marker indicating whether or not there is a join partner (true), there is no join
	             // partner (false)
	SINGLE = 8   // SINGLE join is like LEFT OUTER JOIN, BUT returns at most one join partner per entry on the LEFT side
	             // (and NULL if no partner is found)
};

string JoinTypeToString(JoinType type);
substrait::HashJoinRel_JoinType JoinTypeToSubstraitHashJoinType(JoinType type);
JoinType SubstraitHashJoinTypeToJoinType(substrait::HashJoinRel_JoinType type);

substrait::SIPJoinRel_JoinType JoinTypeToSubstraitSIPJoinType(JoinType type);
JoinType SubstraitSIPJoinTypeToJoinType(substrait::SIPJoinRel_JoinType type);

substrait::MergeSIPJoinRel_JoinType JoinTypeToSubstraitMergeSIPJoinType(JoinType type);
JoinType SubstraitMergeSIPJoinTypeToJoinType(substrait::MergeSIPJoinRel_JoinType type);

} // namespace duckdb
