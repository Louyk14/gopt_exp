#include "duckdb/optimizer/sip_join_rewriter.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {
using namespace std;

unique_ptr<LogicalOperator> SIPJoinRewriter::Rewrite(unique_ptr<LogicalOperator> op) {
	if (op->type == LogicalOperatorType::LOGICAL_CREATE_RAI) {
		return op;
	}
	VisitOperator(*op);
	return op;
}

static bool IsDirectScan(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		return true;
	}
	if (op.children.size() == 1) {
		return IsDirectScan(*op.children[0]);
	} else {
		return false;
	}
}

static inline void RewriteJoinCondition(column_t edge_column, BoundColumnRefExpression *edge, column_t vertex_column,
                                        BoundColumnRefExpression *vertex, LogicalComparisonJoin &join, bool check_aj) {
	// rewrite the rai join condition
	ColumnBinding edge_binding(edge->binding.table_index, edge_column, edge_column, edge->binding.table);
	edge->binding = join.PushdownColumnBinding(edge_binding);
	edge->alias = edge->binding.table->GetColumn(LogicalIndex(edge_column)).Name() + "_rowid";
	edge->return_type = LogicalType::BIGINT;
	ColumnBinding vertex_binding(vertex->binding.table_index, vertex_column, vertex_column, vertex->binding.table);
    vertex->binding = join.PushdownColumnBinding(vertex_binding);
	vertex->alias = vertex_column == COLUMN_IDENTIFIER_ROW_ID
	                    ? vertex_binding.table->name + "_rowid"
	                    : vertex_binding.table->GetColumn(LogicalIndex(vertex_column)).Name() + ".rowid";
	vertex->return_type = LogicalType::BIGINT;
	// add join op mark
	join.op_mark = OpMark::SIP_JOIN;
	// if (check_aj) {
	//	join.enable_lookup_join = IsDirectScan(*join.children[0]);
	//} else {
	//	join.enable_lookup_join = false;
	//}
}

bool SIPJoinRewriter::BindRAIInfo(LogicalComparisonJoin &join, vector<unique_ptr<RAI>> &rais,
                                  JoinCondition &condition) {
	bool check_if_enable_aj = false;
	auto left = reinterpret_cast<BoundColumnRefExpression *>(condition.left.get());
	auto right = reinterpret_cast<BoundColumnRefExpression *>(condition.right.get());
	for (auto &rai : rais) {
		auto rai_info = make_uniq<RAIInfo>();
		if (left->binding.table == rai->referenced_tables[0] && right->binding.table == rai->table &&
		    left->binding.column_ordinal == rai->referenced_columns[0] &&
		    right->binding.column_index == rai->column_ids[0]) { // SOURCE_EDGE
			rai_info->rai_type = RAIType::SOURCE_EDGE;
			check_if_enable_aj = true;
		} else if (left->binding.table == rai->referenced_tables[1] && right->binding.table == rai->table &&
		           left->binding.column_index == rai->referenced_columns[1] &&
		           right->binding.column_index == rai->column_ids[1]) { // TARGET_EDGE
			rai_info->rai_type = RAIType::TARGET_EDGE;
			check_if_enable_aj = true;
#if ENABLE_ALISTS
		} else if (left->binding.table == rai->table && right->binding.table == rai->referenced_tables[0] &&
		           left->binding.column_ordinal == rai->column_ids[0] &&
		           right->binding.column_ordinal == rai->referenced_columns[0]) { // EDGE_SOURCE
			rai_info->rai_type = RAIType::EDGE_SOURCE;
			rai_info->forward = true;
			rai_info->passing_tables[0] = left->binding.table_index;
			auto edge_table = left->binding.table_index;
			check_if_enable_aj = true;
			if (rai_info_map.find(edge_table) != rai_info_map.end()) {
				// IF EXTEND PUSHDOWN THROUGH EDGE TABLE
				//
				// passing_tables[0] == 0 => type EDGE_TARGET
				// rinfo->rai == rai.get() => the same edge
				// rinfo->vertex == rai->referenced_tables[1] => the vertex is the target vertex
				// => TARGET_EDGE or EDGE_TARGET
				for (auto rinfo : rai_info_map[edge_table]) {
					if (rinfo->rai == rai.get() && rinfo->vertex == rai->referenced_tables[1] &&
					    rinfo->passing_tables[0] == 0) {
						rai_info->passing_tables[1] = rinfo->vertex_id;
						rai_info->left_cardinalities[1] = rinfo->vertex->GetStorage().info->cardinality;
					}
				}
			}
        }
		else if (left->binding.table == rai->table && right->binding.table == rai->referenced_tables[1] &&
		           left->binding.column_ordinal == rai->column_ids[1] &&
		           right->binding.column_ordinal == rai->referenced_columns[1]) { // EDGE_TARGET
			rai_info->rai_type = RAIType::EDGE_TARGET;
			if (rai->rai_direction == RAIDirection::UNDIRECTED) {
				rai_info->forward = false;
				rai_info->passing_tables[0] = left->binding.table_index;
				check_if_enable_aj = true;
			}
		} else if (left->binding.table == right->binding.table && left->binding.table == rai->table &&
		           left->binding.column_ordinal == rai->column_ids[0] &&
		           right->binding.column_ordinal == rai->column_ids[1]) {
			// forward
			rai_info->rai_type = RAIType::SELF;
			rai_info->forward = true;
		} else if (left->binding.table == right->binding.table && left->binding.table == rai->table &&
		           right->binding.column_ordinal == rai->column_ids[0] &&
		           left->binding.column_ordinal == rai->column_ids[1]) {
			// backward
			rai_info->rai_type = RAIType::SELF;
			rai_info->forward = false;
#endif
		}

		switch (rai_info->rai_type) {
		case RAIType::SOURCE_EDGE:
		case RAIType::TARGET_EDGE: {
			rai_info->rai = rai.get();
			rai_info->forward = rai_info->rai_type == RAIType::TARGET_EDGE;
			rai_info->vertex = left->binding.table;
			rai_info->vertex_id = left->binding.table_index;
			rai_info->passing_tables[0] = left->binding.table_index;
			rai_info->left_cardinalities[0] = left->binding.table->GetStorage().info->cardinality;
			if (rai_info_map.find(right->binding.table_index) == rai_info_map.end()) {
				vector<RAIInfo *> infos;
				rai_info_map[right->binding.table_index] = infos;
			}
			rai_info_map[right->binding.table_index].push_back(rai_info.get());
			RewriteJoinCondition(right->binding.column_ordinal, right, COLUMN_IDENTIFIER_ROW_ID, left, join,
			                     check_if_enable_aj);
			condition.rais.push_back(move(rai_info));
			return true;
		}
#if ENABLE_ALISTS
		case RAIType::EDGE_TARGET:
		case RAIType::EDGE_SOURCE: {
			// left is an edge, while right is a vertex
			rai_info->rai = rai.get();
			rai_info->vertex = right->binding.table;
			rai_info->vertex_id = right->binding.table_index;
			rai_info->left_cardinalities[0] = left->binding.table->GetStorage().info->cardinality;
			if (rai_info->rai_type == RAIType::EDGE_SOURCE) {
				rai_info->compact_list = &rai_info->rai->alist->compact_forward_list;
			} else if (rai_info->rai_type == RAIType::EDGE_TARGET &&
			           rai_info->rai->rai_direction == RAIDirection::UNDIRECTED) {
				rai_info->compact_list = &rai_info->rai->alist->compact_backward_list;
			}
			auto edge_table = left->binding.table_index;
			if (rai_info_map.find(edge_table) == rai_info_map.end()) {
				vector<RAIInfo *> infos;
				rai_info_map[edge_table] = infos;
			}
			rai_info_map[edge_table].push_back(rai_info.get());
			RewriteJoinCondition(left->binding.column_ordinal, left, COLUMN_IDENTIFIER_ROW_ID, right, join,
			                     check_if_enable_aj);
			condition.rais.push_back(move(rai_info));
			return true;
		}
		case RAIType::SELF: {
			rai_info->rai = rai.get();
			rai_info->passing_tables[0] = left->binding.table_index;
			rai_info->left_cardinalities[0] = left->binding.table->GetStorage().info->cardinality;
			rai_info->compact_list = rai_info->forward ? &rai_info->rai->alist->compact_forward_list
			                                           : &rai_info->rai->alist->compact_backward_list;
			RewriteJoinCondition(left->binding.column_ordinal, left, right->binding.column_ordinal, right, join, true);
			condition.rais.push_back(move(rai_info));
			return true;
		}
#endif
		default:
			continue;
		}
	}
	return false;
}

void SIPJoinRewriter::DoRewrite(LogicalComparisonJoin &join) {
	for (auto condition = join.conditions.begin(); condition != join.conditions.end(); condition++) {
		if (condition->left->type == ExpressionType::BOUND_COLUMN_REF &&
		    condition->right->type == ExpressionType::BOUND_COLUMN_REF &&
		    condition->comparison == ExpressionType::COMPARE_EQUAL) {
			auto &left_binding = reinterpret_cast<BoundColumnRefExpression *>(condition->left.get())->binding;
			auto &right_binding = reinterpret_cast<BoundColumnRefExpression *>(condition->right.get())->binding;

            if (left_binding.table == NULL) {
                // left_binding.column_ordinal = left_binding.column_index;
                int left_index = left_binding.table_index;
                if (left_index < binder.bind_context.GetBindingsList().size())
                    left_binding.table = binder.bind_context.GetBindingsEntry(left_index);
            }
            if (right_binding.table == NULL) {
                // right_binding.column_ordinal = right_binding.column_index;
                int right_index = right_binding.table_index;
                if (right_index < binder.bind_context.GetBindingsList().size())
                    right_binding.table = binder.bind_context.GetBindingsEntry(right_index);
            }

			if (left_binding.table == nullptr || right_binding.table == nullptr) {
				continue;
			}
			auto &left_rais = left_binding.table->GetStorage().info->rais;
			auto &right_rais = right_binding.table->GetStorage().info->rais;
			bool reorder_condition = false;
			if (left_rais.size() != 0) {
				if (BindRAIInfo(join, left_rais, *condition)) {
					reorder_condition = true;
				}
			}
			if (reorder_condition == false && right_rais.size() != 0) {
				if (BindRAIInfo(join, right_rais, *condition)) {
					reorder_condition = true;
				}
			}
			if (reorder_condition) {
				auto x = move(*condition);
				join.conditions.erase(condition);
				join.conditions.insert(join.conditions.begin(), move(x));
				return;
			}
		}
	}
}

void SIPJoinRewriter::VisitOperator(LogicalOperator &op) {
    VisitOperatorChildren(op);
    if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN && op.op_mark != OpMark::HASH_JOIN) {
        auto &join = reinterpret_cast<LogicalComparisonJoin &>(op);
        DoRewrite(join);
    }
}



} // namespace duckdb
