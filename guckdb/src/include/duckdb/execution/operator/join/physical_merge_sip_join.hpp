//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_merge_sip_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/execution/sip_hashtable.hpp"
#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

//! PhysicalHashJoin represents a hash loop join between two tables
    class PhysicalMergeSIPJoin : public PhysicalComparisonJoin {
    public:
        static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::MERGE_SIP_JOIN;

    public:
        PhysicalMergeSIPJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
                         vector<JoinCondition> cond, JoinType join_type, const vector<idx_t> &left_projection_map,
                         const vector<idx_t> &right_projection_map, const vector<idx_t> &merge_projection_map, vector<LogicalType> delim_types,
                         idx_t estimated_cardinality);

        //! Initialize HT for this operator
        unique_ptr<SIPHashTable> InitializeHashTable(ClientContext &context) const;
        void InitializeAList();
        void InitializeZoneFilter();
        void PassZoneFilter() const;
        void AppendHTBlocks(LocalSinkState &input, DataChunk &chunk, DataChunk &build_chunk) const;

        vector<idx_t> right_projection_map;
        //! The types of the keys
        vector<LogicalType> condition_types;
        //! The types of all conditions
        vector<LogicalType> build_types;
        //! Duplicate eliminated types; only used for delim_joins (i.e. correlated subqueries)
        vector<LogicalType> delim_types;
        //! Used in perfect hash join
        PerfectHashJoinStats perfect_join_statistics;

    public:
        string ParamsToString() const;

        // Operator Interface
        unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

        bool ParallelOperator() const override {
            return false;
        }

    protected:
        // CachingOperator Interface
        OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                           GlobalOperatorState &gstate, OperatorState &state) const override;

        // Source interface
        unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
        unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
                                                         GlobalSourceState &gstate) const override;
        SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

        //! Becomes a source when it is an external join
        bool IsSource() const override {
            return true;
        }

        bool ParallelSource() const override {
            return false;
        }

    public:
        // Sink Interface
        unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

        unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
        SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
        SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
        SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                  OperatorSinkFinalizeInput &input) const override;

        bool IsSink() const override {
            return true;
        }
        bool ParallelSink() const override {
            return false;
        }
    };

} // namespace duckdb
