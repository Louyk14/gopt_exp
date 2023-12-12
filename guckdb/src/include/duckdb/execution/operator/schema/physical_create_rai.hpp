//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_edge.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_rai_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"

namespace duckdb {

class PhysicalCreateRAI : public CachingPhysicalOperator {
public:
    static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_RAI;

	PhysicalCreateRAI(LogicalOperator &op, string name, TableCatalogEntry &table, RAIDirection rai_direction,
	                  vector<column_t> column_ids, vector<TableCatalogEntry *> referenced_tables,
	                  vector<column_t> referenced_columns)
	    : CachingPhysicalOperator(PhysicalOperatorType::CREATE_RAI, op.types, 0), name(name), table(table),
	      rai_direction(rai_direction), column_ids(column_ids), referenced_tables(referenced_tables),
	      referenced_columns(referenced_columns) {
	}

	string name;
    TableCatalogEntry &table;
	RAIDirection rai_direction;
	vector<column_t> column_ids;
	vector<TableCatalogEntry *> referenced_tables;
	vector<column_t> referenced_columns;

public:
    //! Source interface, NOP for this operator
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

    unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
    OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                       GlobalOperatorState &gstate, OperatorState &state) const override;
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

    unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
    SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override;

    void EnlargeTable(ClientContext &context, DataChunk &input) const;
    // static void BuildCreateRAIPipelines(Pipeline &current, MetaPipeline &meta_pipeline, PhysicalOperator &op);
    // void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

    bool IsSink() const override {
        return true;
    }
    bool ParallelSink() const override {
        return false;
    }

    bool IsSource() const override {
        return true;
    }
    bool ParallelSource() const override {
        return true;
    }
    bool ParallelOperator() const override {
        return true;
    }
};
} // namespace duckdb
