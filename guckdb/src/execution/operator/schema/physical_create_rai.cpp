#include "duckdb/execution/operator/schema/physical_create_rai.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/rai.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

using namespace duckdb;

class CreateRAIState : public CachingOperatorState {
public:
    explicit CreateRAIState(ExecutionContext &context) {
        vector<LogicalType> types;
        for (int i = 0; i < 3; ++i) {
            types.push_back(LogicalType::BIGINT);
        }
        collection.Initialize(context.client, types);
    }

    DataChunk collection;

public:
    void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {

    }
};

class RAIGlobalSinkState : public GlobalSinkState {
public:
    RAIGlobalSinkState(ClientContext &context_p)
            : context(context_p) {
        vector<LogicalType> types;
        for (int i = 0; i < 3; ++i) {
            types.push_back(LogicalType::BIGINT);
        }
        collection.Initialize(context, types);
    }

public:
    ClientContext &context;
    DataChunk collection;
};


class RAILocalSinkState : public LocalSinkState {
public:
    RAILocalSinkState(ClientContext &context) {
        vector<LogicalType> types;
        for (int i = 0; i < 3; ++i) {
            types.push_back(LogicalType::BIGINT);
        }
        collection.Initialize(context, types);
    }
public:
    DataChunk collection;
};



OperatorResultType PhysicalCreateRAI::ExecuteInternal(ExecutionContext &context, DataChunk &inputs, DataChunk &chunk,
                                               GlobalOperatorState &gstate, OperatorState &state_p) const {

    auto &state = state_p.Cast<CreateRAIState>();
    auto &sink = sink_state->Cast<RAIGlobalSinkState>();

    int counts = 30;
    UnifiedVectorFormat source_data{}, edges_data{}, target_data{};
    sink.collection.data[0].ToUnifiedFormat(counts, source_data);
    sink.collection.data[1].ToUnifiedFormat(counts, edges_data);
    sink.collection.data[2].ToUnifiedFormat(counts, target_data);
    auto source_ids = UnifiedVectorFormat::GetData<hash_t>(source_data);
    auto edge_ids = UnifiedVectorFormat::GetData<hash_t>(edges_data);
    auto target_ids = UnifiedVectorFormat::GetData<hash_t>(target_data);

    assert(children.size() == 1);
    unique_ptr<RAI> rai = make_uniq<RAI>(name, &table, rai_direction, column_ids, referenced_tables, referenced_columns);
    rai->alist->source_num = referenced_tables[0]->GetStorage().info->cardinality;
    rai->alist->target_num = referenced_tables[1]->GetStorage().info->cardinality;
    idx_t count = 0;

    idx_t chunk_count = sink.collection.size();
    vector<column_t> updated_columns = {0, 0};
    table.AddRAIColumns(context.client, column_ids, updated_columns);
    int index0 = updated_columns[0];
    int index1 = updated_columns[1];
    table.GetStorage().row_groups = table.GetStorage().row_groups->CreateAppendColumnRAI(index0, LogicalType::BIGINT, sink.collection.data[0], chunk_count);
    table.GetStorage().row_groups = table.GetStorage().row_groups->CreateAppendColumnRAI(index1, LogicalType::BIGINT, sink.collection.data[2], chunk_count);

    /*
    TableAppendState table_state;
    table.GetStorage().row_groups->InitializeAppend(table_state);

    auto& append_state_0 = table_state.row_group_append_state.states[updated_columns[0]];
    auto& append_state_1 = table_state.row_group_append_state.states[updated_columns[1]];
    */
    if (chunk_count != 0) {
        /*if (updated_columns[0] != 0) {
            table.GetStorage().row_groups->AppendColumnRAI(updated_columns[0], append_state_0, input.data[0], chunk_count);
        }
        if (updated_columns[1] != 0) {
            table.GetStorage().row_groups->AppendColumnRAI(updated_columns[1], append_state_1, input.data[2], chunk_count);
        }*/
#if ENABLE_ALISTS
        if (rai->rai_direction == RAIDirection::PKFK) {
            rai->alist->AppendPKFK(sink.collection.data[0], sink.collection.data[1], chunk_count);
        } else {
            rai->alist->Append(sink.collection.data[0], sink.collection.data[1], sink.collection.data[2],
                               chunk_count, rai_direction);
        }
#endif
        count += chunk_count;
    }

#if ENABLE_ALISTS
    rai->alist->Finalize(rai_direction);
#endif

    table.GetStorage().AddRAI(move(rai));
    chunk.SetCardinality(1);
    chunk.SetValue(0, 0, Value::BIGINT(count));

    return OperatorResultType::NEED_MORE_INPUT;
}


void PhysicalCreateRAI::EnlargeTable(ClientContext &context, DataChunk &input) const {
    // auto &state = state_p.Cast<CreateRAIState>();
    auto &sink = sink_state->Cast<RAIGlobalSinkState>();

    int counts = 30;
    UnifiedVectorFormat source_data{}, edges_data{}, target_data{};
    input.data[0].ToUnifiedFormat(counts, source_data);
    input.data[1].ToUnifiedFormat(counts, edges_data);
    input.data[2].ToUnifiedFormat(counts, target_data);
    auto source_ids = UnifiedVectorFormat::GetData<hash_t>(source_data);
    auto edge_ids = UnifiedVectorFormat::GetData<hash_t>(edges_data);
    auto target_ids = UnifiedVectorFormat::GetData<hash_t>(target_data);

    assert(children.size() == 1);
    unique_ptr<RAI> rai = make_uniq<RAI>(name, &table, rai_direction, column_ids, referenced_tables, referenced_columns);
    rai->alist->source_num = referenced_tables[0]->GetStorage().info->cardinality;
    rai->alist->target_num = referenced_tables[1]->GetStorage().info->cardinality;
    idx_t count = 0;

    idx_t chunk_count = input.size();
    vector<column_t> updated_columns = {0, 0};
    table.AddRAIColumns(context, column_ids, updated_columns);
    int index0 = updated_columns[0];
    int index1 = updated_columns[1];
    table.GetStorage().row_groups = table.GetStorage().row_groups->CreateAppendColumnRAI(index0, LogicalType::BIGINT, input.data[0], chunk_count);
    table.GetStorage().row_groups = table.GetStorage().row_groups->CreateAppendColumnRAI(index1, LogicalType::BIGINT, input.data[2], chunk_count);

    /*
    TableAppendState table_state;
    table.GetStorage().row_groups->InitializeAppend(table_state);

    auto& append_state_0 = table_state.row_group_append_state.states[updated_columns[0]];
    auto& append_state_1 = table_state.row_group_append_state.states[updated_columns[1]];
    */
    if (chunk_count != 0) {
        /*if (updated_columns[0] != 0) {
            table.GetStorage().row_groups->AppendColumnRAI(updated_columns[0], append_state_0, input.data[0], chunk_count);
        }
        if (updated_columns[1] != 0) {
            table.GetStorage().row_groups->AppendColumnRAI(updated_columns[1], append_state_1, input.data[2], chunk_count);
        }*/
#if ENABLE_ALISTS
        if (rai->rai_direction == RAIDirection::PKFK) {
            rai->alist->AppendPKFK(input.data[0], input.data[1], chunk_count);
        } else {
            rai->alist->Append(input.data[0], input.data[1], input.data[2],
                               chunk_count, rai_direction);
        }
#endif
        count += chunk_count;
    }

#if ENABLE_ALISTS
    rai->alist->Finalize(rai_direction);
#endif

    table.GetStorage().AddRAI(move(rai));
    // chunk.SetCardinality(1);
    // chunk.SetValue(0, 0, Value::BIGINT(count));
}


unique_ptr<OperatorState> PhysicalCreateRAI::GetOperatorState(ExecutionContext &context) const {
    return make_uniq<CreateRAIState>(context);
}



SinkResultType PhysicalCreateRAI::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
    auto &lstate = input.local_state.Cast<RAILocalSinkState>();

    lstate.collection.Append(chunk, true);

    return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCreateRAI::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
    auto &gstate = input.global_state.Cast<RAIGlobalSinkState>();
    auto &lstate = input.local_state.Cast<RAILocalSinkState>();

    gstate.collection.Append(lstate.collection, true);

    // EnlargeTable(context, lstate.collection, gstate.collection);

    return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCreateRAI::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
    auto &sink = input.global_state.Cast<RAIGlobalSinkState>();
    EnlargeTable(context, sink.collection);
    return SinkFinalizeType::READY;
}


unique_ptr<GlobalSinkState> PhysicalCreateRAI::GetGlobalSinkState(ClientContext &context) const {
    return make_uniq<RAIGlobalSinkState>(context);
}

unique_ptr<LocalSinkState> PhysicalCreateRAI::GetLocalSinkState(ExecutionContext &context) const {
    return make_uniq<RAILocalSinkState>(context.client);
}

SourceResultType PhysicalCreateRAI::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
    return SourceResultType::FINISHED;
}