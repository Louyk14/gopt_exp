#include "duckdb/execution/operator/scan/physical_index_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

class PhysicalIndexScanOperatorState : public PhysicalOperatorState {
public:
	PhysicalIndexScanOperatorState() : PhysicalOperatorState(nullptr), initialized(false) {
	}

	bool initialized;
	TableIndexScanState scan_state;
};

void PhysicalIndexScan::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
                                         SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	auto state = reinterpret_cast<PhysicalIndexScanOperatorState *>(state_);
	if (column_ids.size() == 0) {
		return;
	}

	auto &transaction = Transaction::GetTransaction(context);
	if (!state->initialized) {
		// initialize the scan state of the index
		if (low_index && high_index) {
			// two predicates
			table.InitializeIndexScan(transaction, state->scan_state, index, low_value, low_expression_type, high_value,
			                          high_expression_type, column_ids);
		} else {
			// single predicate
			Value value;
			ExpressionType type;
			if (low_index) {
				// > or >=
				value = low_value;
				type = low_expression_type;
			} else if (high_index) {
				// < or <=
				value = high_value;
				type = high_expression_type;
			} else {
				// equality
				assert(equal_index);
				value = equal_value;
				type = ExpressionType::COMPARE_EQUAL;
			}
			table.InitializeIndexScan(transaction, state->scan_state, index, value, type, column_ids);
		}
		state->initialized = true;
	}
	// scan the index
	table.IndexScan(transaction, chunk, state->scan_state);
}

string PhysicalIndexScan::ExtraRenderInformation() const {
	auto result = tableref.name + "(" + to_string(table_index) + ")";
	result += "[LOW_VALUE: " + low_value.ToString() + "]";
	result += "[";
	for (auto &id : column_ids) {
		if (id == COLUMN_IDENTIFIER_ROW_ID) {
			result += "rowid,";
		} else {
			result += tableref.columns[id].name + ",";
		}
	}
	result = result.substr(0, result.size() - 1);
	result += "]";
	return result;
}

unique_ptr<PhysicalOperatorState> PhysicalIndexScan::GetOperatorState() {
	return make_unique<PhysicalIndexScanOperatorState>();
}

substrait::Rel* PhysicalIndexScan::ToSubstraitClass(unordered_map<int, string>& tableid2name) const {
    substrait::Rel* index_scan_rel = new substrait::Rel();
    substrait::ReadRel* read = new substrait::ReadRel();
    substrait::RelCommon* common = new substrait::RelCommon();
    substrait::RelCommon_Emit* emit = new substrait::RelCommon_Emit();

    for (int i = 0; i < column_ids.size(); ++i) {
        emit->add_output_mapping(column_ids[i]);
        if (column_ids[i] == COLUMN_IDENTIFIER_ROW_ID) {
            emit->add_output_types(TypeIdToString(TypeId::INT64));
        }
        else {
            emit->add_output_types(TypeIdToString(table.GetColumn(column_ids[i])->type));
        }
    }

    if (low_index) {
        emit->add_output_exp_type(TypeIdToString(low_value.type));
        emit->add_output_names("LOW");
        emit->add_output_names(low_value.ToString());
        emit->add_output_order(static_cast<int>(low_expression_type));
    }
    else {
        emit->add_output_exp_type("");
        emit->add_output_names("NLOW");
        emit->add_output_names("");
    }
    if (high_index) {
        emit->add_output_exp_type(TypeIdToString(high_value.type));
        emit->add_output_names("HIGH");
        emit->add_output_names(high_value.ToString());
        emit->add_output_order(static_cast<int>(high_expression_type));
    }
    else {
        emit->add_output_exp_type("");
        emit->add_output_names("NHIGH");
        emit->add_output_names("");
    }
    if (equal_index) {
        emit->add_output_exp_type(TypeIdToString(equal_value.type));
        emit->add_output_names("EQUAL");
        emit->add_output_names(equal_value.ToString());
    }
    else {
        emit->add_output_exp_type("");
        emit->add_output_names("NEQUAL");
        emit->add_output_names("");
    }

    common->set_allocated_emit(emit);

    substrait::ReadRel_NamedTable* named_table = new substrait::ReadRel_NamedTable();
    named_table->add_names(table.info->table);
    named_table->add_names(to_string(index.column_ids[0]));

    read->set_allocated_common(common);
    read->set_allocated_named_table(named_table);

    index_scan_rel->set_allocated_read(read);

    return index_scan_rel;
}