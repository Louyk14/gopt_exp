#include "duckdb/execution/physical_operator.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

string PhysicalOperator::AssignBlank(int depth) const {
	if (depth < 0)
		return "error";

	return string(depth, ' ');
}

string PhysicalOperator::GetHeader(unordered_map<duckdb::ExpressionType, duckdb::idx_t> &func_map,
                                   duckdb::idx_t &func_num, int depth) const {
	unordered_map<duckdb::ExpressionType, string> urimap;
	urimap[ExpressionType::COMPARE_EQUAL] = "\"https://github.com/substrait-io/substrait/blob/main/extensions/functions_comparison.yaml\"";

	vector<duckdb::ExpressionType> etype_list(func_map.size() + 1, ExpressionType::COMPARE_EQUAL);
	for (const auto& pair : func_map) {
		etype_list[pair.second] = pair.first;
	}
	unordered_map<string, int> extension_uri_reference_index;
	int reference_index = 0;
	vector<string> reference_list;

	string version_str = AssignBlank(depth) + "\"version: \" { \"minorNumber\": 20, \"producer\": \"validator-test\" },\n";

	string extension_str = AssignBlank(depth) + "\"extensions\": [\n";
	for (int i = 0; i < func_num; ++i) {
		extension_str += AssignBlank(++depth) + "{\n";
		extension_str += AssignBlank(++depth) + "\"extensionFunction\": {\n";

		int index = -1;
		if (extension_uri_reference_index.find(urimap[etype_list[i]]) != extension_uri_reference_index.end()) {
			index = extension_uri_reference_index[urimap[etype_list[i]]];
		}
		else {
			index = reference_index++;
			extension_uri_reference_index[urimap[etype_list[i]]] = index;
			reference_list.push_back(urimap[etype_list[i]]);
		}

		extension_str += AssignBlank(depth) + "\"extensionUriReference\": " + to_string(index) + ",\n";
		extension_str += AssignBlank(depth) + "\"functionAnchor:\" " + to_string(i) + ",\n";
		extension_str += AssignBlank(depth) + "\"name\": \"" + ExpressionTypeToString(etype_list[i]) + "\"\n";
		extension_str += AssignBlank(--depth) + "}";

		if (i != func_num - 1) {
			extension_str += ",\n";
		}
		else {
			extension_str += "\n";
		}
	}
	extension_str += AssignBlank(--depth) + "],\n";

	string uri_str = AssignBlank(depth) + "\"extensionUris\": [\n";
	for (int i = 0; i < reference_index; ++i) {
		uri_str += AssignBlank(++depth) + "{\n";
		uri_str += AssignBlank(++depth) + "\"extensionUriAnchor\": " + to_string(i) + ",\n";
		uri_str += AssignBlank(depth) + "\"uri\": \"" + reference_list[i] + "\"\n";
		uri_str += AssignBlank(--depth) + "}";

		if (i != func_num - 1) {
			uri_str += ",\n";
		}
		else {
			uri_str += "\n";
		}
	}
	uri_str += AssignBlank(--depth) + "],\n";

	return version_str + uri_str + extension_str;
}

string PhysicalOperator::ToSubstrait() const {
    unordered_map<ExpressionType, idx_t> func_map;
    idx_t func_num = 0;
	idx_t depth = 5;

	string body = GetSubstraitInfo(func_map, func_num, depth);
	string header = GetHeader(func_map, func_num, 1);

	depth = 1;
	string interface = AssignBlank(depth) + "\"relations\": [\n";
	interface += AssignBlank(++depth) + "{\n";
	interface += AssignBlank(++depth) + "\"root\" {\n";
	interface += AssignBlank(++depth) + "\"names\": [\n";
	interface += AssignBlank(depth) + "],\n";
	interface += AssignBlank(depth) + "\"input\": {\n";
	interface += body;
	interface += AssignBlank(depth) + "}\n";
	interface += AssignBlank(--depth) + "}\n";
	interface += AssignBlank(--depth) + "}\n";
	interface += AssignBlank(--depth) + "]\n";

	return "{\n" + header + interface + "}";
}

string PhysicalOperator::ToString(idx_t depth) const {
	string extra_info = StringUtil::Replace(ExtraRenderInformation(), "\n", " ");
	StringUtil::RTrim(extra_info);
	if (!extra_info.empty()) {
		extra_info = "[" + extra_info + "]";
	}
	string result = PhysicalOperatorToString(type) + extra_info;
	if (children.size() > 0) {
		for (idx_t i = 0; i < children.size(); i++) {
			result += "\n" + string(depth * 4, ' ');
			auto &child = children[i];
			result += child->ToString(depth + 1);
		}
		result += "";
	}
	return result;
}

static string ToJSONRecursive(const PhysicalOperator &node) {
	string result = "{ \"name\": \"" + PhysicalOperatorToString(node.type) + "\",\n";
	result += "\"timing\":" + StringUtil::Format("%.2f", 0) + ",\n";
	result += "\"cardinality\":" + to_string(0) + ",\n";
	result += "\"extra_info\": \"" + StringUtil::Replace(node.ExtraRenderInformation(), "\n", " ") + "\",\n";
	result += "\"children\": [";
	result += StringUtil::Join(node.children, node.children.size(), ",\n",
	                           [](const unique_ptr<PhysicalOperator> &child) { return ToJSONRecursive(*child); });
	result += "]\n}\n";
	return result;
}

string PhysicalOperator::ToJSON() const {
	string result = "{ \"result\": " + to_string(0.1) + ",\n";
	// print the phase timings
	result += "\"timings\": {},\n";
	// recursively print the physical operator tree
	result += "\"tree\": ";
	result += ToJSONRecursive(*this);

	return result + "}";
}

PhysicalOperatorState::PhysicalOperatorState(PhysicalOperator *child) : finished(false) {
	if (child) {
		child->InitializeChunk(child_chunk);
		child_state = child->GetOperatorState();
	}
}

void PhysicalOperator::GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state,
                                SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	if (context.interrupted) {
		throw InterruptException();
	}

	chunk.Reset();
	if (state->finished) {
		return;
	}

	context.profiler.StartOperator(this);
	GetChunkInternal(context, chunk, state, sel, rid_vector, rai_chunk);
	context.profiler.EndOperator(chunk);

	chunk.Verify();
}

void PhysicalOperator::Print() {
	Printer::Print(ToString());
}
