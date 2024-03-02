#include "datadocs.hpp"
#include "datadocs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "json_common.hpp"
#include "inferrer_impl.h"

namespace duckdb {

namespace {

struct IngestBindData : public TableFunctionData {
	explicit IngestBindData(const string &file_name, ClientContext &context)
	    : parser(Parser::get_parser(file_name, context)) {
	}

	void BindSchema(std::vector<LogicalType> &return_types, std::vector<string> &names) {
		parser->BindSchema(return_types, names);
	}

	std::unique_ptr<Parser> parser;
};

static unique_ptr<FunctionData> IngestBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	if (!DBConfig::GetConfig(context).options.enable_external_access) {
		throw PermissionException("Scanning external files is disabled through configuration");
	}
	const string &file_name = StringValue::Get(input.inputs[0]);
	auto result = make_uniq<IngestBindData>(file_name, context);
	Parser &parser = *result->parser;
	if (input.inputs.size() > 1) {
		JSONAllocator alc {Allocator::DefaultAllocator()};
		auto doc = JSONCommon::ReadDocument(StringValue::Get(input.inputs[1]), JSONCommon::READ_FLAG, alc.GetYYAlc());
		auto root = yyjson_doc_get_root(doc);
		yyjson_val *val = yyjson_obj_get(root, "path");
		if (yyjson_is_str(val)) {
			parser.select_path(std::string_view(unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val)));
		} else if (yyjson_is_arr(val)) {
			parser.select_path(val);
		}
		parser.get_schema()->FromJson(root); //yyjson_obj_get(root, "options"));
	} else {
		while (parser.get_file_count() > 0) {
			parser.select_file(0);
		}
		if (!parser.infer_schema()) {
			throw InvalidInputException("Cannot ingest file");
		}
	}
	parser.BuildColumns();
	parser.BindSchema(return_types, names);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> IngestInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<IngestBindData>();
	bind_data.parser->open();
	return nullptr;
}

static void IngestImpl(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<IngestBindData>();
	auto &parser = *bind_data.parser;
	if (parser.is_finished) {
		return;
	}
	idx_t n_rows = parser.FillChunk(output);
	output.SetCardinality(n_rows);
}

unique_ptr<TableRef> ReadIngestReplacement(ClientContext &context, const string &table_name,
                                           ReplacementScanData *data) {
	auto lower_name = StringUtil::Lower(table_name);
	// remove any compression
	if (StringUtil::EndsWith(lower_name, ".gz")) {
		lower_name = lower_name.substr(0, lower_name.size() - 3);
	} else if (StringUtil::EndsWith(lower_name, ".zst")) {
		lower_name = lower_name.substr(0, lower_name.size() - 4);
	}
	if (!StringUtil::EndsWith(lower_name, ".csv") && !StringUtil::Contains(lower_name, ".csv?") &&
	    !StringUtil::EndsWith(lower_name, ".tsv") && !StringUtil::Contains(lower_name, ".tsv?") &&
	    !StringUtil::EndsWith(lower_name, ".json") && !StringUtil::Contains(lower_name, ".json?") &&
	    !StringUtil::EndsWith(lower_name, ".jsonl") && !StringUtil::Contains(lower_name, ".jsonl?") &&
	    !StringUtil::EndsWith(lower_name, ".ndjson") && !StringUtil::Contains(lower_name, ".ndjson?")) {
		return nullptr;
	}
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	table_function->function = make_uniq<FunctionExpression>("ingest_file", std::move(children));

	if (!FileSystem::HasGlob(table_name)) {
		auto &fs = FileSystem::GetFileSystem(context);
		table_function->alias = fs.ExtractBaseName(table_name);
	}

	return std::move(table_function);
}

} // namespace

void DatadocsExtension::LoadIngest(DatabaseInstance &inst) {
	TableFunctionSet ingest_set("ingest_file");
	ingest_set.AddFunction(TableFunction({LogicalType::VARCHAR}, IngestImpl, IngestBind, IngestInit));
	ingest_set.AddFunction(TableFunction({LogicalType::VARCHAR, DDJsonType}, IngestImpl, IngestBind, IngestInit));
	ExtensionUtil::RegisterFunction(inst, ingest_set);

	auto &config = DBConfig::GetConfig(inst);
	config.replacement_scans.emplace(config.replacement_scans.begin(), ReadIngestReplacement);
}

} // namespace duckdb
