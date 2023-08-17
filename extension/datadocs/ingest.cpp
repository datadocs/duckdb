#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"

#include "datadocs_extension.hpp"
#include "datadocs.hpp"
#include "inferrer.h"

namespace duckdb {

namespace {

struct IngestBindData : public TableFunctionData {
	explicit IngestBindData(const string &file_name, ClientContext &context)
	    : parser(Parser::get_parser(file_name, context)) {
		parser->select_file(0);
	}

	void BindSchema(std::vector<LogicalType> &return_types, std::vector<string> &names) {
		if (!parser->infer_schema()) {
			throw InvalidInputException("Cannot ingest file");
		}
		parser->BuildColumns();
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
	result->BindSchema(return_types, names);
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

} // namespace

void DatadocsExtension::LoadIngest(DatabaseInstance &inst) {
	ExtensionUtil::RegisterFunction(
	    inst, TableFunction("ingest_file", {LogicalType::VARCHAR}, IngestImpl, IngestBind, IngestInit));
}

} // namespace duckdb
