#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#endif

#include "datadocs_extension.hpp"
#include "datadocs.hpp"
#include "inferrer.h"

namespace duckdb {

namespace {

struct IngestBindData : public TableFunctionData {
	explicit IngestBindData(const string &file_name)
	    : parser(Parser::get_parser(file_name)) {
	}

	void BindSchema(vector<LogicalType> &return_types, vector<string> &names) {
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
	auto result = ::duckdb::make_uniq<IngestBindData>(file_name);
	result->BindSchema(return_types, names);
	return result;
}

static unique_ptr<GlobalTableFunctionState> IngestInit(ClientContext &context, TableFunctionInitInput &input) {
	auto *bind_data = (IngestBindData *)input.bind_data.get();
	bind_data->parser->open();
	return nullptr;
}

static void IngestImpl(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto *bind_data = (IngestBindData *)data_p.bind_data.get();
	auto &parser = *bind_data->parser;
	if (parser.is_finished) {
		return;
	}
	idx_t n_rows = parser.FillChunk(output);
	output.SetCardinality(n_rows);
}

} // namespace

void DatadocsExtension::LoadIngest(Connection &con) {
	auto &context = *con.context;
	auto &catalog = Catalog::GetSystemCatalog(context);
	CreateTableFunctionInfo schema_info(
	    TableFunction("ingest_file", {LogicalType::VARCHAR}, IngestImpl, IngestBind, IngestInit));
	catalog.CreateTableFunction(context, &schema_info);
}

} // namespace duckdb
