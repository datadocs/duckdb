#define DUCKDB_EXTENSION_MAIN

#include "datadocs_extension.hpp"

namespace duckdb {

void DatadocsExtension::Load(DuckDB &db) {
	auto &inst = *db.instance;
	LoadGeo(inst);
	LoadVariant(inst);
	LoadIngest(inst);
	LoadParseNum(inst);
}

string DatadocsExtension::Name() {
	return "datadocs";
}

} // namespace duckdb
