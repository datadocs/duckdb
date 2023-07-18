#define DUCKDB_EXTENSION_MAIN

#include "datadocs_extension.hpp"

namespace duckdb {

void DatadocsExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();
	LoadGeo(con);
	LoadVariant(con);
	LoadIngest(con);
	con.Commit();
}

string DatadocsExtension::Name() {
	return "datadocs";
}

} // namespace duckdb
