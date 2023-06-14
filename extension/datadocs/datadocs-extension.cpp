#define DUCKDB_EXTENSION_MAIN

#include "datadocs-extension.hpp"

namespace duckdb {

void DataDocsExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();
	LoadGeo(con);
	LoadVariant(con);
	LoadIngest(con);
	con.Commit();
}

string DataDocsExtension::Name() {
	return "datadocs";
}

} // namespace duckdb
