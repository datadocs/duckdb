#pragma once

#include "duckdb.hpp"

namespace duckdb {

class DataDocsExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	string Name() override;

private:
	void LoadGeo(Connection &con);
	void LoadVariant(Connection &con);
	void LoadIngest(Connection &con);
};

} // namespace duckdb
