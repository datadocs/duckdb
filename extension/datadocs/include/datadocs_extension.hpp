#pragma once

#include "duckdb.hpp"

namespace duckdb {

class DatadocsExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	string Name() override;

private:
	void LoadGeo(Connection &con);
	void LoadVariant(Connection &con);
	void LoadIngest(Connection &con);
	void LoadParseNum(Connection &con);
};

} // namespace duckdb
