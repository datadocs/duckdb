#pragma once

#include "duckdb.hpp"

namespace duckdb {

class DatadocsExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	string Name() override;

private:
	void LoadGeo(DatabaseInstance &inst);
	void LoadVariant(DatabaseInstance &inst);
	void LoadIngest(DatabaseInstance &inst);
	void LoadParseNum(DatabaseInstance &inst);
};

} // namespace duckdb
