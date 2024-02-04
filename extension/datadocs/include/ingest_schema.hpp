#pragma once

#include "duckdb.hpp"

namespace duckdb {

std::string ingest_get_schema(Connection &conn, const std::string &file_name, const std::string &path);

} // namespace duckdb
