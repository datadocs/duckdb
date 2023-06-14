#pragma once

#include "duckdb.hpp"

namespace duckdb {

constexpr int dd_numeric_width = 38;
constexpr int dd_numeric_scale = 9;

extern LogicalType DDGeoType;
extern const LogicalType DDJsonType;
extern const LogicalType DDNumericType;
extern const LogicalType DDVariantType;

} // namespace duckdb
