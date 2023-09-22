#pragma once

#include "duckdb.hpp"
#include "vector_proxy.hpp"

struct yyjson_val;
struct yyjson_alc;

namespace duckdb {

constexpr int dd_numeric_width = 38;
constexpr int dd_numeric_scale = 9;

extern LogicalType DDGeoType;
extern const LogicalType DDJsonType;
extern const LogicalType DDNumericType;
extern const LogicalType DDVariantType;

extern bool IsDatetime(LogicalType type);
extern bool IsDecayableType(LogicalType type);
extern Value GetDecayVariantValue(Value v, yyjson_alc *alc);
extern Value GetDecayJsonValue(Value v, yyjson_alc *alc);
extern bool VariantReadScalar(VectorWriter &result, yyjson_val *val, LogicalType type, bool is_list, yyjson_val *info);
extern bool VariantToJson(VectorWriter &result, Value v);
bool VariantWriteValue(VectorWriter &result, Value v);
extern LogicalType ConvertLogicalTypeFromString(std::string type);
extern LogicalType ConvertLogicalTypeFromJson(yyjson_val *type_info);

} // namespace duckdb
