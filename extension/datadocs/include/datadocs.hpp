#pragma once

#include "duckdb.hpp"
#include "vector_proxy.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

constexpr int dd_numeric_width = 38;
constexpr int dd_numeric_scale = 9;

/**
 * @brief name of optional arguments for function `comparison_any`, `any_in_array` and `sort_hash`
 *
 */
enum class ComparisonArgumentType : uint8_t { ci, keys_ci, ansi_null };

static string ci_str = "ci";
static string keys_ci_str = "keys_ci";
static string ansi_null_str = "ansi_nulls";

/**
 * @brief Default value of optional arguments
 * ci: true
 * keys_ci: true
 */
static bool ci_default_value = true;
static bool keys_ci_default_value = true;
static bool ansi_nulls_default_value = false;

/**
 * @brief Comparison type, ranking types for compare between types in case values couldn't convertible
 *
 */
enum class ComparisonType : uint8_t {
	INVALID,
	C_NULL,
	BOOL,
	NUMERIC,
	STRING,
	BYTES,
	TIME,
	DATETIME,
	INTERVAL,
	GEO,
	LIST,
	STRUCT
};

/**
 * @brief Result constant for `comparison_any` function
 * 0: equal
 * -1: less than
 * 1: bigger than
 *
 */
static int COMPARISON_RS_IS_NULL = -2;
static int COMPARISON_RS_EQUAL = 0;
static int COMPARISON_RS_LESS = -1;
static int COMPARISON_RS_BIGGER = 1;

extern LogicalType DDGeoType;
extern const LogicalType DDJsonType;
extern const LogicalType DDNumericType;
extern const LogicalType DDVariantType;
extern const LogicalType DDVariantArrayType;

extern bool IsDatetime(LogicalType type);
extern string ToLowerCase(string str);
extern bool IsDecayableType(LogicalType type);
extern Value GetDecayVariantValue(Value v, yyjson_alc *alc);
extern Value GetDecayJsonValue(Value v, yyjson_alc *alc);
extern bool VariantReadScalar(VectorWriter &result, yyjson_val *val, LogicalType type, bool is_list, yyjson_val *info);
extern bool VariantToJson(VectorWriter &result, Value v);
bool VariantWriteValue(VectorWriter &result, Value v);
extern LogicalType ConvertLogicalTypeFromString(std::string type);
extern LogicalType ConvertLogicalTypeFromJson(yyjson_val *type_info);
extern void TransformVariantArrayFunc(Vector &source, Vector &result, idx_t count);

} // namespace duckdb
