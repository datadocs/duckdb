#include "datadocs.hpp"
#include "datadocs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

/**
 * @brief Function bind information for list_transform function in case `ci` is true
 * create expression covert list of string to lowercase
 * reference src/core_functions/scalar/list_functions.cpp
 *
 */
struct CompareListLambdaBindData : public FunctionData {
	CompareListLambdaBindData(const LogicalType &stype_p, unique_ptr<Expression> lambda_expr);
	~CompareListLambdaBindData() override;

	LogicalType stype;
	unique_ptr<Expression> lambda_expr;

public:
	bool Equals(const FunctionData &other_p) const override;
	unique_ptr<FunctionData> Copy() const override;
	static void Serialize(FieldWriter &writer, const FunctionData *bind_data_p, const ScalarFunction &function) {
		throw NotImplementedException("FIXME: list lambda serialize");
	}
	static unique_ptr<FunctionData> Deserialize(PlanDeserializationState &state, FieldReader &reader,
	                                            ScalarFunction &bound_function) {
		throw NotImplementedException("FIXME: list lambda deserialize");
	}

	static void FormatSerialize(FormatSerializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                            const ScalarFunction &function) {
		auto &bind_data = bind_data_p->Cast<CompareListLambdaBindData>();
		serializer.WriteProperty(100, "stype", bind_data.stype);
		serializer.WriteOptionalProperty(101, "lambda_expr", bind_data.lambda_expr);
	}

	static unique_ptr<FunctionData> FormatDeserialize(FormatDeserializer &deserializer, ScalarFunction &function) {
		auto stype = deserializer.ReadProperty<LogicalType>(100, "stype");
		auto lambda_expr = deserializer.ReadOptionalProperty<unique_ptr<Expression>>(101, "lambda_expr");
		return make_uniq<CompareListLambdaBindData>(stype, std::move(lambda_expr));
	}
};

CompareListLambdaBindData::CompareListLambdaBindData(const LogicalType &stype_p, unique_ptr<Expression> lambda_expr_p)
    : stype(stype_p), lambda_expr(std::move(lambda_expr_p)) {
}

unique_ptr<FunctionData> CompareListLambdaBindData::Copy() const {
	return make_uniq<CompareListLambdaBindData>(stype, lambda_expr ? lambda_expr->Copy() : nullptr);
}

bool CompareListLambdaBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<CompareListLambdaBindData>();
	return Expression::Equals(lambda_expr, other.lambda_expr) && stype == other.stype;
}

CompareListLambdaBindData::~CompareListLambdaBindData() {
}

/**
 * @brief Check logical type is datetime/date
 * datetime and date types can covertible
 *
 * @param type
 * @return true
 * @return false
 */
static bool TypeIncludeDate(LogicalType type) {
	if (IsDatetime(type) && type != LogicalType::TIME) {
		return true;
	}
	return false;
}

/**
 * @brief Get Comparison type from LogicalType for compare between two types
 *
 * @param type
 * @return ComparisonType
 */
static ComparisonType LogicalTypeToComparisonType(LogicalType type) {
	if (type.IsNumeric()) {
		return ComparisonType::NUMERIC;
	}
	if (TypeIncludeDate(type)) {
		return ComparisonType::DATETIME;
	}
	if (type == DDGeoType) {
		return ComparisonType::GEO;
	}
	switch (type.id()) {
	case LogicalType::BOOLEAN: {
		return ComparisonType::BOOL;
	}

	case LogicalType::VARCHAR: {
		return ComparisonType::STRING;
	}

	case LogicalType::BLOB: {
		return ComparisonType::BYTES;
	}

	case LogicalType::INTERVAL: {
		return ComparisonType::INTERVAL;
	}

	case LogicalType::TIME: {
		return ComparisonType::TIME;
	}

	case LogicalTypeId::LIST: {
		return ComparisonType::LIST;
	}

	case LogicalTypeId::STRUCT: {
		return ComparisonType::STRUCT;
	}

	default:
		throw InternalException("Unimplemented comparison type for Logical Type %s", type.ToString());
		break;
	}

	return ComparisonType::INVALID;
}

/**
 * @brief Check Logical type is decayable type (JSON/VARIANT)
 *
 * @param type
 * @return true
 * @return false
 */
bool IsDecayableType(LogicalType type) {
	return type == DDJsonType || type == DDVariantType;
}

/**
 * @brief Check Logical type is nested type (LIST/STRUCT)
 *
 * @param type
 * @return true
 * @return false
 */
static bool IsNestedType(LogicalType type) {
	return type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::STRUCT;
}

/**
 * @brief Compare between two logical type base on comparison types
 *
 * @param type1
 * @param type2
 * @return int
 */
static int CompareType(LogicalType type1, LogicalType type2) {
	if ((type1 == type2) || (TypeIncludeDate(type1) && TypeIncludeDate(type2)) ||
	    (type1.IsNumeric() && type2.IsNumeric())) {
		return COMPARISON_RS_EQUAL;
	}
	auto comparison_type1 = LogicalTypeToComparisonType(type1);
	auto comparison_type2 = LogicalTypeToComparisonType(type2);
	if (comparison_type1 == comparison_type2) {
		return COMPARISON_RS_EQUAL;
	} else if (comparison_type1 < comparison_type2) {
		return COMPARISON_RS_LESS;
	} else {
		return COMPARISON_RS_BIGGER;
	}
}

/**
 * @brief Get the Decay Variant Value object
 * Get original value from variant value
 *
 * @param v
 * @param alc
 * @return Value
 */
Value GetDecayVariantValue(Value v, yyjson_alc *alc) {
	D_ASSERT(v.type() == DDVariantType);
	auto &children = StructValue::GetChildren(v);
	if (children.size() != 3) {
		throw SyntaxException("Unimplemented decay value from variant %s", v.ToString());
		return Value(LogicalType::SQLNULL);
	}
	yyjson_val *info = nullptr;
	LogicalType child_type;
	auto doc = JSONCommon::ReadDocument(children[1].GetValueUnsafe<string_t>(), JSONCommon::READ_FLAG, alc);
	yyjson_val *val = yyjson_doc_get_root(doc);
	if (children[2].IsNull()) {
		child_type = ConvertLogicalTypeFromString(StringValue::Get(children[0]));
	} else {
		auto type_info_doc =
		    JSONCommon::ReadDocument(children[2].GetValueUnsafe<string_t>(), JSONCommon::READ_FLAG, alc);
		child_type = ConvertLogicalTypeFromJson(yyjson_doc_get_root(type_info_doc));
		info = yyjson_doc_get_root(type_info_doc);
	}
	Vector res(child_type);
	VectorWriter writer(res, 0);
	if (!val || unsafe_yyjson_is_null(val)) {
		return Value(LogicalType::SQLNULL);
	} else if (VariantReadScalar(writer, val, child_type, false, info)) {
		return res.GetValue(0);
	} else {
		return Value(LogicalType::SQLNULL);
	}
}

/**
 * @brief Get the Decay Json Value From Val object
 * Get original value from json value base on json type
 *
 * @param val
 * @param alc
 * @return Value
 */
static Value GetDecayJsonValueFromVal(yyjson_val *val, yyjson_alc *alc) {
	auto json_type = JSONCommon::ValTypeToStringT(val);
	if (json_type == JSONCommon::TYPE_STRING_VARCHAR) {
		return Value(yyjson_get_str(val));
	} else if (json_type == JSONCommon::TYPE_STRING_UBIGINT) {
		return Value::UBIGINT(yyjson_get_uint(val));
	} else if (json_type == JSONCommon::TYPE_STRING_BIGINT) {
		return Value::BIGINT(yyjson_get_int(val));
	} else if (json_type == JSONCommon::TYPE_STRING_BOOLEAN) {
		return Value::BOOLEAN(yyjson_get_bool(val));
	} else if (json_type == JSONCommon::TYPE_STRING_NULL) {
		return Value(LogicalType::SQLNULL);
	} else if (json_type == JSONCommon::TYPE_STRING_DOUBLE) {
		return Value::DOUBLE(yyjson_get_real(val));
	} else if (json_type == JSONCommon::TYPE_STRING_ARRAY) {
		size_t idx, max;
		yyjson_val *child_val;
		vector<Value> vals(yyjson_arr_size(val));
		yyjson_arr_foreach(val, idx, max, child_val) {
			auto child_v = JSONCommon::WriteVal(child_val, alc);
			vals[idx] = Value(child_v);
		}
		return Value::LIST(DDJsonType, vals);
	} else if (json_type == JSONCommon::TYPE_STRING_OBJECT) {
		child_list_t<Value> children;
		child_list_t<LogicalType> child_types;
		size_t idx, max;
		yyjson_val *k, *v;
		yyjson_obj_foreach(val, idx, max, k, v) {
			string kstr = yyjson_get_str(k);
			auto child_v = JSONCommon::WriteVal(v, alc);
			children.push_back(make_pair(kstr, Value(child_v).DefaultCastAs(DDJsonType)));
		}
		return Value::STRUCT(children);
	}
	throw SyntaxException("Unimplemented decay value from json %s", yyjson_get_str(val));
	return Value(LogicalType::SQLNULL);
}

/**
 * @brief Get the Decay Json Value object
 *
 * @param v
 * @param alc
 * @return Value
 */
Value GetDecayJsonValue(Value v, yyjson_alc *alc) {
	auto doc = JSONCommon::ReadDocument(v.GetValueUnsafe<string_t>(), JSONCommon::READ_FLAG, alc);
	yyjson_val *val = yyjson_doc_get_root(doc);
	return GetDecayJsonValueFromVal(val, alc);
}

/**
 * @brief Compare between two Value object
 * if one of two value types is decayable type => get original value of decayable value and compare
 * if two value is nested value
 * 		- difference types: compare type
 * 		- both list (array): compare each value of list
 * 		- both struct (object):
 * 				+ sorting keys first
 * 				+ compare each key, value
 * remaining case:
 * 		- can't cast between two types: compare between types
 * 		- cast between two values and compare
 *
 * @param v1
 * @param v2
 * @param ci
 * @param keys_ci
 * @return int
 */
static int CompareAnyValue(Value v1, Value v2, bool ci, bool keys_ci) {
	auto type1 = v1.type();
	auto type2 = v2.type();
	JSONAllocator alc {Allocator::DefaultAllocator()};
	if (IsDecayableType(type1) || IsDecayableType(type2)) {
		// Handle for one of two value is decay able type
		Value new_v1 = v1;
		Value new_v2 = v2;
		if (type1 == DDVariantType) {
			new_v1 = GetDecayVariantValue(v1, alc.GetYYAlc());
		} else if (type1 == DDJsonType) {
			new_v1 = GetDecayJsonValue(v1, alc.GetYYAlc());
		}
		if (type2 == DDVariantType) {
			new_v2 = GetDecayVariantValue(v2, alc.GetYYAlc());
		} else if (type2 == DDJsonType) {
			new_v2 = GetDecayJsonValue(v2, alc.GetYYAlc());
		}
		return CompareAnyValue(new_v1, new_v2, ci, keys_ci);
	} else if (IsNestedType(type1) && IsNestedType(type2)) {
		// Handle for one of two value is nested type
		if (type1.id() != type2.id()) {
			if (type1.id() < type2.id()) {
				return COMPARISON_RS_LESS;
			} else {
				return COMPARISON_RS_BIGGER;
			}
		} else {
			if (type1.id() == LogicalTypeId::LIST) {
				auto values1 = ListValue::GetChildren(v1);
				auto values2 = ListValue::GetChildren(v2);
				auto minsize = std::min(values1.size(), values2.size());
				auto compare_value = 0;
				for (idx_t i = 0; i < minsize; i++) {
					compare_value = CompareAnyValue(values1[i], values2[i], ci, keys_ci);
					if (compare_value != COMPARISON_RS_EQUAL) {
						break;
					}
				}
				if (compare_value == COMPARISON_RS_EQUAL && values1.size() != values2.size()) {
					if (values1.size() < values2.size()) {
						return COMPARISON_RS_LESS;
					} else {
						return COMPARISON_RS_BIGGER;
					}
				} else {
					return compare_value;
				}
			} else {
				child_list_t<LogicalType> child_types1 = StructType::GetChildTypes(v1.type());
				if (keys_ci) {
					std::transform(child_types1.begin(), child_types1.end(), child_types1.begin(),
					               [](std::pair<string, LogicalType> item) {
						               return std::pair<string, LogicalType>(ToLowerCase(item.first), item.second);
					               });
				}
				std::map<string, idx_t> key_idx_map1 {};
				auto values1 = StructValue::GetChildren(v1);
				for (idx_t i = 0; i < child_types1.size(); i++) {
					auto key = keys_ci ? ToLowerCase(child_types1[i].first) : child_types1[i].first;
					key_idx_map1.insert(std::pair<string, idx_t>(key, i));
				}
				// sort child types 1 by key
				std::sort(child_types1.begin(), child_types1.end(),
				          [](std::pair<string, LogicalType> const &a, std::pair<string, LogicalType> const &b) {
					          return a.first < b.first;
				          });
				auto child_types2 = StructType::GetChildTypes(v2.type());
				if (keys_ci) {
					std::transform(child_types2.begin(), child_types2.end(), child_types2.begin(),
					               [](std::pair<string, LogicalType> item) {
						               return std::pair<string, LogicalType>(ToLowerCase(item.first), item.second);
					               });
				}
				std::map<string, idx_t> key_idx_map2 {};
				auto values2 = StructValue::GetChildren(v2);
				for (idx_t i = 0; i < child_types2.size(); i++) {
					auto key = keys_ci ? ToLowerCase(child_types2[i].first) : child_types2[i].first;
					key_idx_map2.insert(std::pair<string, idx_t>(key, i));
				}
				// sort child types 2 by key
				std::sort(child_types2.begin(), child_types2.end(),
				          [](std::pair<string, LogicalType> const &a, std::pair<string, LogicalType> const &b) {
					          return a.first < b.first;
				          });
				auto struct_size = std::min(child_types1.size(), child_types2.size());
				auto compare_value = 0;
				for (idx_t i = 0; i < struct_size; i++) {
					auto key1 = keys_ci ? ToLowerCase(child_types1[i].first) : child_types1[i].first;
					auto key2 = keys_ci ? ToLowerCase(child_types2[i].first) : child_types2[i].first;
					if (key1 != key2) {
						return key1 > key2 ? COMPARISON_RS_BIGGER : COMPARISON_RS_LESS;
					}
					compare_value =
					    CompareAnyValue(values1[key_idx_map1[key1]], values2[key_idx_map2[key2]], ci, keys_ci);
					if (compare_value != COMPARISON_RS_EQUAL) {
						break;
					}
				}
				if (compare_value == COMPARISON_RS_EQUAL && values1.size() != values2.size()) {
					if (values1.size() < values2.size()) {
						return COMPARISON_RS_LESS;
					} else {
						return COMPARISON_RS_BIGGER;
					}
				}
				return compare_value;
			}
		}
	} else {
		Value new_v1 = v1;
		Value new_v2 = v2;
		bool v1_isnull = v1.IsNull();
		bool v2_isnull = v2.IsNull();
		if (v1_isnull && v2_isnull) {
			return COMPARISON_RS_EQUAL;
		} else if (v1_isnull || v2_isnull) {
			return v1_isnull ? COMPARISON_RS_LESS : COMPARISON_RS_BIGGER;
		}
		if ((type1 == type2) || (TypeIncludeDate(type1) && TypeIncludeDate(type2)) ||
		    (type1.IsNumeric() && type2.IsNumeric())) {
			new_v1 = (ci && v1.type() == LogicalType::VARCHAR) ? Value(ToLowerCase(v1.GetValue<string>())) : v1;
			new_v2 = (ci && v2.type() == LogicalType::VARCHAR) ? Value(ToLowerCase(v2.GetValue<string>())) : v2;
			if (new_v1 == new_v2) {
				return COMPARISON_RS_EQUAL;
			} else if (new_v1 < new_v2) {
				return COMPARISON_RS_LESS;
			} else if (new_v1 > new_v2) {
				return COMPARISON_RS_BIGGER;
			}
		}
		return CompareType(type1, type2);
	}
	return 1;
}

/**
 * @brief Comparison Operation between orginal values
 *
 */
struct CompareAny {
	template <typename T>
	static inline int Operation(const T &left, const T &right) {
		if (left == right)
			return COMPARISON_RS_EQUAL;
		if (left < right)
			return COMPARISON_RS_LESS;
		return COMPARISON_RS_BIGGER;
	}
};

/**
 * @brief Executor for compare any between two vector with same type
 *
 * @tparam T
 * @tparam OP
 * @param left
 * @param right
 * @param result
 * @param count
 */
template <class T, class OP>
static inline void TemplatedCompareAnyExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
	auto ldata = FlatVector::GetData<T>(left);
	auto rdata = FlatVector::GetData<T>(right);
	auto left_vector_type = left.GetVectorType();
	auto right_vector_type = right.GetVectorType();
	auto LEFT_CONSTANT = left_vector_type == VectorType::CONSTANT_VECTOR;
	auto RIGHT_CONSTANT = right_vector_type == VectorType::CONSTANT_VECTOR;

	UnifiedVectorFormat vdata1, vdata2;
	left.ToUnifiedFormat(count, vdata1);
	right.ToUnifiedFormat(count, vdata2);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<int>(result);
	auto left_valid = LEFT_CONSTANT && !ConstantVector::IsNull(left);
	auto right_valid = RIGHT_CONSTANT && !ConstantVector::IsNull(right);

	for (idx_t i = 0; i < count; i++) {
		auto idx1 = vdata1.sel->get_index(i);
		auto idx2 = vdata2.sel->get_index(i);
		bool v1_valid = LEFT_CONSTANT ? left_valid : vdata1.validity.RowIsValid(idx1);
		bool v2_valid = RIGHT_CONSTANT ? right_valid : vdata2.validity.RowIsValid(idx2);
		if (v1_valid && v2_valid) {
			auto lentry = ldata[LEFT_CONSTANT ? 0 : idx1];
			auto rentry = rdata[RIGHT_CONSTANT ? 0 : idx2];
			result_data[i] = OP::Operation(lentry, rentry);
		} else if (!v1_valid && !v2_valid) {
			result_data[i] = COMPARISON_RS_EQUAL;
		} else {
			result_data[i] = v1_valid ? COMPARISON_RS_BIGGER : COMPARISON_RS_LESS;
		}
	}
}

/**
 * @brief Compare any for two vector same type except decayable/nested types
 *
 * @param left
 * @param right
 * @param result
 * @param count
 */
static void CompareAnyBaseVectors(Vector &left, Vector &right, Vector &result, idx_t count) {
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedCompareAnyExecute<int8_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::INT16:
		TemplatedCompareAnyExecute<int16_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::INT32:
		TemplatedCompareAnyExecute<int32_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::INT64:
		TemplatedCompareAnyExecute<int64_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::UINT8:
		TemplatedCompareAnyExecute<uint8_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::UINT16:
		TemplatedCompareAnyExecute<uint16_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::UINT32:
		TemplatedCompareAnyExecute<uint32_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::UINT64:
		TemplatedCompareAnyExecute<uint64_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::INT128:
		TemplatedCompareAnyExecute<hugeint_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedCompareAnyExecute<float, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedCompareAnyExecute<double, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedCompareAnyExecute<interval_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedCompareAnyExecute<string_t, CompareAny>(left, right, result, count);
		break;
	default:
		throw InternalException("Invalid type for comparison");
	}
}

/**
 * @brief compare any vector for each values for general type
 *
 * @param left
 * @param right
 * @param result
 * @param count
 * @param ci
 * @param keys_ci
 */
static void CompareAnyVectorEachValue(Vector &left, Vector &right, Vector &result, idx_t count, bool ci, bool keys_ci) {
	UnifiedVectorFormat vdata1, vdata2;
	left.ToUnifiedFormat(count, vdata1);
	right.ToUnifiedFormat(count, vdata2);
	auto result_data = FlatVector::GetData<int>(result);
	auto left_type = left.GetType();
	auto right_type = right.GetType();
	for (idx_t i = 0; i < count; i++) {
		auto idx1 = vdata1.sel->get_index(i);
		auto idx2 = vdata2.sel->get_index(i);
		bool v1_valid = vdata1.validity.RowIsValid(idx1);
		bool v2_valid = vdata2.validity.RowIsValid(idx2);
		if ((v1_valid && v2_valid) || (v1_valid && IsDecayableType(left_type)) ||
		    (v2_valid && IsDecayableType(right_type))) {
			result_data[i] = CompareAnyValue(left.GetValue(idx1), right.GetValue(idx2), ci, keys_ci);
		} else if (!v1_valid && !v2_valid) {
			result_data[i] = COMPARISON_RS_EQUAL;
		} else {
			result_data[i] = v1_valid ? COMPARISON_RS_BIGGER : COMPARISON_RS_LESS;
		}
	}
}

/**
 * @brief Compare types between two logical type in case not same types and can't cast
 *
 * @param left
 * @param right
 * @param result
 * @param count
 */
static void CompareAnyVectorsType(Vector &left, Vector &right, Vector &result, idx_t count) {
	auto type1 = left.GetType();
	auto type2 = right.GetType();
	UnifiedVectorFormat vdata1, vdata2;
	left.ToUnifiedFormat(count, vdata1);
	right.ToUnifiedFormat(count, vdata2);
	auto result_data = FlatVector::GetData<int>(result);
	auto type_compare = CompareType(type1, type2);
	for (idx_t i = 0; i < count; i++) {
		auto idx1 = vdata1.sel->get_index(i);
		auto idx2 = vdata2.sel->get_index(i);
		bool v1_valid = vdata1.validity.RowIsValid(idx1);
		bool v2_valid = vdata2.validity.RowIsValid(idx2);
		if (v1_valid && v2_valid) {
			result_data[i] = type_compare;
		} else if (!v1_valid && !v2_valid) {
			result_data[i] = COMPARISON_RS_EQUAL;
		} else {
			result_data[i] = v1_valid ? COMPARISON_RS_BIGGER : COMPARISON_RS_LESS;
		}
	}
}

/**
 * @brief Compare between two nested types
 * if not same type: compare types
 * if same type
 * 		- both list: compare each value of list
 * 		- both struct:
 * 				+ sorting keys of each value
 * 				+ compare each pair of key and value
 *
 * @param left
 * @param right
 * @param result
 * @param count
 * @param ci
 * @param keys_ci
 */
static void CompareAnyNestedVectors(Vector &left, Vector &right, Vector &result, idx_t count, bool ci, bool keys_ci) {
	auto type1 = left.GetType();
	auto type2 = right.GetType();
	if (type1.id() != type2.id()) {
		CompareAnyVectorsType(left, right, result, count);
	} else if (type1.id() == LogicalTypeId::LIST) {
		CompareAnyVectorEachValue(left, right, result, count, ci, keys_ci);
	} else if (type1.id() == LogicalTypeId::STRUCT) {
		child_list_t<LogicalType> child_types1 = StructType::GetChildTypes(type1);
		if (keys_ci) {
			std::transform(child_types1.begin(), child_types1.end(), child_types1.begin(),
			               [](std::pair<string, LogicalType> item) {
				               return std::pair<string, LogicalType>(ToLowerCase(item.first), item.second);
			               });
		}
		std::map<string, idx_t> key_idx_map1 {};
		for (idx_t i = 0; i < child_types1.size(); i++) {
			auto key = keys_ci ? ToLowerCase(child_types1[i].first) : child_types1[i].first;
			key_idx_map1.insert(std::pair<string, idx_t>(key, i));
		}
		// sort child types 1 by key
		std::sort(child_types1.begin(), child_types1.end(),
		          [](std::pair<string, LogicalType> const &a, std::pair<string, LogicalType> const &b) {
			          return a.first < b.first;
		          });
		auto child_types2 = StructType::GetChildTypes(type2);
		if (keys_ci) {
			std::transform(child_types2.begin(), child_types2.end(), child_types2.begin(),
			               [](std::pair<string, LogicalType> item) {
				               return std::pair<string, LogicalType>(ToLowerCase(item.first), item.second);
			               });
		}
		std::map<string, idx_t> key_idx_map2 {};
		for (idx_t i = 0; i < child_types2.size(); i++) {
			auto key = keys_ci ? ToLowerCase(child_types2[i].first) : child_types2[i].first;
			key_idx_map2.insert(std::pair<string, idx_t>(key, i));
		}
		// sort child types 2 by key
		std::sort(child_types2.begin(), child_types2.end(),
		          [](std::pair<string, LogicalType> const &a, std::pair<string, LogicalType> const &b) {
			          return a.first < b.first;
		          });
		auto struct_size = std::min(child_types1.size(), child_types2.size());
		auto size_compare = child_types1.size() < child_types2.size()   ? COMPARISON_RS_LESS
		                    : child_types1.size() > child_types2.size() ? COMPARISON_RS_BIGGER
		                                                                : COMPARISON_RS_EQUAL;
		UnifiedVectorFormat vdata1, vdata2;
		left.ToUnifiedFormat(count, vdata1);
		right.ToUnifiedFormat(count, vdata2);
		auto result_data = FlatVector::GetData<int>(result);
		auto &left_children_vec = StructVector::GetEntries(left);
		auto &right_children_vec = StructVector::GetEntries(right);
		for (idx_t i = 0; i < count; i++) {
			auto idx1 = vdata1.sel->get_index(i);
			auto idx2 = vdata2.sel->get_index(i);
			bool v1_valid = vdata1.validity.RowIsValid(idx1);
			bool v2_valid = vdata2.validity.RowIsValid(idx2);
			if (v1_valid && v2_valid) {
				for (idx_t idx = 0; idx < struct_size; idx++) {
					auto key1 = keys_ci ? ToLowerCase(child_types1[idx].first) : child_types1[idx].first;
					auto key2 = keys_ci ? ToLowerCase(child_types2[idx].first) : child_types2[idx].first;
					if (key1 != key2) {
						result_data[i] = key1 > key2 ? COMPARISON_RS_BIGGER : COMPARISON_RS_LESS;
						break;
					}
					result_data[i] =
					    CompareAnyValue(left_children_vec[key_idx_map1[key1]]->GetValue(idx1),
					                    right_children_vec[key_idx_map2[key2]]->GetValue(idx2), ci, keys_ci);
					if (result_data[i] != COMPARISON_RS_EQUAL) {
						break;
					}
				}
				if (result_data[i] == COMPARISON_RS_EQUAL) {
					result_data[i] = size_compare;
				}
			} else if (!v1_valid && !v2_valid) {
				result_data[i] = COMPARISON_RS_EQUAL;
			} else {
				result_data[i] = v1_valid ? COMPARISON_RS_BIGGER : COMPARISON_RS_LESS;
			}
		}
	}
}

/**
 * @brief Compare between two vectors
 * - both constant vectors: call compare value functions
 * - one of two vector type is decayable type: call compare value functions
 * - both nested types: call compare nested vectors function
 * - check type of two vectors
 * 		+ Same types or can cast between two types:
 * 			1. same type: call compare any function for base vectors
 * 			2. cast: call compare value function
 * 		+ others: call compare types function
 *
 * @param left
 * @param right
 * @param result
 * @param count
 * @param ci
 * @param keys_ci
 */
static void CompareAnyVectors(Vector &left, Vector &right, Vector &result, idx_t count, bool ci, bool keys_ci) {
	auto type1 = left.GetType();
	auto type2 = right.GetType();
	if (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		CompareAnyVectorEachValue(left, right, result, count, ci, keys_ci);
	} else if (IsDecayableType(type1) || IsDecayableType(type2)) {
		// handle for Decay type
		CompareAnyVectorEachValue(left, right, result, count, ci, keys_ci);
	} else if (IsNestedType(type1) && IsNestedType(type2)) {
		// handle for nested type
		CompareAnyNestedVectors(left, right, result, count, ci, keys_ci);
	} else {
		// handle for base type
		if ((type1 == type2) || (TypeIncludeDate(type1) && TypeIncludeDate(type2)) ||
		    (type1.IsNumeric() && type2.IsNumeric())) {
			if (type1 == type2) {
				CompareAnyBaseVectors(left, right, result, count);
			} else {
				CompareAnyVectorEachValue(left, right, result, count, ci, keys_ci);
			}
		} else {
			// Compare Type
			CompareAnyVectorsType(left, right, result, count);
		}
	}
}

/**
 * @brief main body of compare any function
 *
 * @param args
 * @param state
 * @param result
 */
static void CompareAnyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType() == LogicalType::INTEGER);
	auto first_operand = args.data[0];
	auto second_operand = args.data[1];
	auto ci_vec = args.data[2];
	auto keys_ci_vec = args.data[3];
	D_ASSERT(ci_vec.GetType() == LogicalType::BOOLEAN);
	D_ASSERT(keys_ci_vec.GetType() == LogicalType::BOOLEAN);
	auto count = args.size();

	if (ci_vec.GetVectorType() != VectorType::CONSTANT_VECTOR ||
	    keys_ci_vec.GetVectorType() != VectorType::CONSTANT_VECTOR) {
		throw Exception("'ci' or 'keys_ci' must be constant!");
		return;
	}

	UnifiedVectorFormat vdata3, vdata4;
	ci_vec.ToUnifiedFormat(count, vdata3);
	keys_ci_vec.ToUnifiedFormat(count, vdata4);

	bool ci = ci_default_value;
	bool keys_ci = keys_ci_default_value;
	if (count > 0) {
		ci = vdata3.validity.RowIsValid(0) ? ci_vec.GetValue(0).GetValue<bool>() : ci_default_value;
		keys_ci = vdata4.validity.RowIsValid(0) ? keys_ci_vec.GetValue(0).GetValue<bool>() : keys_ci_default_value;
	}

	CompareAnyVectors(first_operand, second_operand, result, count, ci, keys_ci);

	if (first_operand.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	    second_operand.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

/**
 * @brief check value in list of value or not
 *
 * @param left
 * @param right
 * @param result
 * @param count
 * @param ci
 * @param keys_ci
 */
static void AnyInArrayVectorEachValue(Vector &left, Vector &right, Vector &result, idx_t count, bool ci, bool keys_ci) {
	UnifiedVectorFormat vdata1, vdata2;
	left.ToUnifiedFormat(count, vdata1);
	right.ToUnifiedFormat(count, vdata2);
	auto result_data = FlatVector::GetData<bool>(result);
	auto null_val = Value(LogicalType::SQLNULL);
	for (idx_t i = 0; i < count; i++) {
		auto idx1 = vdata1.sel->get_index(i);
		auto idx2 = vdata2.sel->get_index(i);
		bool v1_valid = vdata1.validity.RowIsValid(idx1);
		bool v2_valid = vdata2.validity.RowIsValid(idx2);
		if (v2_valid) {
			auto left_val = v1_valid ? left.GetValue(idx1) : null_val;
			auto right_val = right.GetValue(idx2);
			auto children_vals = ListValue::GetChildren(right_val);
			bool in_array = false;
			for (idx_t i = 0; i < children_vals.size(); i++) {
				in_array = CompareAnyValue(left_val, children_vals[i], ci, keys_ci) == 0;
				if (in_array) {
					break;
				}
			}
			result_data[i] = in_array;
		} else {
			result_data[i] = false;
		}
	}
}

/**
 * @brief Check value in list of value in cast not same type and can't cast
 * true: value is null and list have null value element
 * false: other
 *
 * @param value_vector
 * @param list
 * @param result
 * @param count
 */
static void AnyInArrayVectorsNotEqualType(Vector &value_vector, Vector &list, Vector &result, idx_t count) {
	auto result_data = FlatVector::GetData<bool>(result);

	auto list_size = ListVector::GetListSize(list);
	auto &child_vector = ListVector::GetEntry(list);

	UnifiedVectorFormat child_data;
	child_vector.ToUnifiedFormat(list_size, child_data);

	UnifiedVectorFormat list_data;
	list.ToUnifiedFormat(count, list_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);

	UnifiedVectorFormat value_data;
	value_vector.ToUnifiedFormat(count, value_data);

	for (idx_t i = 0; i < count; i++) {
		auto value_index = value_data.sel->get_index(i);
		auto list_index = list_data.sel->get_index(i);
		bool value_valid = value_data.validity.RowIsValid(value_index);
		bool list_valid = list_data.validity.RowIsValid(list_index);
		if (list_valid) {
			if (value_valid) {
				result_data[i] = false;
			} else {
				// handle for case SELECT any_in_array(null, ['hi', null]);
				const auto &list_entry = list_entries[list_index];
				bool in_array = false;
				for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
					auto child_value_idx = child_data.sel->get_index(list_entry.offset + child_idx);
					if (!child_data.validity.RowIsValid(child_value_idx)) {
						in_array = true;
						break;
					}
				}
				result_data[i] = in_array;
			}
		} else {
			result_data[i] = false;
		}
	}
}

/**
 * @brief executor for any in array for base vectors (same types)
 * true: (value in list of values) or (null and null in list of value)
 * false: other
 *
 * @tparam T
 * @tparam OP
 * @param value_vector
 * @param list
 * @param result
 * @param count
 */
template <class T, class OP>
static inline void TemplatedAnyInArrayExecute(Vector &value_vector, Vector &list, Vector &result, idx_t count) {
	auto result_data = FlatVector::GetData<bool>(result);
	auto &result_validity = FlatVector::Validity(result);

	auto list_size = ListVector::GetListSize(list);
	auto &child_vector = ListVector::GetEntry(list);

	UnifiedVectorFormat child_data;
	child_vector.ToUnifiedFormat(list_size, child_data);

	UnifiedVectorFormat list_data;
	list.ToUnifiedFormat(count, list_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);

	UnifiedVectorFormat value_data;
	value_vector.ToUnifiedFormat(count, value_data);

	// not required for a comparison of nested types
	auto child_value = UnifiedVectorFormat::GetData<T>(child_data);
	auto values = UnifiedVectorFormat::GetData<T>(value_data);

	for (idx_t i = 0; i < count; i++) {
		auto value_index = value_data.sel->get_index(i);
		auto list_index = list_data.sel->get_index(i);
		bool value_valid = value_data.validity.RowIsValid(value_index);
		bool list_valid = list_data.validity.RowIsValid(list_index);
		if (list_valid) {
			const auto &list_entry = list_entries[list_index];
			bool in_array = false;
			if (value_valid) {
				auto value_entry = values[value_index];
				for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
					auto child_value_idx = child_data.sel->get_index(list_entry.offset + child_idx);
					if (child_data.validity.RowIsValid(child_value_idx)) {
						auto child_value_entry = child_value[child_value_idx];
						in_array = OP::Operation(value_entry, child_value_entry);
						if (in_array) {
							break;
						}
					}
				}
			} else {
				for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
					auto child_value_idx = child_data.sel->get_index(list_entry.offset + child_idx);
					if (!child_data.validity.RowIsValid(child_value_idx)) {
						in_array = true;
						break;
					}
				}
			}
			result_data[i] = in_array;
		} else {
			result_data[i] = false;
		}
	}
}

/**
 * @brief Any in array for two vectors in case same types
 *
 * @param value_vector
 * @param list
 * @param result
 * @param count
 */
static void AnyInArrayBaseVectors(Vector &value_vector, Vector &list, Vector &result, idx_t count) {
	switch (value_vector.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedAnyInArrayExecute<int8_t, duckdb::Equals>(value_vector, list, result, count);
		break;
	case PhysicalType::INT16:
		TemplatedAnyInArrayExecute<int16_t, duckdb::Equals>(value_vector, list, result, count);
		break;
	case PhysicalType::INT32:
		TemplatedAnyInArrayExecute<int32_t, duckdb::Equals>(value_vector, list, result, count);
		break;
	case PhysicalType::INT64:
		TemplatedAnyInArrayExecute<int64_t, duckdb::Equals>(value_vector, list, result, count);
		break;
	case PhysicalType::UINT8:
		TemplatedAnyInArrayExecute<uint8_t, duckdb::Equals>(value_vector, list, result, count);
		break;
	case PhysicalType::UINT16:
		TemplatedAnyInArrayExecute<uint16_t, duckdb::Equals>(value_vector, list, result, count);
		break;
	case PhysicalType::UINT32:
		TemplatedAnyInArrayExecute<uint32_t, duckdb::Equals>(value_vector, list, result, count);
		break;
	case PhysicalType::UINT64:
		TemplatedAnyInArrayExecute<uint64_t, duckdb::Equals>(value_vector, list, result, count);
		break;
	case PhysicalType::INT128:
		TemplatedAnyInArrayExecute<hugeint_t, duckdb::Equals>(value_vector, list, result, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedAnyInArrayExecute<float, duckdb::Equals>(value_vector, list, result, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedAnyInArrayExecute<double, duckdb::Equals>(value_vector, list, result, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedAnyInArrayExecute<interval_t, duckdb::Equals>(value_vector, list, result, count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedAnyInArrayExecute<string_t, duckdb::Equals>(value_vector, list, result, count);
		break;
	default:
		throw InternalException("Invalid type for comparison");
	}
}

/**
 * @brief Any in array vectors main functions
 * - both constant vectors: call function for each value
 * - one of type1 and child of type2 is decayable: call function for each value
 * - both type1 and child of type2 is nested: call function for each value
 * - type1 and child of type2 is same type: call function for same type
 * - can cast between two type: call function for each value
 * - can't cast: call function to compare two types
 *
 * @param left
 * @param right
 * @param result
 * @param count
 * @param ci
 * @param keys_ci
 */
static void AnyInArrayVectors(Vector &left, Vector &right, Vector &result, idx_t count, bool ci, bool keys_ci) {
	auto type1 = left.GetType();
	auto type2 = right.GetType();
	if (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		AnyInArrayVectorEachValue(left, right, result, count, ci, keys_ci);
	} else {
		auto child_type2 = ListType::GetChildType(type2);
		if (IsDecayableType(type1) || IsDecayableType(child_type2)) {
			// handle for Decay type
			AnyInArrayVectorEachValue(left, right, result, count, ci, keys_ci);
		} else if (IsNestedType(type1) && IsNestedType(child_type2)) {
			// handle for nested type
			AnyInArrayVectorEachValue(left, right, result, count, ci, keys_ci);
		} else {
			// handle for base type
			if ((type1 == child_type2) || (TypeIncludeDate(type1) && TypeIncludeDate(child_type2)) ||
			    (type1.IsNumeric() && child_type2.IsNumeric())) {
				if (type1 == child_type2) {
					AnyInArrayBaseVectors(left, right, result, count);
				} else {
					AnyInArrayVectorEachValue(left, right, result, count, ci, keys_ci);
				}
			} else {
				AnyInArrayVectorsNotEqualType(left, right, result, count);
			}
		}
	}
}

/**
 * @brief Any in array body for adding to catalog
 *
 * @param args
 * @param state
 * @param result
 */
static void AnyInArrayFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType() == LogicalType::BOOLEAN);
	auto first_operand = args.data[0];
	auto second_operand = args.data[1];
	auto ci_vec = args.data[2];
	auto keys_ci_vec = args.data[3];
	D_ASSERT(ci_vec.GetType() == LogicalType::BOOLEAN);
	D_ASSERT(keys_ci_vec.GetType() == LogicalType::BOOLEAN);
	D_ASSERT(second_operand.GetType().id() == LogicalTypeId::LIST);
	auto count = args.size();

	// `ci` and `keys_ci` must be constant vectors
	if (ci_vec.GetVectorType() != VectorType::CONSTANT_VECTOR ||
	    keys_ci_vec.GetVectorType() != VectorType::CONSTANT_VECTOR) {
		throw Exception("'ci' or 'keys_ci' must be constant!");
		return;
	}

	if (second_operand.GetType().id() != LogicalTypeId::LIST) {
		throw Exception("Second parameter must be LIST!");
		return;
	}

	UnifiedVectorFormat vdata3, vdata4;
	ci_vec.ToUnifiedFormat(count, vdata3);
	keys_ci_vec.ToUnifiedFormat(count, vdata4);

	bool ci = ci_default_value;
	bool keys_ci = keys_ci_default_value;
	if (count > 0) {
		ci = vdata3.validity.RowIsValid(0) ? ci_vec.GetValue(0).GetValue<bool>() : ci_default_value;
		keys_ci = vdata4.validity.RowIsValid(0) ? keys_ci_vec.GetValue(0).GetValue<bool>() : keys_ci_default_value;
	}

	AnyInArrayVectors(first_operand, second_operand, result, count, ci, keys_ci);

	if (first_operand.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	    second_operand.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

/**
 * @brief Create a Bound Ref Expression object
 * For argument of list_transform function
 *
 * @param alias
 * @param rtype
 * @param binding
 * @param lambda_index
 * @param depth
 * @return unique_ptr<Expression>
 */
unique_ptr<Expression> CreateBoundRefExpression(string alias, LogicalType rtype, ColumnBinding binding,
                                                idx_t lambda_index, idx_t depth = 0) {
	return std::move(make_uniq<BoundReferenceExpression>(alias, rtype, 0));
}

/**
 * @brief Create a Bound To Lower object
 * create expression for `lower` function: convert string to lowercase
 *
 * @param context
 * @param expr
 * @return unique_ptr<Expression>
 */
unique_ptr<Expression> CreateBoundToLower(ClientContext &context, unique_ptr<Expression> expr) {
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(std::move(expr));
	auto lower_function = LowerFun::GetFunction();
	auto return_type = lower_function.return_type;
	auto result =
	    make_uniq<BoundFunctionExpression>(return_type, std::move(lower_function), std::move(arguments), nullptr);
	return std::move(result);
}

/**
 * @brief Create a Lambda To Lower object
 * Create lambda function expression for list_transform function `x -> lower(x)`
 *
 * @param context
 * @return unique_ptr<Expression>
 */
unique_ptr<Expression> CreateLambdaToLower(ClientContext &context) {
	auto bound_ref =
	    CreateBoundRefExpression("x", LogicalType::VARCHAR, ColumnBinding(DConstants::INVALID_INDEX, 0), 0);
	auto expr = CreateBoundToLower(context, std::move(bound_ref));
	return std::move(expr);
}

/**
 * @brief Create a List Transform To Lower object
 * create list_transform for lower function
 * covert list of string to lowercase
 * list_transform(str, x -> lower(x))
 * ['Hello', 'temP'] -> ['hello', 'temp']
 *
 * @param context
 * @param expr
 * @return unique_ptr<Expression>
 */
unique_ptr<Expression> CreateListTransformToLower(ClientContext &context, unique_ptr<Expression> expr) {
	auto return_type = expr->return_type;
	vector<unique_ptr<Expression>> arguments;
	auto lambda_expr = CreateLambdaToLower(context);
	arguments.push_back(std::move(expr));
	unique_ptr<FunctionData> bind_info = make_uniq<CompareListLambdaBindData>(return_type, std::move(lambda_expr));
	auto list_transform_to_lower_function = ListTransformFun::GetFunction();
	list_transform_to_lower_function.name = "list_transform";
	auto result = make_uniq<BoundFunctionExpression>(return_type, std::move(list_transform_to_lower_function),
	                                                 std::move(arguments), std::move(bind_info));
	return std::move(result);
}

/**
 * @brief Bind function for `compare_any` and `any_in_array` function
 *
 * @param context
 * @param arguments
 * @param is_compare_any
 * @return true
 * @return false
 */
static bool ComparisonBind(ClientContext &context, vector<unique_ptr<Expression>> &arguments, bool is_compare_any) {
	// collect names and deconflict, construct return type
	if (arguments.empty()) {
		throw Exception("Can't compare nothing");
		return false;
	}
	if (arguments.size() < 2 || arguments.size() > 4) {
		throw Exception("Can't compare invalid operands");
		return false;
	}

	// create argument maps from keys `ci` and `keys_ci`
	map<ComparisonArgumentType, idx_t> arguments_maps;
	for (idx_t i = 2; i < arguments.size(); i++) {
		auto &child = arguments[i];
		auto alias = child->alias;
		if (alias == ci_str) {
			arguments_maps.insert(std::pair<ComparisonArgumentType, idx_t>(ComparisonArgumentType::ci, i));
		} else if (alias == keys_ci_str) {
			arguments_maps.insert(std::pair<ComparisonArgumentType, idx_t>(ComparisonArgumentType::keys_ci, i));
		} else {
			throw Exception("Argument key is invalid key");
			return false;
		}
	}
	vector<unique_ptr<Expression>> option_arguments(2);
	for (idx_t i = 0; i < 2; i++) {
		auto iter = arguments_maps.find(ComparisonArgumentType(i));
		if (iter != arguments_maps.end()) {
			option_arguments[i] = std::move(arguments[iter->second]);
		} else {
			option_arguments[i] = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
		}
	}
	arguments.resize(4);
	for (idx_t i = 0; i < 2; i++) {
		arguments[i + 2] = std::move(option_arguments[i]);
	}

	// Getting `ci` value for calling lowercase function
	Value ci_value = Value::BOOLEAN(true);
	if (arguments[2]->type == ExpressionType::OPERATOR_CAST) {
		auto child = std::move(arguments[2]->Cast<BoundCastExpression>().child);
		if (child->type != ExpressionType::VALUE_CONSTANT) {
			throw Exception("'ci' must be constant!");
		}
		ci_value = child->Cast<BoundConstantExpression>().value.DefaultCastAs(arguments[2]->return_type);
		arguments[2] = BoundCastExpression::AddCastToType(context, std::move(child), arguments[2]->return_type);
	} else {
		if (arguments[2]->type != ExpressionType::VALUE_CONSTANT) {
			throw Exception("'ci' must be constant!");
		}
		ci_value = arguments[2]->Cast<BoundConstantExpression>().value;
	}

	// Create expression for case `ci` is true
	// column str -> lower(str) case column `str` is varchar and suitable function
	// column str -> list_transform(str, x -> lower(x)) in case `str` is varchar[] and `any_in_array` function
	if (ci_value.GetValue<bool>()) {
		if (arguments[0]->return_type == LogicalType::VARCHAR) {
			arguments[0] = CreateBoundToLower(context, std::move(arguments[0]));
		}

		if (arguments[1]->return_type == LogicalType::VARCHAR && is_compare_any) {
			arguments[1] = CreateBoundToLower(context, std::move(arguments[1]));
		} else if (arguments[1]->return_type == LogicalType::LIST(LogicalType::VARCHAR) && !is_compare_any) {
			arguments[1] = CreateListTransformToLower(context, std::move(arguments[1]));
		}
	}

	return true;
}

/**
 * @brief Bind function for `compare_any` function
 * add cast expression for two operand if can cast between two type
 *
 * @param context
 * @param bound_function
 * @param arguments
 * @return unique_ptr<FunctionData>
 */
static unique_ptr<FunctionData> CompareAnyBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (!ComparisonBind(context, arguments, true)) {
		return nullptr;
	}

	auto type1 = arguments[0]->return_type;
	auto type2 = arguments[1]->return_type;
	if (type1 != type2) {
		if ((type1.IsNumeric() && type2.IsNumeric()) || (TypeIncludeDate(type1) && TypeIncludeDate(type2))) {
			auto input_type = BoundComparisonExpression::BindComparison(type1, type2);
			arguments[0] = BoundCastExpression::AddCastToType(context, std::move(arguments[0]), input_type,
			                                                  input_type.id() == LogicalTypeId::ENUM);
			arguments[1] = BoundCastExpression::AddCastToType(context, std::move(arguments[1]), input_type,
			                                                  input_type.id() == LogicalTypeId::ENUM);
		}
	}
	return nullptr;
}

/**
 * @brief Bind function for `any_in_array` function
 * add cast expression if type 1 and sub type of type 2 can be cast between them
 *
 * @param context
 * @param bound_function
 * @param arguments
 * @return unique_ptr<FunctionData>
 */
static unique_ptr<FunctionData> AnyInArrayBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (!ComparisonBind(context, arguments, false)) {
		return nullptr;
	}

	auto type1 = arguments[0]->return_type;
	auto type2 = arguments[1]->return_type;
	if (type2.id() != LogicalTypeId::LIST) {
		throw Exception("Second parameter must be LIST!");
		return nullptr;
	} else {
		auto child_type2 = ListType::GetChildType(type2);
		if ((type1.IsNumeric() && child_type2.IsNumeric()) ||
		    (TypeIncludeDate(type1) && TypeIncludeDate(child_type2))) {
			auto input_type = BoundComparisonExpression::BindComparison(type1, child_type2);
			arguments[0] = BoundCastExpression::AddCastToType(context, std::move(arguments[0]), input_type,
			                                                  input_type.id() == LogicalTypeId::ENUM);
			arguments[1] =
			    BoundCastExpression::AddCastToType(context, std::move(arguments[1]), LogicalType::LIST(input_type),
			                                       input_type.id() == LogicalTypeId::ENUM);
		}
	}

	return nullptr;
}

void DatadocsExtension::LoadComparisonFunctions(DatabaseInstance &inst) {
	// Adding `compare_any` to catalog
	ScalarFunction compare_any_fun("compare_any", {}, LogicalType::INTEGER, CompareAnyFunction, CompareAnyBind);
	compare_any_fun.varargs = LogicalType::ANY;
	compare_any_fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	ExtensionUtil::RegisterFunction(inst, compare_any_fun);

	// Adding `any_in_array` to catalog
	ScalarFunction any_in_array_fun("any_in_array", {}, LogicalType::BOOLEAN, AnyInArrayFunction, AnyInArrayBind);
	any_in_array_fun.varargs = LogicalType::ANY;
	any_in_array_fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	ExtensionUtil::RegisterFunction(inst, any_in_array_fun);
}

} // namespace duckdb
