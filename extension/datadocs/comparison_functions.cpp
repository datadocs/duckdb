#include "datadocs.hpp"
#include "datadocs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

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

enum class ComparisonArgumentType : uint8_t { ci, keys_ci };

static string ci = "ci";
static string keys_ci = "keys_ci";

static bool ci_default_value = true;
static bool keys_ci_default_value = true;

static int COMPARISON_RS_EQUAL = 0;
static int COMPARISON_RS_LESS = -1;
static int COMPARISON_RS_BIGGER = 1;

static string ToLowerCase(string str) {
	string result = str;
	std::transform(str.begin(), str.end(), result.begin(), [](unsigned char c) { return std::tolower(c); });
	return result;
}

static bool TypeIncludeDate(LogicalType type) {
	if (IsDatetime(type) && type != LogicalType::TIME) {
		return true;
	}
	return false;
}

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

static bool IsDecayableType(LogicalType type) {
	return type == DDJsonType || type == DDVariantType;
}

static bool IsNestedType(LogicalType type) {
	return type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::STRUCT;
}

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

static Value GetDecayVariantValue(Value v, yyjson_alc *alc) {
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

static Value GetDecayJsonValue(Value v, yyjson_alc *alc) {
	auto doc = JSONCommon::ReadDocument(v.GetValueUnsafe<string_t>(), JSONCommon::READ_FLAG, alc);
	yyjson_val *val = yyjson_doc_get_root(doc);
	return GetDecayJsonValueFromVal(val, alc);
}

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

struct CompareAny {
	template <class T>
	static inline int Operation(const T &left, const T &right) {
		if (left == right)
			return COMPARISON_RS_EQUAL;
		if (left < right)
			return COMPARISON_RS_LESS;
		return COMPARISON_RS_BIGGER;
	}
};

template <class T, class OP>
static inline void TemplatedExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
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

static void CompareAnyBaseVectors(Vector &left, Vector &right, Vector &result, idx_t count) {
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedExecute<int8_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::INT16:
		TemplatedExecute<int16_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::INT32:
		TemplatedExecute<int32_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::INT64:
		TemplatedExecute<int64_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::UINT8:
		TemplatedExecute<uint8_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::UINT16:
		TemplatedExecute<uint16_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::UINT32:
		TemplatedExecute<uint32_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::UINT64:
		TemplatedExecute<uint64_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::INT128:
		TemplatedExecute<hugeint_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedExecute<float, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedExecute<double, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedExecute<interval_t, CompareAny>(left, right, result, count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedExecute<string_t, CompareAny>(left, right, result, count);
		break;
	default:
		throw InternalException("Invalid type for comparison");
	}
}

static void CompareAnyVectorEachValue(Vector &left, Vector &right, Vector &result, idx_t count, bool ci, bool keys_ci) {
	UnifiedVectorFormat vdata1, vdata2;
	left.ToUnifiedFormat(count, vdata1);
	right.ToUnifiedFormat(count, vdata2);
	auto result_data = FlatVector::GetData<int>(result);
	for (idx_t i = 0; i < count; i++) {
		auto idx1 = vdata1.sel->get_index(i);
		auto idx2 = vdata2.sel->get_index(i);
		bool v1_valid = vdata1.validity.RowIsValid(idx1);
		bool v2_valid = vdata2.validity.RowIsValid(idx2);
		if (v1_valid && v2_valid) {
			result_data[i] = CompareAnyValue(left.GetValue(idx1), right.GetValue(idx2), ci, keys_ci);
		} else if (!v1_valid && !v2_valid) {
			result_data[i] = COMPARISON_RS_EQUAL;
		} else {
			result_data[i] = v1_valid ? COMPARISON_RS_BIGGER : COMPARISON_RS_LESS;
		}
	}
}

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

unique_ptr<Expression> CreateBoundToLower(ClientContext &context, unique_ptr<Expression> expr) {
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(std::move(expr));
	auto lower_function = LowerFun::GetFunction();
	auto return_type = lower_function.return_type;
	auto result =
	    make_uniq<BoundFunctionExpression>(return_type, std::move(lower_function), std::move(arguments), nullptr);
	return std::move(result);
}

static unique_ptr<FunctionData> CompareAnyBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	// collect names and deconflict, construct return type
	if (arguments.empty()) {
		throw Exception("Can't compare nothing");
		return nullptr;
	}
	if (arguments.size() < 2 || arguments.size() > 4) {
		throw Exception("Can't compare invalid operands");
		return nullptr;
	}

	map<ComparisonArgumentType, idx_t> arguments_maps;
	for (idx_t i = 2; i < arguments.size(); i++) {
		auto &child = arguments[i];
		auto alias = child->alias;
		if (alias == ci) {
			arguments_maps.insert(std::pair<ComparisonArgumentType, idx_t>(ComparisonArgumentType::ci, i));
		} else if (alias == keys_ci) {
			arguments_maps.insert(std::pair<ComparisonArgumentType, idx_t>(ComparisonArgumentType::keys_ci, i));
		} else {
			throw Exception("Argument key is invalid key");
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
	if (ci_value.GetValue<bool>()) {
		if (arguments[0]->return_type == LogicalType::VARCHAR) {
			arguments[0] = CreateBoundToLower(context, std::move(arguments[0]));
		}

		if (arguments[1]->return_type == LogicalType::VARCHAR) {
			arguments[1] = CreateBoundToLower(context, std::move(arguments[1]));
		}
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

void DatadocsExtension::LoadComparisonFunctions(DatabaseInstance &inst) {
	// ExtensionUtil::RegisterFunction(inst, ScalarFunction("compare_any", {LogicalType::ANY, LogicalType::ANY},
	//                                                      LogicalType::INTEGER, CompareAnyFunction, CompareAnyBind));
	ScalarFunction fun("compare_any", {}, LogicalType::INTEGER, CompareAnyFunction, CompareAnyBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	ExtensionUtil::RegisterFunction(inst, fun);
}

} // namespace duckdb
