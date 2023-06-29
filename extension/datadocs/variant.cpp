#include "duckdb.hpp"

#include <charconv>
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#endif
#include "converters.hpp"
#include "datadocs-extension.hpp"
#include "datadocs.hpp"
#include "fmt/format.h"
#include "geometry.hpp"
#include "json_common.hpp"
#include "postgis/lwgeom_ogc.hpp"
#include "vector_proxy.hpp"

namespace duckdb {

LogicalType DDGeoType;

const LogicalType DefaultDecimalType = LogicalType::DECIMAL(18, 3);
const LogicalType DDNumericType = LogicalType::DECIMAL(dd_numeric_width, dd_numeric_scale);
const LogicalType DDJsonType = JSONCommon::JSONType();

LogicalType getVariantType() {
	child_list_t<LogicalType> children;
	children.push_back(make_pair("__type", LogicalType::VARCHAR));
	children.push_back(make_pair("__value", DDJsonType));
	auto variant_type = LogicalType::STRUCT(move(children));
	variant_type.SetAlias("VARIANT");

	return variant_type;
}

// clang-format off
// const LogicalType DDVariantType = LogicalType::STRUCT({
// 	{"__type", LogicalType::VARCHAR},
// 	{"__value", DDJsonType}
// });
const LogicalType DDVariantType = getVariantType();
// clang-format on

namespace {

using namespace std::placeholders;

class VariantWriter {
public:
	VariantWriter(const LogicalType &arg_type, yyjson_mut_doc *doc = nullptr)
	    : doc(doc), alc(Allocator::DefaultAllocator()), type(&arg_type) {
		is_list = type->id() == LogicalTypeId::LIST;
		if (is_list) {
			type = &ListType::GetChildType(*type);
		}
		switch (type->id()) {
		case LogicalTypeId::BOOLEAN:
			type_name = is_list ? "BOOL[]" : "BOOL";
			write_func = &VariantWriter::WriteBool;
			break;
		case LogicalTypeId::TINYINT:
			type_name = is_list ? "INT64[]" : "INT64";
			write_func = &VariantWriter::WriteInt<int8_t>;
			break;
		case LogicalTypeId::SMALLINT:
			type_name = is_list ? "INT64[]" : "INT64";
			write_func = &VariantWriter::WriteInt<int16_t>;
			break;
		case LogicalTypeId::INTEGER:
			type_name = is_list ? "INT64[]" : "INT64";
			write_func = &VariantWriter::WriteInt<int32_t>;
			break;
		case LogicalTypeId::BIGINT:
			type_name = is_list ? "INT64[]" : "INT64";
			write_func = &VariantWriter::WriteInt<int64_t>;
			break;
		case LogicalTypeId::UTINYINT:
			type_name = is_list ? "INT64[]" : "INT64";
			write_func = &VariantWriter::WriteInt<uint8_t>;
			break;
		case LogicalTypeId::USMALLINT:
			type_name = is_list ? "INT64[]" : "INT64";
			write_func = &VariantWriter::WriteInt<uint16_t>;
			break;
		case LogicalTypeId::UINTEGER:
			type_name = is_list ? "INT64[]" : "INT64";
			write_func = &VariantWriter::WriteInt<uint32_t>;
			break;
		case LogicalTypeId::UBIGINT:
			type_name = is_list ? "INT64[]" : "INT64";
			write_func = &VariantWriter::WriteUInt64;
			break;
		case LogicalTypeId::HUGEINT:
			type_name = is_list ? "INT64[]" : "INT64";
			write_func = &VariantWriter::WriteHugeInt;
			break;
		case LogicalTypeId::FLOAT:
			type_name = is_list ? "FLOAT64[]" : "FLOAT64";
			write_func = &VariantWriter::WriteFloat<float>;
			break;
		case LogicalTypeId::DOUBLE:
			type_name = is_list ? "FLOAT64[]" : "FLOAT64";
			write_func = &VariantWriter::WriteFloat<double>;
			break;
		case LogicalTypeId::DECIMAL:
			type_name = is_list ? "NUMERIC[]" : "NUMERIC";
			switch (type->InternalType()) {
			case PhysicalType::INT16:
				write_func = &VariantWriter::WriteNumeric<int16_t>;
				break;
			case PhysicalType::INT32:
				write_func = &VariantWriter::WriteNumeric<int32_t>;
				break;
			case PhysicalType::INT64:
				write_func = &VariantWriter::WriteNumeric<int64_t>;
				break;
			case PhysicalType::INT128:
				write_func = &VariantWriter::WriteNumeric<hugeint_t>;
				break;
			default:
				D_ASSERT(false);
				is_list = false;
				write_func = &VariantWriter::WriteNull;
				break;
			}
			break;
		case LogicalTypeId::VARCHAR:
			if (type->GetAlias() == JSONCommon::JSON_TYPE_NAME) {
				type_name = is_list ? "JSON[]" : "JSON";
				write_func = &VariantWriter::WriteJSON;
			} else {
				type_name = is_list ? "STRING[]" : "STRING";
				write_func = &VariantWriter::WriteString;
			}
			break;
		case LogicalTypeId::BLOB:
			if (type->GetAlias() == "GEOGRAPHY") {
				type_name = is_list ? "GEOGRAPHY[]" : "GEOGRAPHY";
				write_func = &VariantWriter::WriteGeography;
			} else {
				type_name = is_list ? "BYTES[]" : "BYTES";
				write_func = &VariantWriter::WriteBytes;
			}
			break;
		case LogicalTypeId::UUID:
			type_name = is_list ? "STRING[]" : "STRING";
			write_func = &VariantWriter::WriteUUID;
			break;
		case LogicalTypeId::ENUM:
			type_name = is_list ? "STRING[]" : "STRING";
			switch (type->InternalType()) {
			case PhysicalType::UINT8:
				write_func = &VariantWriter::WriteEnum<uint8_t>;
				break;
			case PhysicalType::UINT16:
				write_func = &VariantWriter::WriteEnum<uint16_t>;
				break;
			case PhysicalType::UINT32:
				write_func = &VariantWriter::WriteEnum<uint32_t>;
				break;
			default:
				D_ASSERT(false);
				is_list = false;
				write_func = &VariantWriter::WriteNull;
				break;
			}
			break;
		case LogicalTypeId::DATE:
			type_name = is_list ? "DATE[]" : "DATE";
			write_func = &VariantWriter::WriteDate;
			break;
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIME_TZ:
			type_name = is_list ? "TIME[]" : "TIME";
			write_func = &VariantWriter::WriteTime;
			break;
		case LogicalTypeId::TIMESTAMP:
			type_name = is_list ? "DATETIME[]" : "DATETIME";
			write_func = &VariantWriter::WriteTimestamp;
			break;
		case LogicalTypeId::TIMESTAMP_SEC:
			type_name = is_list ? "DATETIME[]" : "DATETIME";
			write_func = &VariantWriter::WriteTimestamp<Timestamp::FromEpochSeconds>;
			break;
		case LogicalTypeId::TIMESTAMP_MS:
			type_name = is_list ? "DATETIME[]" : "DATETIME";
			write_func = &VariantWriter::WriteTimestamp<Timestamp::FromEpochMs>;
			break;
		case LogicalTypeId::TIMESTAMP_NS:
			type_name = is_list ? "DATETIME[]" : "DATETIME";
			write_func = &VariantWriter::WriteTimestamp<Timestamp::FromEpochNanoSeconds>;
			break;
		case LogicalTypeId::TIMESTAMP_TZ:
			type_name = is_list ? "TIMESTAMP[]" : "TIMESTAMP";
			write_func = &VariantWriter::WriteTimestamp;
			break;
		case LogicalTypeId::INTERVAL:
			type_name = is_list ? "INTERVAL[]" : "INTERVAL";
			write_func = &VariantWriter::WriteInterval;
			break;
		case LogicalTypeId::SQLNULL:
			type_name = is_list ? "NULL[]" : "NULL";
			write_func = &VariantWriter::WriteNull;
			break;
		case LogicalTypeId::LIST:
			type_name = is_list ? "JSON[]" : "JSON";
			write_func = &VariantWriter::WriteList;
			break;
		case LogicalTypeId::STRUCT:
			type_name = is_list ? "STRUCT[]" : "STRUCT";
			write_func = &VariantWriter::WriteStruct;
			break;
		case LogicalTypeId::MAP:
			type_name = is_list ? "STRUCT[]" : "STRUCT";
			write_func = &VariantWriter::WriteMap;
			break;
		case LogicalTypeId::UNION:
			type_name = is_list ? "JSON[]" : nullptr;
			write_func = &VariantWriter::WriteUnion;
			break;
		default:
			type_name = "UNKNOWN";
			is_list = false;
			write_func = &VariantWriter::WriteNull;
			break;
		}
	}

	bool Process(VectorWriter &result, const VectorReader &arg) {
		alc.Reset();
		doc = JSONCommon::CreateDocument(alc.GetYYAlc());
		yyjson_mut_val *root = ProcessValue(arg);
		if (yyjson_mut_is_null(root)) {
			return false;
		}
		VectorStructWriter writer = result.SetStruct();
		writer[0].SetString(type_name);
		yyjson_mut_doc_set_root(doc, root);
		size_t len;
		char *data = yyjson_mut_write_opts(doc, 0, alc.GetYYAlc(), &len, nullptr);
		writer[1].SetString(string_t(data, len));
		return true;
	}

private:
	yyjson_mut_val *ProcessValue(const VectorReader &arg) {
		yyjson_mut_val *root;
		if (is_list) {
			root = yyjson_mut_arr(doc);
			for (const VectorReader &item : arg) {
				if (item.IsNull()) {
					yyjson_mut_arr_add_null(doc, root);
				} else {
					yyjson_mut_arr_append(root, (this->*write_func)(item));
				}
			}
		} else {
			root = (this->*write_func)(arg);
		}
		return root;
	}

	yyjson_mut_val *WriteNull(const VectorReader &arg) {
		return yyjson_mut_null(doc);
	}

	yyjson_mut_val *WriteBool(const VectorReader &arg) {
		return yyjson_mut_bool(doc, arg.Get<bool>());
	}

	template <typename T>
	yyjson_mut_val *WriteInt(const VectorReader &arg) {
		return yyjson_mut_int(doc, arg.Get<T>());
	}

	yyjson_mut_val *WriteUInt64(const VectorReader &arg) {
		uint64_t val = arg.Get<uint64_t>();
		return val <= std::numeric_limits<int64_t>::max() ? yyjson_mut_int(doc, (int64_t)val) : yyjson_mut_null(doc);
	}

	yyjson_mut_val *WriteHugeInt(const VectorReader &arg) {
		hugeint_t val = arg.Get<hugeint_t>();
		return val <= std::numeric_limits<int64_t>::max() && val >= std::numeric_limits<int64_t>::lowest()
		           ? yyjson_mut_int(doc, (int64_t)val.lower)
		           : yyjson_mut_null(doc);
	}

	template <typename T>
	yyjson_mut_val *WriteFloat(const VectorReader &arg) {
		return WriteDoubleImpl(arg.Get<T>());
	}

	yyjson_mut_val *WriteDoubleImpl(double val) {
		if (std::isinf(val)) {
			return yyjson_mut_str(doc, val < 0 ? "-Infinity" : "Infinity");
		}
		if (std::isnan(val)) {
			return yyjson_mut_str(doc, "NaN");
		}
		return yyjson_mut_real(doc, val);
	}

	template <typename T>
	yyjson_mut_val *WriteNumeric(const VectorReader &arg) {
		const T &val = arg.Get<T>();
		uint8_t width, scale;
		type->GetDecimalProperties(width, scale);
		string s = Decimal::ToString(val, width, scale);
		return yyjson_mut_strncpy(doc, s.data(), s.size());
	}

	yyjson_mut_val *WriteString(const VectorReader &arg) {
		std::string_view val = arg.GetString();
		return yyjson_mut_strn(doc, val.data(), val.size());
	}

	yyjson_mut_val *WriteBytes(const VectorReader &arg) {
		string_t val = arg.Get<string_t>();
		idx_t size = Blob::ToBase64Size(val);
		string s(size, '\0');
		Blob::ToBase64(val, s.data());
		return yyjson_mut_strncpy(doc, s.data(), size);
	}

	yyjson_mut_val *WriteUUID(const VectorReader &arg) {
		char s[UUID::STRING_SIZE];
		UUID::ToString(arg.Get<hugeint_t>(), s);
		return yyjson_mut_strncpy(doc, s, UUID::STRING_SIZE);
	}

	template <typename T>
	yyjson_mut_val *WriteEnum(const VectorReader &arg) {
		return WriteEnumImpl(arg.Get<T>());
	}

	yyjson_mut_val *WriteEnumImpl(idx_t val) {
		const Vector &enum_dictionary = EnumType::GetValuesInsertOrder(*type);
		const string_t &s = FlatVector::GetData<string_t>(enum_dictionary)[val];
		return yyjson_mut_strncpy(doc, s.GetDataUnsafe(), s.GetSize());
	}

	yyjson_mut_val *WriteDate(const VectorReader &arg) {
		string s = Date::ToString(arg.Get<date_t>());
		return yyjson_mut_strncpy(doc, s.data(), s.size());
	}

	yyjson_mut_val *WriteTime(const VectorReader &arg) {
		string s = Time::ToString(arg.Get<dtime_t>());
		return yyjson_mut_strncpy(doc, s.data(), s.size());
	}

	yyjson_mut_val *WriteTimestamp(const VectorReader &arg) {
		return WriteTimestampImpl(arg.Get<timestamp_t>());
	}

	template <timestamp_t (*FUNC)(int64_t)>
	yyjson_mut_val *WriteTimestamp(const VectorReader &arg) {
		return WriteTimestampImpl(FUNC(arg.Get<timestamp_t>().value));
	}

	yyjson_mut_val *WriteTimestampImpl(const timestamp_t &ts) {
		string s = Timestamp::ToString(ts);
		return yyjson_mut_strncpy(doc, s.data(), s.size());
	}

	yyjson_mut_val *WriteInterval(const VectorReader &arg) {
		const interval_t &val = arg.Get<interval_t>();
		if (val.months < -10000 * 12 || val.months > 10000 * 12 || val.days < -3660000 || val.days > 3660000 ||
		    val.micros < -87840000 * Interval::MICROS_PER_HOUR || val.micros > 87840000 * Interval::MICROS_PER_HOUR) {
			return yyjson_mut_null(doc);
		}
		string s = IntervalToISOString(val);
		return yyjson_mut_strncpy(doc, s.data(), s.size());
	}

	yyjson_mut_val *WriteJSON(const VectorReader &arg) {
		auto arg_doc = JSONCommon::ReadDocument(arg.Get<string_t>(), JSONCommon::READ_FLAG, alc.GetYYAlc());
		return yyjson_val_mut_copy(doc, yyjson_doc_get_root(arg_doc));
	}

	yyjson_mut_val *WriteGeography(const VectorReader &arg) {
		string s = Geometry::GetString(arg.Get<string_t>(), DataFormatType::FORMAT_VALUE_TYPE_WKT);
		return yyjson_mut_strncpy(doc, s.data(), s.size());
	}

	yyjson_mut_val *WriteList(const VectorReader &arg) {
		yyjson_mut_val *obj = yyjson_mut_arr(doc);
		idx_t i = 0;
		for (const VectorReader &item : arg) {
			if (item.IsNull()) {
				yyjson_mut_arr_add_null(doc, obj);
			} else {
				yyjson_mut_arr_append(obj, VariantWriter(ListType::GetChildType(*type), doc).ProcessValue(item));
			}
		}
		return obj;
	}

	yyjson_mut_val *WriteStruct(const VectorReader &arg) {
		yyjson_mut_val *obj = yyjson_mut_obj(doc);
		idx_t i = 0;
		for (auto &[child_key, child_type] : StructType::GetChildTypes(*type)) {
			yyjson_mut_val *key = yyjson_mut_strn(doc, child_key.data(), child_key.size());
			const VectorReader item = arg[i++];
			yyjson_mut_val *val =
			    item.IsNull() ? yyjson_mut_null(doc) : VariantWriter(child_type, doc).ProcessValue(item);
			yyjson_mut_obj_put(obj, key, val);
		}
		return obj;
	}

	yyjson_mut_val *WriteMap(const VectorReader &arg) {
		if (MapType::KeyType(*type).id() != LogicalTypeId::VARCHAR) {
			return yyjson_mut_null(doc);
		}
		VariantWriter writer(MapType::ValueType(*type), doc);
		yyjson_mut_val *obj = yyjson_mut_obj(doc);
		for (const VectorReader &item : arg) {
			D_ASSERT(!item.IsNull());
			std::string_view child_key = item[0].GetString();
			yyjson_mut_val *key = yyjson_mut_strn(doc, child_key.data(), child_key.size());
			const VectorReader &arg_value = item[1];
			yyjson_mut_val *val = arg_value.IsNull() ? yyjson_mut_null(doc) : writer.ProcessValue(arg_value);
			yyjson_mut_obj_put(obj, key, val);
		}
		return obj;
	}

	yyjson_mut_val *WriteUnion(const VectorReader &arg) {
		union_tag_t tag = arg[0].Get<union_tag_t>();
		VariantWriter writer(StructType::GetChildType(*type, tag + 1), doc);
		yyjson_mut_val *val = writer.ProcessValue(arg[tag + 1]);
		if (!type_name) {
			type_name = writer.type_name;
		}
		return val;
	}

private:
	yyjson_mut_doc *doc;
	JSONAllocator alc;
	yyjson_mut_val *(VariantWriter::*write_func)(const VectorReader &) = nullptr;
	const char *type_name = nullptr;
	bool is_list = false;
	const LogicalType *type;
};

static void VariantFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType() == DDVariantType);
	VariantWriter writer(args.data[0].GetType());
	VectorExecute(args, result, writer, &VariantWriter::Process);
}

class VariantReaderBase {
public:
	bool ProcessScalar(VectorWriter &result, const VectorReader &arg) {
		alc.Reset();
		auto doc = JSONCommon::ReadDocument(arg[1].Get<string_t>(), JSONCommon::READ_FLAG, alc.GetYYAlc());
		auto val = yyjson_doc_get_root(doc);
		return ReadScalar(result, val);
	}

	bool ProcessList(VectorWriter &result, const VectorReader &arg) {
		alc.Reset();
		auto doc = JSONCommon::ReadDocument(arg[1].Get<string_t>(), JSONCommon::READ_FLAG, alc.GetYYAlc());
		auto root = yyjson_doc_get_root(doc);
		yyjson_arr_iter iter;
		if (!yyjson_arr_iter_init(root, &iter)) {
			return false;
		}
		VectorListWriter list_writer = result.SetList();
		yyjson_val *val;
		while (val = yyjson_arr_iter_next(&iter)) {
			VectorWriter item = list_writer.Append();
			if (!ReadScalar(item, val)) {
				item.SetNull();
			}
		}
		return true;
	}

	virtual bool ReadScalar(VectorWriter &result, yyjson_val *val) = 0;

protected:
	JSONAllocator alc {Allocator::DefaultAllocator()};
};

class VariantReaderBool : public VariantReaderBase {
public:
	bool ReadScalar(VectorWriter &result, yyjson_val *val) override {
		if (!unsafe_yyjson_is_bool(val)) {
			return false;
		}
		result.Set(unsafe_yyjson_get_bool(val));
		return true;
	}
};

class VariantReaderInt64 : public VariantReaderBase {
public:
	bool ReadScalar(VectorWriter &result, yyjson_val *val) override {
		int64_t res;
		switch (unsafe_yyjson_get_tag(val)) {
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
			res = unsafe_yyjson_get_sint(val);
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT: {
			uint64_t i = unsafe_yyjson_get_uint(val);
			if (i > (uint64_t)std::numeric_limits<int64_t>::max()) {
				return false;
			}
			res = (int64_t)i;
			break;
		}
		default:
			return false;
		}
		result.Set(res);
		return true;
	}
};

class VariantReaderFloat64 : public VariantReaderBase {
public:
	bool ReadScalar(VectorWriter &result, yyjson_val *val) override {
		double res;
		switch (unsafe_yyjson_get_tag(val)) {
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
			res = unsafe_yyjson_get_real(val);
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
			res = (double)unsafe_yyjson_get_uint(val);
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
			res = (double)unsafe_yyjson_get_sint(val);
			break;
		case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE: {
			const char *s = unsafe_yyjson_get_str(val);
			if (strcmp(s, "Infinity") == 0) {
				res = std::numeric_limits<double>::infinity();
			} else if (strcmp(s, "-Infinity") == 0) {
				res = -std::numeric_limits<double>::infinity();
			} else if (strcmp(s, "NaN") == 0) {
				res = NAN;
			} else {
				return false;
			}
			break;
		}
		default:
			return false;
		}
		result.Set(res);
		return true;
	}
};

class VariantReaderNumeric : public VariantReaderBase {
public:
	bool ReadScalar(VectorWriter &result, yyjson_val *val) override {
		hugeint_t res;
		string message(1, ' ');
		switch (unsafe_yyjson_get_tag(val)) {
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
			if (!TryCastToDecimal::Operation(unsafe_yyjson_get_real(val), res, &message, dd_numeric_width,
			                                 dd_numeric_scale)) {
				return false;
			}
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
			if (!TryCastToDecimal::Operation(unsafe_yyjson_get_uint(val), res, &message, dd_numeric_width,
			                                 dd_numeric_scale)) {
				return false;
			}
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
			if (!TryCastToDecimal::Operation(unsafe_yyjson_get_sint(val), res, &message, dd_numeric_width,
			                                 dd_numeric_scale)) {
				return false;
			}
			break;
		case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
			if (!TryCastToDecimal::Operation(string_t(unsafe_yyjson_get_str(val)), res, &message, dd_numeric_width,
			                                 dd_numeric_scale)) {
				return false;
			}
			break;
		default:
			return false;
		}
		result.Set(res);
		return true;
	}
};

class VariantReaderString : public VariantReaderBase {
public:
	bool ProcessScalar(VectorWriter &result, const VectorReader &arg) {
		std::string_view tp = arg[0].GetString();
		return (tp == "STRING" || tp == "JSON") && VariantReaderBase::ProcessScalar(result, arg);
	}

	bool ProcessList(VectorWriter &result, const VectorReader &arg) {
		std::string_view tp = arg[0].GetString();
		return (tp == "STRING[]" || tp == "JSON[]" || tp == "JSON") && VariantReaderBase::ProcessList(result, arg);
	}

	bool ReadScalar(VectorWriter &result, yyjson_val *val) override {
		const char *res = yyjson_get_str(val);
		if (!res) {
			return false;
		}
		result.SetString(res);
		return true;
	}
};

class VariantReaderBytes : public VariantReaderBase {
public:
	bool ProcessScalar(VectorWriter &result, const VectorReader &arg) {
		return arg[0].GetString() == "BYTES" && VariantReaderBase::ProcessScalar(result, arg);
	}

	bool ProcessList(VectorWriter &result, const VectorReader &arg) {
		return arg[0].GetString() == "BYTES[]" && VariantReaderBase::ProcessList(result, arg);
	}

	bool ReadScalar(VectorWriter &result, yyjson_val *val) override {
		const char *str_val = yyjson_get_str(val);
		if (!str_val) {
			return false;
		}
		string_t str(str_val);
		idx_t size = Blob::FromBase64Size(str);
		string res(size, '\0');
		Blob::FromBase64(str, (data_ptr_t)res.data(), size);
		result.SetString(res);
		return true;
	}
};

class VariantReaderDate : public VariantReaderBase {
public:
	bool ProcessScalar(VectorWriter &result, const VectorReader &arg) {
		return arg[0].GetString() == "DATE" && VariantReaderBase::ProcessScalar(result, arg);
	}

	bool ProcessList(VectorWriter &result, const VectorReader &arg) {
		return arg[0].GetString() == "DATE[]" && VariantReaderBase::ProcessList(result, arg);
	}

	bool ReadScalar(VectorWriter &result, yyjson_val *val) override {
		const char *str_val = yyjson_get_str(val);
		if (!str_val) {
			return false;
		}
		date_t res;
		idx_t pos;
		bool special;
		if (!Date::TryConvertDate(str_val, strlen(str_val), pos, res, special, true) ||
		    res.days == std::numeric_limits<int32_t>::max() || res.days <= -std::numeric_limits<int32_t>::max()) {
			return false;
		}
		result.Set(res.days);
		return true;
	}
};

class VariantReaderTime : public VariantReaderBase {
public:
	bool ProcessScalar(VectorWriter &result, const VectorReader &arg) {
		return arg[0].GetString() == "TIME" && VariantReaderBase::ProcessScalar(result, arg);
	}

	bool ProcessList(VectorWriter &result, const VectorReader &arg) {
		return arg[0].GetString() == "TIME[]" && VariantReaderBase::ProcessList(result, arg);
	}

	bool ReadScalar(VectorWriter &result, yyjson_val *val) override {
		const char *str_val = yyjson_get_str(val);
		if (!str_val) {
			return false;
		}
		dtime_t res;
		idx_t pos;
		if (!Time::TryConvertTime(str_val, strlen(str_val), pos, res, true)) {
			return false;
		}
		result.Set(res.micros);
		return true;
	}
};

class VariantReaderTimestamp : public VariantReaderBase {
public:
	bool ProcessScalar(VectorWriter &result, const VectorReader &arg) {
		std::string_view tp = arg[0].GetString();
		return (tp == "TIMESTAMP" || tp == "DATE" || tp == "DATETIME") && VariantReaderBase::ProcessScalar(result, arg);
	}

	bool ProcessList(VectorWriter &result, const VectorReader &arg) {
		std::string_view tp = arg[0].GetString();
		return (tp == "TIMESTAMP[]" || tp == "DATE[]" || tp == "DATETIME[]") &&
		       VariantReaderBase::ProcessList(result, arg);
	}

	bool ReadScalar(VectorWriter &result, yyjson_val *val) override {
		const char *str_val = yyjson_get_str(val);
		if (!str_val) {
			return false;
		}
		timestamp_t res;
		if (Timestamp::TryConvertTimestamp(str_val, strlen(str_val), res) != TimestampCastResult::SUCCESS) {
			return false;
		}
		result.Set(res.value);
		return true;
	}
};

class VariantReaderDatetime : public VariantReaderBase {
public:
	bool ProcessScalar(VectorWriter &result, const VectorReader &arg) {
		std::string_view tp = arg[0].GetString();
		return (tp == "DATE" || tp == "DATETIME") && VariantReaderBase::ProcessScalar(result, arg);
	}

	bool ProcessList(VectorWriter &result, const VectorReader &arg) {
		std::string_view tp = arg[0].GetString();
		return (tp == "DATE[]" || tp == "DATETIME[]") && VariantReaderBase::ProcessList(result, arg);
	}

	bool ReadScalar(VectorWriter &result, yyjson_val *val) override {
		const char *str_val = yyjson_get_str(val);
		if (!str_val) {
			return false;
		}
		timestamp_t res;
		if (Timestamp::TryConvertTimestamp(str_val, strlen(str_val), res) != TimestampCastResult::SUCCESS) {
			return false;
		}
		result.Set(res.value);
		return true;
	}
};

class VariantReaderInterval : public VariantReaderBase {
public:
	bool ProcessScalar(VectorWriter &result, const VectorReader &arg) {
		std::string_view tp = arg[0].GetString();
		return (tp == "INTERVAL" || tp == "TIME") && VariantReaderBase::ProcessScalar(result, arg);
	}

	bool ProcessList(VectorWriter &result, const VectorReader &arg) {
		std::string_view tp = arg[0].GetString();
		return (tp == "INTERVAL[]" || tp == "TIME[]") && VariantReaderBase::ProcessList(result, arg);
	}

	bool ReadScalar(VectorWriter &result, yyjson_val *val) override {
		const char *str_val = yyjson_get_str(val);
		if (!str_val) {
			return false;
		}
		interval_t res;
		if (!IntervalFromISOString(str_val, strlen(str_val), res)) {
			string message(1, ' ');
			if (!Interval::FromCString(str_val, strlen(str_val), res, &message, true)) {
				return false;
			}
		}
		result.Set(res);
		return true;
	}
};

class VariantReaderJSON : public VariantReaderBase {
public:
	bool ProcessScalar(VectorWriter &result, const VectorReader &arg) {
		result.SetString(arg[1].Get<string_t>());
		return true;
	}

	bool ReadScalar(VectorWriter &result, yyjson_val *val) override {
		auto res_doc = JSONCommon::CreateDocument(alc.GetYYAlc());
		yyjson_mut_doc_set_root(res_doc, yyjson_val_mut_copy(res_doc, val));
		size_t len;
		char *data = yyjson_mut_write_opts(res_doc, 0, alc.GetYYAlc(), &len, nullptr);
		result.SetString(string_t(data, len));
		return true;
	}
};

class VariantReaderGeography : public VariantReaderBase {
public:
	bool ProcessScalar(VectorWriter &result, const VectorReader &arg) {
		return arg[0].GetString() == "GEOGRAPHY" && VariantReaderBase::ProcessScalar(result, arg);
	}

	bool ProcessList(VectorWriter &result, const VectorReader &arg) {
		return arg[0].GetString() == "GEOGRAPHY[]" && VariantReaderBase::ProcessList(result, arg);
	}

	bool ReadScalar(VectorWriter &result, yyjson_val *val) override {
		const char *str_val = yyjson_get_str(val);
		if (!str_val) {
			return false;
		}
		GSERIALIZED *gser = LWGEOM_from_text((char *)str_val);
		if (!gser) {
			return false;
		}
		string_t &s = result.ReserveString(Geometry::GetGeometrySize(gser));
		Geometry::ToGeometry(gser, (data_ptr_t)s.GetDataWriteable());
		Geometry::DestroyGeometry(gser);
		s.Finalize();
		return true;
	}
};

static bool VariantError(LogicalType source, string *error_message, LogicalType target) {
	string e = "Failed to convert variant " + source.ToString() + " to " + target.ToString();
	HandleCastError::AssignError(e, error_message);
	return false;
}

static bool IsDatetime(LogicalType type) {
	if (LogicalType::TypeIsTimestamp(type)) {
		return true;
	}
	switch (type.id()) {
	case LogicalType::DATE:
	case LogicalType::TIME: {
		return true;
	} break;
	}

	return false;
}

Vector GetVariantVector(VectorReader reader, string type) {
	if (type == "STRING") {
		Vector res(LogicalType::VARCHAR);
		VectorCastExecute(reader, res, VariantReaderString(), 0, &VariantReaderString::ProcessScalar);
		return res;
	} else if (type == "STRING[]") {
		Vector res(LogicalType::LIST(LogicalType::VARCHAR));
		VectorCastExecute(reader, res, VariantReaderString(), 0, &VariantReaderString::ProcessList);
		return res;
	} else if (type == "JSON") {
		Vector res(DDJsonType);
		VectorCastExecute(reader, res, VariantReaderJSON(), 0, &VariantReaderJSON::ProcessScalar);
		return res;
	} else if (type == "JSON[]") {
		Vector res(LogicalType::LIST(DDJsonType));
		VectorCastExecute(reader, res, VariantReaderJSON(), 0, &VariantReaderJSON::ProcessList);
		return res;
	} else if (type == "INT64") {
		Vector res(LogicalType::BIGINT);
		VectorCastExecute(reader, res, VariantReaderInt64(), 0, &VariantReaderInt64::ProcessScalar);
		return res;
	} else if (type == "INT64[]") {
		Vector res(LogicalType::LIST(LogicalType::BIGINT));
		VectorCastExecute(reader, res, VariantReaderInt64(), 0, &VariantReaderInt64::ProcessList);
		return res;
	} else if (type == "BOOL") {
		Vector res(LogicalType::BOOLEAN);
		VectorCastExecute(reader, res, VariantReaderBool(), 0, &VariantReaderBool::ProcessScalar);
		return res;
	} else if (type == "BOOL[]") {
		Vector res(LogicalType::LIST(LogicalType::BOOLEAN));
		VectorCastExecute(reader, res, VariantReaderBool(), 0, &VariantReaderBool::ProcessList);
		return res;
	} else if (type == "FLOAT64") {
		Vector res(LogicalType::DOUBLE);
		VectorCastExecute(reader, res, VariantReaderFloat64(), 0, &VariantReaderFloat64::ProcessScalar);
		return res;
	} else if (type == "FLOAT64[]") {
		Vector res(LogicalType::LIST(LogicalType::DOUBLE));
		VectorCastExecute(reader, res, VariantReaderFloat64(), 0, &VariantReaderFloat64::ProcessList);
		return res;
	} else if (type == "NUMERIC") {
		Vector res(DDNumericType);
		VectorCastExecute(reader, res, VariantReaderNumeric(), 0, &VariantReaderNumeric::ProcessScalar);
		return res;
	} else if (type == "NUMERIC[]") {
		Vector res(LogicalType::LIST(DDNumericType));
		VectorCastExecute(reader, res, VariantReaderNumeric(), 0, &VariantReaderNumeric::ProcessList);
		return res;
	} else if (type == "DATE") {
		Vector res(LogicalType::DATE);
		VectorCastExecute(reader, res, VariantReaderDate(), 0, &VariantReaderDate::ProcessScalar);
		return res;
	} else if (type == "DATE[]") {
		Vector res(LogicalType::LIST(LogicalType::DATE));
		VectorCastExecute(reader, res, VariantReaderDate(), 0, &VariantReaderDate::ProcessList);
		return res;
	} else if (type == "TIME") {
		Vector res(LogicalType::TIME);
		VectorCastExecute(reader, res, VariantReaderTime(), 0, &VariantReaderTime::ProcessScalar);
		return res;
	} else if (type == "TIME[]") {
		Vector res(LogicalType::LIST(LogicalType::TIME));
		VectorCastExecute(reader, res, VariantReaderTime(), 0, &VariantReaderTime::ProcessList);
		return res;
	} else if (type == "DATETIME") {
		Vector res(LogicalType::TIMESTAMP);
		VectorCastExecute(reader, res, VariantReaderDatetime(), 0, &VariantReaderDatetime::ProcessScalar);
		return res;
	} else if (type == "DATETIME[]") {
		Vector res(LogicalType::LIST(LogicalType::TIMESTAMP));
		VectorCastExecute(reader, res, VariantReaderDatetime(), 0, &VariantReaderDatetime::ProcessList);
		return res;
	} else if (type == "TIMESTAMP") {
		Vector res(LogicalType::TIMESTAMP_TZ);
		VectorCastExecute(reader, res, VariantReaderTimestamp(), 0, &VariantReaderTimestamp::ProcessScalar);
		return res;
	} else if (type == "TIMESTAMP[]") {
		Vector res(LogicalType::LIST(LogicalType::TIMESTAMP_TZ));
		VectorCastExecute(reader, res, VariantReaderTimestamp(), 0, &VariantReaderTimestamp::ProcessList);
		return res;
	} else if (type == "INTERVAL") {
		Vector res(LogicalType::INTERVAL);
		VectorCastExecute(reader, res, VariantReaderInterval(), 0, &VariantReaderInterval::ProcessScalar);
		return res;
	} else if (type == "INTERVAL[]") {
		Vector res(LogicalType::LIST(LogicalType::INTERVAL));
		VectorCastExecute(reader, res, VariantReaderInterval(), 0, &VariantReaderInterval::ProcessList);
		return res;
	} else if (type == "GEOGRAPHY") {
		Vector res(DDGeoType);
		VectorCastExecute(reader, res, VariantReaderGeography(), 0, &VariantReaderGeography::ProcessScalar);
		return res;
	} else if (type == "GEOGRAPHY[]") {
		Vector res(LogicalType::LIST(DDGeoType));
		VectorCastExecute(reader, res, VariantReaderGeography(), 0, &VariantReaderGeography::ProcessList);
		return res;
	} else if (type == "BYTES") {
		Vector res(LogicalType::BLOB);
		VectorCastExecute(reader, res, VariantReaderBytes(), 0, &VariantReaderBytes::ProcessScalar);
		return res;
	} else if (type == "BYTES[]") {
		Vector res(LogicalType::LIST(LogicalType::BLOB));
		VectorCastExecute(reader, res, VariantReaderBytes(), 0, &VariantReaderBytes::ProcessList);
		return res;
	} else if (type == "STRUCT") {
		Vector res(LogicalType::VARCHAR);
		VectorCastExecute(reader, res, VariantReaderString(), 0, &VariantReaderString::ProcessScalar);
		return res;
	} else if (type == "STRUCT[]") {
		Vector res(LogicalType::LIST(LogicalType::VARCHAR));
		VectorCastExecute(reader, res, VariantReaderString(), 0, &VariantReaderString::ProcessList);
		return res;
	}

	return Vector(LogicalType::VARCHAR);
}

bool TryCastVariant(VectorReader reader, Value val, LogicalType target, Vector &result, idx_t idx,
                    CastParameters &parameters) {
	switch (target.id()) {
	case LogicalType::VARCHAR: {
		string tempVal;
		if (target.GetAlias() == JSONCommon::JSON_TYPE_NAME && val.type().GetAlias() == JSONCommon::JSON_TYPE_NAME) {
			// Handle for JSON
			tempVal = val.ToString();
		} else if (val.type().id() == LogicalType::VARCHAR) {
			tempVal = val.ToString();
		} else if (val.type() == DDGeoType) {
			tempVal = Geometry::GetString(val.GetValueUnsafe<string_t>(), DataFormatType::FORMAT_VALUE_TYPE_WKT);
		} else {
			tempVal = val.ToString();
			// return VariantError(val.type(), parameters.error_message, target);
		}
		result.SetValue(idx, Value(tempVal));
		return true;
	} break;
	case LogicalType::INTEGER:
	case LogicalType::BIGINT: {
		if (val.type().IsNumeric()) {
			result.SetValue(idx, Value::Numeric(target, val.GetValue<int64_t>()));
			return true;
		} else {
			return VariantError(val.type(), parameters.error_message, target);
		}
	} break;
	case LogicalType::DOUBLE: {
		if (val.type().IsNumeric()) {
			result.SetValue(idx, Value(val.GetValue<double>()));
			return true;
		} else {
			return VariantError(val.type(), parameters.error_message, target);
		}
	} break;
	case LogicalTypeId::DECIMAL: {
		if (val.type().IsNumeric()) {
			result.SetValue(idx, val);
			return true;
		} else {
			return VariantError(val.type(), parameters.error_message, target);
		}
	} break;
	case LogicalType::DATE:
	case LogicalType::TIMESTAMP:
	case LogicalType::TIMESTAMP_TZ: {
		if (IsDatetime(val.type()) && val.type().id() != LogicalType::TIME) {
			result.SetValue(idx, val);
			return true;
		} else {
			return VariantError(val.type(), parameters.error_message, target);
		}
	} break;
	case LogicalType::TIME: {
		if (IsDatetime(val.type()) && val.type().id() != LogicalType::DATE) {
			result.SetValue(idx, val);
			return true;
		} else {
			return VariantError(val.type(), parameters.error_message, target);
		}
	} break;
	// Cast for Interval, BLOB and GEOGRAPHY
	case LogicalType::BLOB:
	case LogicalType::INTERVAL: {
		if (val.type() == target) {
			result.SetValue(idx, val);
			return true;
		} else {
			return VariantError(val.type(), parameters.error_message, target);
		}
	} break;
	}
	return false;
}

struct VariantCasts {
	static bool VariantCastAny(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		bool success = true;
		UnifiedVectorFormat vdata;
		source.ToUnifiedFormat(count, vdata);

		auto &entries = StructVector::GetEntries(source);
		auto type_data = FlatVector::GetData<string_t>(*entries[0]);
		auto value_data = FlatVector::GetData<string_t>(*entries[1]);

		bool constant = (source.GetVectorType() == VectorType::CONSTANT_VECTOR);
		VectorHolder holder(source, count);
		VectorReader reader(holder);
		auto isTargetList = result.GetType().id() == LogicalTypeId::LIST;
		idx_t offset = 0;
		auto list_entries = FlatVector::GetData<list_entry_t>(result);
		vector<Value> vals;
		for (idx_t i = 0; i < (constant ? 1 : count); i++) {
			auto idx = vdata.sel->get_index(i);

			if (!vdata.validity.RowIsValid(idx)) {
				FlatVector::SetNull(result, i, true);
				continue;
			}
			auto type = type_data[idx].GetString();
			reader.SetRow(idx);
			auto res = GetVariantVector(reader, type);
			auto val = res.GetValue(0);
			if (isTargetList) {
				if (val.type().id() != LogicalTypeId::LIST) {
					FlatVector::SetNull(result, i, true);
					success = VariantError(val.type(), parameters.error_message, result.GetType());
					continue;
				} else {
					auto childVals = StructValue::GetChildren(val);
					auto &entry = list_entries[i];
					entry.offset = offset;
					entry.length = childVals.size();
					offset += entry.length;
					vals.insert(vals.end(), childVals.begin(), childVals.end());
				}
			} else {
				if (!TryCastVariant(reader, val, result.GetType(), result, idx, parameters)) {
					FlatVector::SetNull(result, i, true);
					success = false;
					continue;
				}
			}
		}
		if (isTargetList) {
			ListVector::SetListSize(result, offset);
			ListVector::Reserve(result, offset);
			auto &validity = FlatVector::Validity(ListVector::GetEntry(result));
			for (idx_t i = 0; i < offset; i++) {
				if (!TryCastVariant(reader, vals[i], ListVector::GetEntry(result).GetType(),
				                    ListVector::GetEntry(result), i, parameters)) {
					validity.SetInvalid(i);
					success = false;
					continue;
				}
			}
		}
		if (constant) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		return success;
	}
};

template <class Reader>
static void FromVariantFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[0].GetType() == DDVariantType);
	VectorExecute(args, result, Reader(), &Reader::ProcessScalar);
}

template <class Reader>
static void FromVariantListFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[0].GetType() == DDVariantType);
	VectorExecute(args, result, Reader(), &Reader::ProcessList);
}

static bool VariantAccessWrite(VectorWriter &result, const VectorReader &arg, yyjson_val *val, JSONAllocator &alc) {
	if (!val || unsafe_yyjson_is_null(val)) {
		return false;
	}
	std::string_view arg_type = arg[0].GetString();
	string res_type;
	if (arg_type.substr(0, 4) == "JSON" || arg_type.substr(0, 6) == "STRUCT") {
		switch (unsafe_yyjson_get_tag(val)) {
		case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
			res_type = "STRING";
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
			res_type = "FLOAT64";
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
			res_type = "INT64";
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
			res_type = yyjson_get_uint(val) > (uint64_t)std::numeric_limits<int64_t>::max() ? "FLOAT64" : "INT64";
			break;
		case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE:
		case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE:
			res_type = "BOOL";
			break;
		default:
			res_type = "JSON";
			break;
		}
	} else {
		res_type = arg_type.substr(0, arg_type.find('['));
	}

	VectorStructWriter writer = result.SetStruct();
	writer[0].SetString(res_type);
	auto res_doc = JSONCommon::CreateDocument(alc.GetYYAlc());
	yyjson_mut_doc_set_root(res_doc, yyjson_val_mut_copy(res_doc, val));
	size_t len;
	char *data = yyjson_mut_write_opts(res_doc, 0, alc.GetYYAlc(), &len, nullptr);
	writer[1].SetString(string_t(data, len));
	return true;
}

static bool VariantAccessIndexImpl(VectorWriter &result, const VectorReader &arg, const VectorReader &index) {
	int64_t idx = index.Get<int64_t>();
	JSONAllocator alc {Allocator::DefaultAllocator()};
	auto arg_doc = JSONCommon::ReadDocument(arg[1].Get<string_t>(), JSONCommon::READ_FLAG, alc.GetYYAlc());
	auto arg_root = yyjson_doc_get_root(arg_doc);
	return VariantAccessWrite(result, arg, yyjson_arr_get(arg_root, idx), alc);
}

static bool VariantAccessKeyImpl(VectorWriter &result, const VectorReader &arg, const VectorReader &index) {
	std::string_view key = index.GetString();
	JSONAllocator alc {Allocator::DefaultAllocator()};
	auto arg_doc = JSONCommon::ReadDocument(arg[1].Get<string_t>(), JSONCommon::READ_FLAG, alc.GetYYAlc());
	auto arg_root = yyjson_doc_get_root(arg_doc);
	return VariantAccessWrite(result, arg, yyjson_obj_getn(arg_root, key.data(), key.size()), alc);
}

static void VariantAccessIndexFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[0].GetType() == DDVariantType);
	VectorExecute(args, result, VariantAccessIndexImpl);
}

static void VariantAccessKeyFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[0].GetType() == DDVariantType);
	VectorExecute(args, result, VariantAccessKeyImpl);
}

static string VariantSortHashReal(std::string_view arg) {
	bool negative = arg[0] == '-';
	string res(4, '\0');
	res[0] = '2' - negative;
	int start = -1, pos_d = arg.size(), exp = 0;
	for (size_t i = negative; i < arg.size(); ++i) {
		char c = arg[i];
		if (c == '.') {
			pos_d = i;
			continue;
		}
		if (c == 'e' || c == 'E') {
			if (pos_d > i) {
				pos_d = i;
			}
			exp = atoi(&arg[i + 1]);
			break;
		}
		if (start < 0) {
			if (c == '0') {
				continue;
			}
			start = i;
		}
		res += negative ? '0' + '9' - c : c;
	}
	if (start < 0) {
		return "2";
	}
	exp += pos_d - start - (pos_d > start);
	char filler;
	if (negative) {
		filler = '9';
		exp = 500 - exp;
	} else {
		filler = '0';
		exp += 500;
	}
	std::to_chars(&res[1], &res[4], exp);
	res.append(77 - res.size() + 4, filler);
	return res;
}

static string VariantSortHashInt(const string &arg) {
	string res;
	if (arg == "0") {
		res = '2';
	} else if (arg[0] == '-') {
		res = '1' + to_string(502 - arg.size());
		for (size_t i = 1; i < arg.size(); ++i) {
			res += '0' + '9' - arg[i];
		}
		res.append(77 - arg.size() + 1, '9');
	} else {
		res = '2' + to_string(499 + arg.size());
		res.append(arg);
		res.append(77 - arg.size(), '0');
	}
	return res;
}

static bool VariantSortHashImpl(VectorWriter &writer, const VectorReader &arg, const VectorReader &case_sensitive) {
	JSONAllocator alc {Allocator::DefaultAllocator()};
	auto doc = JSONCommon::ReadDocument(arg[1].Get<string_t>(), JSONCommon::READ_FLAG, alc.GetYYAlc());
	auto val = yyjson_doc_get_root(doc);
	if (!val || unsafe_yyjson_is_null(val)) {
		return false;
	}
	string result;
	std::string_view tp = arg[0].GetString();
	bool is_json = tp == "JSON";
	auto js_tp = unsafe_yyjson_get_type(val);
	auto js_tag = unsafe_yyjson_get_tag(val);
	if (tp == "BOOL" || is_json && js_tp == YYJSON_TYPE_BOOL) {
		result = unsafe_yyjson_get_bool(val) ? "01" : "00";
	} else if (tp == "FLOAT64" || is_json && js_tag == (YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL)) {
		switch (js_tag) {
		case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
			if (string s = unsafe_yyjson_get_str(val); s == "NaN") {
				result = '1';
			} else if (s == "-Infinity") {
				result = "10";
			} else if (s == "Infinity") {
				result = "29";
			} else {
				return false;
			}
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
			if (double v = unsafe_yyjson_get_real(val); v == 0.0) {
				result = '2';
			} else {
				result = VariantSortHashReal(duckdb_fmt::format("{:.16e}", v));
			}
			break;
		default:
			return false;
		}
	} else if (tp == "INT64" || is_json && js_tp == YYJSON_TYPE_NUM) {
		switch (js_tag) {
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
			result = VariantSortHashInt(to_string(unsafe_yyjson_get_sint(val)));
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
			result = VariantSortHashInt(to_string(unsafe_yyjson_get_uint(val)));
			break;
		default:
			return false;
		}
	} else if (tp == "NUMERIC") {
		D_ASSERT(js_tp == YYJSON_TYPE_STR);
		result = VariantSortHashReal(unsafe_yyjson_get_str(val));
	} else if (tp == "STRING" || is_json && js_tp == YYJSON_TYPE_STR) {
		result = string("3") + unsafe_yyjson_get_str(val);
		if (!case_sensitive.Get<bool>()) {
			std::transform(result.begin(), result.end(), result.begin(),
			               [](unsigned char c) { return std::tolower(c); });
		}
	} else if (tp == "BYTES") {
		if (const char *s = yyjson_get_str(val)) {
			idx_t size = Blob::FromBase64Size(s);
			string decoded(size, '\0');
			Blob::FromBase64(s, (data_ptr_t)decoded.data(), size);
			result = '4';
			for (unsigned cp : decoded) {
				if (cp == 0) {
					cp = 256;
				}
				if (cp <= 0x7F) {
					result += cp;
				} else {
					result += (cp >> 6) + 192;
					result += (cp & 63) + 128;
				}
			}
		} else {
			return false;
		}
	} else if (tp == "TIME") {
		result = string("5") + unsafe_yyjson_get_str(val);
	} else if (tp == "DATE") {
		result = string("6") + unsafe_yyjson_get_str(val) + "T00:00:00";
	} else if (tp == "DATETIME") {
		result = string("6") + unsafe_yyjson_get_str(val);
	} else if (tp == "TIMESTAMP") {
		result = string("6") + unsafe_yyjson_get_str(val);
	} else if (tp == "INTERVAL") {
		const char *str_val = yyjson_get_str(val);
		interval_t iv;
		if (!IntervalFromISOString(str_val, strlen(str_val), iv)) {
			return false;
		}
		int64_t micros = Interval::GetMicro(iv);
		result = duckdb_fmt::format("7{:019d}000", micros + 943488000000000000);
	} else if (tp == "GEOGRAPHY") {
		result = string("8") + unsafe_yyjson_get_str(val);
	} else {
		auto res_doc = JSONCommon::CreateDocument(alc.GetYYAlc());
		yyjson_mut_doc_set_root(res_doc, yyjson_val_mut_copy(res_doc, val));
		size_t len;
		char *data = yyjson_mut_write_opts(res_doc, 0, alc.GetYYAlc(), &len, nullptr);
		result = '9';
		if (!case_sensitive.Get<bool>() &&
		    (tp == "STRING[]" || tp.substr(0, 4) == "JSON" || tp.substr(0, 6) == "STRUCT")) {
			std::transform(data, data + len, data, [](unsigned char c) { return std::tolower(c); });
		}
		result.append(data, len);
	}
	writer.SetString(result);
	return true;
}

static bool VariantFromSortHashNumber(VectorWriter &writer, bool negative, int ex, std::string_view digits,
                                      bool int_range) {
	if (digits.size() <= ex + 1 && int_range) {
		uint64_t res;
		std::from_chars(digits.data(), &digits[digits.size()], res);
		for (size_t i = ex + 1 - digits.size(); i-- > 0;) {
			res *= 10;
		}
		return VariantWriter(LogicalType::BIGINT).Process(writer, VectorHolder(int64_t(negative ? 0 - res : res))[0]);
	}
	string s;
	if (negative) {
		s += '-';
	}
	s += digits[0];
	s += '.';
	s.append(digits, 1);
	if (digits.size() < 17) {
		s.append(17 - digits.size(), '0');
	}
	s += duckdb_fmt::format("e{:+03d}", ex);
	double d = stod(s);
	if (duckdb_fmt::format("{:.16e}", d) == s) {
		return VariantWriter(LogicalType::DOUBLE).Process(writer, VectorHolder(d)[0]);
	}
	hugeint_t v;
	string error;
	try {
		if (!TryCastToDecimal::Operation(string_t(s), v, &error, dd_numeric_width, dd_numeric_scale)) {
			return false;
		}
	} catch (OutOfRangeException) {
		return false;
	}
	return VariantWriter(DDNumericType).Process(writer, VectorHolder(v)[0]);
}

static bool VariantFromSortHashImpl(VectorWriter &writer, const VectorReader &reader) {
	std::string_view arg = reader.GetString();
	switch (arg[0]) {
	case '0': {
		bool res = arg[1] == '1';
		return VariantWriter(LogicalType::BOOLEAN).Process(writer, VectorHolder(res)[0]);
	}
	case '1': {
		double res;
		if (arg.size() == 1) {
			res = NAN;
		} else if (arg.size() == 2 && arg[1] == '0') {
			res = -std::numeric_limits<double>::infinity();
		} else {
			const char *start = &arg[4], *end = &arg.back();
			while (end >= start && *end == '9') {
				--end;
			}
			string s;
			s.reserve(end - start + 1);
			while (start <= end) {
				s += '0' + '9' - *start++;
			}
			int ex;
			std::from_chars(&arg[1], &arg[4], ex);
			return VariantFromSortHashNumber(writer, true, 500 - ex, s,
			                                 arg >= "14820776627963145224191" && arg <= "15009");
		}
		return VariantWriter(LogicalType::DOUBLE).Process(writer, VectorHolder(res)[0]);
	}
	case '2': {
		if (arg.size() == 1) {
			return VariantWriter(LogicalType::INTEGER).Process(writer, VectorHolder(int32_t(0))[0]);
		} else if (arg.size() == 2 && arg[1] == '9') {
			return VariantWriter(LogicalType::DOUBLE)
			    .Process(writer, VectorHolder(std::numeric_limits<double>::infinity())[0]);
		}
		std::string_view s(&arg[4], arg.size() - 4);
		s.remove_suffix(s.size() - 1 - s.find_last_not_of('0'));
		int ex;
		std::from_chars(&arg[1], &arg[4], ex);
		return VariantFromSortHashNumber(writer, false, ex - 500, s,
		                                 arg >= "25001" && arg <= "251892233720368547758071");
	}
	case '3':
		return VariantWriter(LogicalType::VARCHAR).Process(writer, VectorHolder(arg.substr(1))[0]);
	case '4': {
		string decoded;
		for (size_t i = 1; i < arg.size(); ++i) {
			unsigned c = (unsigned char)arg[i];
			if (c <= 127) {
				decoded += c;
			} else {
				D_ASSERT(c >= 192 && c <= 196);
				c = ((c - 192) << 6) + ((unsigned char)arg[++i] - 128);
				if (c == 256) {
					c = 0;
				}
				decoded += c;
			}
		}
		return VariantWriter(LogicalType::BLOB).Process(writer, VectorHolder(string_t(decoded))[0]);
	}
	case '5':
		arg.remove_prefix(1);
		return VariantWriter(LogicalType::TIME)
		    .Process(writer, VectorHolder(Time::FromCString(arg.data(), arg.size(), true))[0]);
	case '6':
		arg.remove_prefix(1);
		if (arg.size() >= 9 && arg.substr(arg.size() - 9, arg.npos) == "T00:00:00") {
			return VariantWriter(LogicalType::DATE)
			    .Process(writer, VectorHolder(Date::FromCString(arg.data(), arg.size()))[0]);
		} else {
			return VariantWriter(LogicalType::TIMESTAMP)
			    .Process(writer, VectorHolder(Timestamp::FromCString(arg.data(), arg.size()))[0]);
		}
	case '7': {
		int64_t micros;
		std::from_chars(&arg[1], &arg[arg.size() - 3], micros);
		micros -= 943488000000000000;
		return VariantWriter(LogicalType::INTERVAL).Process(writer, VectorHolder(Interval::FromMicro(micros))[0]);
	}
	case '8': {
		string wkt(arg.substr(1));
		GSERIALIZED *gser = LWGEOM_from_text((char *)wkt.data());
		if (!gser) {
			return false;
		}
		string wkb = Geometry::ToGeometry(gser);
		Geometry::DestroyGeometry(gser);
		return VariantWriter(DDGeoType).Process(writer, VectorHolder(string_t(wkb))[0]);
	}
	case '9': {
		return VariantWriter(DDJsonType).Process(writer, VectorHolder(arg.substr(1))[0]);
	}
	default:
		return false;
	}
}

static void VariantSortHash(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[0].GetType() == DDVariantType);
	VectorExecute(args, result, VariantSortHashImpl);
}

static void VariantFromSortHash(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[0].GetType().id() == LogicalTypeId::VARCHAR);
	VectorExecute(args, result, VariantFromSortHashImpl);
}

} // namespace

#define REGISTER_FUNCTION(TYPE, SQL_NAME, C_NAME)                                                                      \
	CreateScalarFunctionInfo from_variant_##SQL_NAME##_info(                                                           \
	    ScalarFunction("from_variant_" #SQL_NAME, {DDVariantType}, TYPE, FromVariantFunc<VariantReader##C_NAME>));     \
	catalog.CreateFunction(context, from_variant_##SQL_NAME##_info);                                                   \
	CreateScalarFunctionInfo from_variant_##SQL_NAME##_array_info(                                                     \
	    ScalarFunction("from_variant_" #SQL_NAME "_array", {DDVariantType}, LogicalType::LIST(TYPE),                   \
	                   FromVariantListFunc<VariantReader##C_NAME>));                                                   \
	catalog.CreateFunction(context, from_variant_##SQL_NAME##_array_info);

#define REGISTER_CAST_FUNCTION(TYPE, casts)                                                                            \
	casts.RegisterCastFunction(DDVariantType, TYPE, VariantCasts::VariantCastAny);                                     \
	casts.RegisterCastFunction(DDVariantType, LogicalType::LIST(TYPE), VariantCasts::VariantCastAny);

void DataDocsExtension::LoadVariant(Connection &con) {
	auto &context = *con.context;
	auto &catalog = Catalog::GetSystemCatalog(context);

	// add the "variant" type
	CreateTypeInfo variant_type_info("VARIANT", DDVariantType);
	variant_type_info.temporary = true;
	variant_type_info.internal = true;
	catalog.CreateType(*con.context, variant_type_info);

	// add variant casts
	auto &config = DBConfig::GetConfig(*con.context);
	auto &casts = config.GetCastFunctions();
	REGISTER_CAST_FUNCTION(LogicalType::VARCHAR, casts);
	REGISTER_CAST_FUNCTION(LogicalType::INTEGER, casts);
	REGISTER_CAST_FUNCTION(LogicalType::BIGINT, casts);
	REGISTER_CAST_FUNCTION(LogicalType::DOUBLE, casts);
	REGISTER_CAST_FUNCTION(DDNumericType, casts);
	REGISTER_CAST_FUNCTION(DefaultDecimalType, casts);
	REGISTER_CAST_FUNCTION(LogicalType::BLOB, casts);
	REGISTER_CAST_FUNCTION(LogicalType::DATE, casts);
	REGISTER_CAST_FUNCTION(LogicalType::TIME, casts);
	REGISTER_CAST_FUNCTION(LogicalType::TIMESTAMP_TZ, casts);
	REGISTER_CAST_FUNCTION(LogicalType::TIMESTAMP, casts);
	REGISTER_CAST_FUNCTION(LogicalType::INTERVAL, casts);
	REGISTER_CAST_FUNCTION(DDGeoType, casts);
	REGISTER_CAST_FUNCTION(DDJsonType, casts);

	CreateScalarFunctionInfo variant_info(
	    ScalarFunction("variant", {LogicalType::ANY}, DDVariantType, VariantFunction));
	catalog.CreateFunction(context, variant_info);

	REGISTER_FUNCTION(LogicalType::BOOLEAN, bool, Bool)
	REGISTER_FUNCTION(LogicalType::BIGINT, int64, Int64)
	REGISTER_FUNCTION(LogicalType::DOUBLE, float64, Float64)
	REGISTER_FUNCTION(DDNumericType, numeric, Numeric)
	REGISTER_FUNCTION(LogicalType::VARCHAR, string, String)
	REGISTER_FUNCTION(LogicalType::BLOB, bytes, Bytes)
	REGISTER_FUNCTION(LogicalType::DATE, date, Date)
	REGISTER_FUNCTION(LogicalType::TIME, time, Time)
	REGISTER_FUNCTION(LogicalType::TIMESTAMP_TZ, timestamp, Timestamp)
	REGISTER_FUNCTION(LogicalType::TIMESTAMP, datetime, Datetime)
	REGISTER_FUNCTION(LogicalType::INTERVAL, interval, Interval)
	REGISTER_FUNCTION(DDJsonType, json, JSON)
	REGISTER_FUNCTION(DDGeoType, geography, Geography)

	ScalarFunctionSet variant_access_set("variant_access");
	variant_access_set.AddFunction(
	    ScalarFunction({DDVariantType, LogicalType::BIGINT}, DDVariantType, VariantAccessIndexFunc));
	variant_access_set.AddFunction(
	    ScalarFunction({DDVariantType, LogicalType::VARCHAR}, DDVariantType, VariantAccessKeyFunc));
	CreateScalarFunctionInfo variant_access_info(std::move(variant_access_set));
	catalog.CreateFunction(context, variant_access_info);

	CreateScalarFunctionInfo sort_hash_info(ScalarFunction("variant_sort_hash", {DDVariantType, LogicalType::BOOLEAN},
	                                                       LogicalType::VARCHAR, VariantSortHash));
	catalog.CreateFunction(context, sort_hash_info);

	CreateScalarFunctionInfo from_sort_hash_info(
	    ScalarFunction("variant_from_sort_hash", {LogicalType::VARCHAR}, DDVariantType, VariantFromSortHash));
	catalog.CreateFunction(context, from_sort_hash_info);
}

} // namespace duckdb
