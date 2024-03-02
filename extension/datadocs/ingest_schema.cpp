#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
namespace rj = rapidjson;
#include "json_common.hpp"

#include "inferrer_impl.h"
#include "ingest_schema.hpp"

namespace duckdb {

class StdStringBuffer {
public:
	typedef char Ch;
	StdStringBuffer(std::string &s) : str(s) { str.reserve(256); }
	void Put(char c) { str.push_back(c); }
	void Flush() {}
	std::string &str;
};

struct StructJsonField {
	size_t offset;
	bool (*reader)(char *, yyjson_val *);
	void (*writer)(char *, rj::Writer<StdStringBuffer> &);
};

class StructJsonMap : public std::unordered_map<std::string_view, StructJsonField> {
public:
	using std::unordered_map<std::string_view, StructJsonField>::unordered_map;
	void read(yyjson_val *json, void *obj) const {
		size_t idx, max;
		yyjson_val *key, *val;
		yyjson_obj_foreach(json, idx, max, key, val) {
			auto it = find(std::string_view(unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key)));
			if (it != end()) {
				if (!it->second.reader((char *)obj + it->second.offset, val)) {
					throw InvalidInputException("Invalid value of \"%s\"", unsafe_yyjson_get_str(key));
				}
			}
		}
	}
	void write(rj::Writer<StdStringBuffer> &json, void *obj) const {
		//json.StartObject();
		for (const auto &[key, value] : *this) {
			json.Key(key.data(), key.size());
			value.writer((char *)obj + value.offset, json);
		}
		//json.EndObject();
	}
};

template <typename T>
struct jhelper {};

template <>
struct jhelper<int> {
	static bool read(int &obj, yyjson_val *json) {
		if (!yyjson_is_int(json)) {
			return false;
		}
		obj = unsafe_yyjson_get_int(json);
		return true;
	}
	static void write(int obj, rj::Writer<StdStringBuffer> &json) {
		json.Int(obj);
	}
};

template <>
struct jhelper<size_t> {
	static bool read(size_t &obj, yyjson_val *json) {
		if (!yyjson_is_uint(json)) {
			return false;
		}
		obj = unsafe_yyjson_get_uint(json);
		return true;
	}
	static void write(size_t obj, rj::Writer<StdStringBuffer> &json) {
		json.Uint(obj);
	}
};

template <>
struct jhelper<uint8_t> {
	static bool read(uint8_t &obj, yyjson_val *json) {
		if (!yyjson_is_uint(json)) {
			return false;
		}
		obj = unsafe_yyjson_get_uint(json);
		return true;
	}
	static void write(uint8_t obj, rj::Writer<StdStringBuffer> &json) {
		json.Uint(obj);
	}
};

template <>
struct jhelper<bool> {
	static bool read(bool &obj, yyjson_val *json) {
		if (!yyjson_is_bool(json)) {
			return false;
		}
		obj = unsafe_yyjson_get_bool(json);
		return true;
	}
	static void write(bool obj, rj::Writer<StdStringBuffer> &json) {
		json.Bool(obj);
	}
};

template <>
struct jhelper<char> {
	static bool read(char &obj, yyjson_val *json) {
		if (!yyjson_is_str(json)) {
			return false;
		}
		obj = unsafe_yyjson_get_str(json)[0];
		return true;
	}
	static void write(const char &obj, rj::Writer<StdStringBuffer> &json) {
		json.String(&obj, obj != '\0');
	}
};

template <>
struct jhelper<std::string> {
	static bool read(std::string &obj, yyjson_val *json) {
		if (!yyjson_is_str(json)) {
			return false;
		}
		obj.assign(unsafe_yyjson_get_str(json), unsafe_yyjson_get_len(json));
		return true;
	}
	static void write(const std::string &obj, rj::Writer<StdStringBuffer> &json) {
		json.String(obj.data(), obj.size());
	}
};

template <typename T>
struct jhelper<std::vector<T>> {
	static bool read(std::vector<T> &obj, yyjson_val *json) {
		if (!yyjson_is_arr(json)) {
			return false;
		}
		obj.clear();
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(json, idx, max, val) {
			obj.emplace_back();
			if (!jhelper<T>::read(obj.back(), val)) {
				return false;
			}
		}
		return true;
	}

	static void write(const std::vector<T> &obj, rj::Writer<StdStringBuffer> &json) {
		json.StartArray();
		for (auto &item : obj) {
			jhelper<T>::write(item, json);
		}
		json.EndArray();
	}
};

template <typename T>
static bool jread(char *obj, yyjson_val *json) {
	return jhelper<T>::read(*(T*)obj, json);
}

template <typename T>
static void jwrite(char *obj, rj::Writer<StdStringBuffer> &json) {
	return jhelper<T>::write(*(T*)obj, json);
}

#define JSON_FIELD(cls, attr) {#attr, {offsetof(cls, attr), jread<decltype(cls::attr)>, jwrite<decltype(cls::attr)>}}

static const std::vector<std::string> type_index {
	"VARCHAR",
	"BOOLEAN",
	"BIGINT",
	"DOUBLE",
	"DATE",
	"TIME",
	"TIMESTAMP",
	"BLOB",
	"NUMERIC",
	"GEOGRAPHY",
	"STRUCT",
	"VARIANT"
};

template <class E>
class EnumMap : public std::unordered_map<std::string, E> {
public:
	EnumMap(const std::vector<std::string> &index) {
		for (size_t i = 0; i < index.size(); ++i) {
			this->emplace(index[i], static_cast<E>(i));
		}
	}
};

static const EnumMap<ColumnType> type_map(type_index);

template <>
class jhelper<IngestColumnDefinition> {
public:
	static bool read(IngestColumnDefinition &obj, yyjson_val *json) {
		if (!yyjson_is_obj(json)) {
			return false;
		}
		struct_map.read(json, &obj);
		return true;
	}

	static void write(const IngestColumnDefinition &obj, rj::Writer<StdStringBuffer> &json) {
		json.StartObject();
		json.Key("name"); json.String(obj.name);
		json.Key("type");
		if (obj.list_levels == 0) {
			json.String(type_index[(size_t)obj.column_type]);
		} else {
			string s = type_index[(size_t)obj.column_type];
			for (int i = obj.list_levels; i --> 0;) {
				s += "[]";
			}
			json.String(s.data(), s.size());
		}
		if (obj.index > 0) {
			json.Key("index"); json.Int(obj.index);
		}
		if (!obj.format.empty()) {
			json.Key("format"); json.String(obj.format.data(), obj.format.size());
		}
		if (obj.is_json) {
			json.Key("is_json"); json.Bool(true);
		}
		if (obj.column_type == ColumnType::Numeric) {
			json.Key("i_digits"); json.Int(obj.i_digits);
			json.Key("f_digits"); json.Int(obj.f_digits);
		}
		if (!obj.fields.empty()) {
			json.Key("fields");
			jhelper<std::vector<IngestColumnDefinition>>::write(obj.fields, json);
		}
		json.EndObject();
	}

private:
	static bool read_type(char *obj, yyjson_val *json) {
		if (!yyjson_is_str(json)) {
			return false;
		}
		IngestColumnDefinition *col = (IngestColumnDefinition *)obj;
		std::string_view s(unsafe_yyjson_get_str(json), unsafe_yyjson_get_len(json));
		auto pos = s.find('[');
		if (pos != std::string_view::npos) {
			size_t suffix = s.size() - pos;
			col->list_levels = suffix / 2;
			s.remove_suffix(suffix);
		}
		auto it = type_map.find(string(s));
		if (it == type_map.end()) {
			throw InvalidInputException("Invalid type: %s", s.data());
		}
		col->column_type = it->second;
		return true;
	}

	inline static const StructJsonMap struct_map {
		JSON_FIELD(IngestColumnDefinition, name),
		{"type", {0, read_type, nullptr}},
		JSON_FIELD(IngestColumnDefinition, index),
		JSON_FIELD(IngestColumnDefinition, format),
		JSON_FIELD(IngestColumnDefinition, is_json),
		JSON_FIELD(IngestColumnDefinition, i_digits),
		JSON_FIELD(IngestColumnDefinition, f_digits),
		JSON_FIELD(IngestColumnDefinition, fields)
	};
};

static const StructJsonMap schema_map {
	JSON_FIELD(Schema, fields)
};

static const StructJsonMap csv_schema_map {
	JSON_FIELD(CSVSchema, remove_null_strings),
	JSON_FIELD(CSVSchema, delimiter),
	JSON_FIELD(CSVSchema, quote_char),
	JSON_FIELD(CSVSchema, escape_char),
	JSON_FIELD(CSVSchema, newline),
	JSON_FIELD(CSVSchema, comment),
	JSON_FIELD(CSVSchema, charset),
	JSON_FIELD(CSVSchema, header_row),
	JSON_FIELD(CSVSchema, first_data_row),
	JSON_FIELD(CSVSchema, fields)
};

static const StructJsonMap xls_schema_map {
	JSON_FIELD(XLSSchema, remove_null_strings),
	JSON_FIELD(XLSSchema, comment),
	JSON_FIELD(XLSSchema, header_row),
	JSON_FIELD(XLSSchema, first_data_row),
	JSON_FIELD(XLSSchema, fields)
};

static const StructJsonMap json_schema_map {
	JSON_FIELD(JSONSchema, start_path),
	JSON_FIELD(JSONSchema, fields)
};

const StructJsonMap *Schema::GetJsonMap() {
	return &schema_map;
}
const StructJsonMap *CSVSchema::GetJsonMap() {
	return &csv_schema_map;
}
const StructJsonMap *XLSSchema::GetJsonMap() {
	return &xls_schema_map;
}
const StructJsonMap *JSONSchema::GetJsonMap() {
	return &json_schema_map;
}

void Schema::FromJson(yyjson_val *json) {
	GetJsonMap()->read(json, this);
}

std::string ingest_get_schema(Connection &conn, const std::string &file_name, const std::string &path) {
	std::string json;
	StdStringBuffer buf(json);
	rj::Writer<StdStringBuffer> writer(buf);
	std::unique_ptr<Parser> parser(Parser::get_parser(file_name, *conn.context));

	writer.StartObject();
	writer.Key("type");
	bool sheet_selected;
	if (path[0] == '[') {
		JSONAllocator alc {Allocator::DefaultAllocator()};
		auto doc = JSONCommon::ReadDocument(path, JSONCommon::READ_FLAG, alc.GetYYAlc());
		sheet_selected = parser->select_path(yyjson_doc_get_root(doc));
	} else {
		sheet_selected = parser->select_path(path);
	}

	if (parser->get_file_count() > 0) {
		writer.String("items");
		writer.Key("items"); writer.StartArray();
		for (const std::string &name : parser->get_file_names()) {
			writer.String(name);
		}
		writer.EndArray();
	} else if (!sheet_selected && parser->get_sheet_count() > 1) {
		writer.String("items");
		writer.Key("items"); writer.StartArray();
		for (const std::string &name : parser->get_sheet_names()) {
			writer.String(name);
		}
		writer.EndArray();
	} else {
		if (parser->infer_schema()) {
			writer.String("data");
			auto schema = parser->get_schema();
			schema->GetJsonMap()->write(writer, schema);
		} else {
			writer.String("error");
			writer.Key("msg");
			writer.String("Cannot ingest file");
		}
	}
	writer.EndObject();
	return json;
}


bool Parser::select_path(const std::string_view &path) {
	bool sheet_selected = false;
	const char *start = path.data();
	const char *end = start + path.size();
	while (start < end) {
		if (get_file_count() > 0) {
			size_t i;
			for (i = 0; start[i] && start[i] != '|'; ++i);
			std::string_view name(start, i);
			if (!select_file(name)) {
				throw InvalidInputException("File not found: %s", name.data());
			}
			start += i + 1;
		} else {
			std::string_view name(start, end - start);
			if (!select_sheet(name)) {
				throw InvalidInputException("Table not found: %s", name.data());
			}
			sheet_selected = true;
			break;
		}
	}
	return sheet_selected;
}

bool Parser::select_path(yyjson_val *path) {
	bool sheet_selected = false;
	size_t idx, max;
	yyjson_val *val;
	yyjson_arr_foreach(path, idx, max, val) {
		bool res;
		sheet_selected = get_sheet_count() > 0;
		if (yyjson_is_str(val)) {
			std::string_view item(unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val));
			res = sheet_selected ? select_sheet(item) : select_file(item);
		} else if (yyjson_is_uint(val)) {
			size_t item = unsafe_yyjson_get_uint(val);
			res = sheet_selected ? select_sheet(item) : select_file(item);
		} else {
			res = false;
		}
		if (!res) {
			throw InvalidInputException("Invalid path");
		}
	}
	return sheet_selected;
}

} // namespace duckdb