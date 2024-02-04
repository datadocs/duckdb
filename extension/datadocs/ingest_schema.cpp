#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
namespace rj = rapidjson;

#include "inferrer.h"
#include "ingest_schema.hpp"

namespace duckdb {

std::string ingest_get_schema(Connection &conn, const std::string &file_name, const std::string &path) {

	class StdStringBuffer {
	public:
		typedef char Ch;
		StdStringBuffer(std::string &s) : str(s) { str.reserve(256); }
		void Put(char c) { str.push_back(c); }
		void Flush() {}
		std::string &str;
	};

	class StdStringWriter : public rj::Writer<StdStringBuffer> {
	public:
		using rj::Writer<StdStringBuffer>::Writer;
		void Error(const char *message, const std::string_view &name) {
			String("error");
			Key("msg");
			string s = message; s += name;
			String(s);
			EndObject();
		}
	};

	std::string json;
	StdStringBuffer buf(json);
	StdStringWriter writer(buf);
	std::unique_ptr<Parser> parser(Parser::get_parser(file_name, *conn.context));

	writer.StartObject();
	writer.Key("type");
	bool sheet_selected = false;
	const char *start = path.data();
	const char *end = start + path.size();
	while (start < end) {
		if (parser->get_file_count() > 0) {
			size_t i;
			for (i = 0; start[i] && start[i] != '|'; ++i);
			std::string_view name(start, i);
			if (!parser->select_file(name)) {
				writer.Error("File not found: ", name);
				return json;
			}
			start += i + 1;
		} else {
			std::string_view name(start, end - start);
			if (!parser->select_sheet(name)) {
				writer.Error("Table not found: ", name);
				return json;
			}
			sheet_selected = true;
			break;
		}
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
		if (!parser->infer_schema()) {
			writer.Error("Cannot ingest file", std::string_view());
			return json;
		}
		writer.String("data");
		auto schema = parser->get_schema();
		writer.Key("options"); writer.StartObject();
		writer.EndObject();

		const auto json_columns = [&writer](const auto &self, const std::vector<IngestColumnDefinition> &fields) -> void {
			writer.Key("fields"); writer.StartArray();
			for (const IngestColumnDefinition &col : fields) {
				writer.StartObject();
				writer.Key("name"); writer.String(col.column_name);
				if (col.list_levels > 0) {
					writer.Key("list_levels"); writer.Int(col.list_levels);
				}
				if (!col.fields.empty()) {
					self(self, col.fields);
				}
				writer.EndObject();
			}
			writer.EndArray();
		};

		json_columns(json_columns, schema->columns);
	}

	writer.EndObject();
	return json;
}

} // namespace duckdb