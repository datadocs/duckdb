#ifndef INFERRER_H
#define INFERRER_H

#include "duckdb.hpp"
#include "yyjson.hpp"

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

enum class ColumnType : uint8_t {
	String,
	Boolean,
	Integer,
	Decimal,
	Date,
	Time,
	Datetime,
	Interval,
	Bytes,
	Numeric,
	Geography,
	Struct,
	Variant,
	JSON
};

struct VariantCell
{
	typedef int32_t IndexType;
	inline static constexpr
	int inplace_size[] {    0,     -1,       1,       4,        4,        -1,         -1,    -1,    4,   -1,       -1,    -1,      -1,        -1,     -1,   -1 };
	enum VariantTypeId { Null, String, Boolean, Integer, Unsigned, Integer64, Unsigned64, Float, Date, Time, Datetime, Bytes, Numeric, Geography, Struct, List };

	template<VariantTypeId new_type, typename T>
	void assign(T new_value)
	{
		type = new_type;
		if constexpr (inplace_size[new_type] <= 0)
			data.assign((const char*)&new_value, sizeof(new_value));
		else if constexpr (inplace_size[new_type] == 1)
			data = (unsigned char)new_value;
		else
		{
			static_assert (sizeof(T) == sizeof(IndexType) && sizeof(T) == inplace_size[new_type]);
			IndexType value = *(IndexType*)&new_value;
			data.assign((const char*)&value, sizeof(value));
		}
	}

	VariantTypeId type = Null;
	std::string data;
};

class Cell;
class Cell : public std::variant<std::string, bool, int64_t, int32_t, int16_t, int8_t, double, std::vector<Cell>, VariantCell>
{
public:
	using base = std::variant<std::string, bool, int64_t, int32_t, int16_t, int8_t, double, std::vector<Cell>, VariantCell>;
	using base::base;
	using base::operator =;
};

enum class InferrerErrorCode { NoError = 0, TypeError = 1 };

struct InferrerErrorType
{
	InferrerErrorCode error_code;
	std::string value;
	static const InferrerErrorType NoErrorValue;
};

// enum ServiceColumns { COL_ROWNUM = -1 };

struct IngestColumnDefinition
{
	std::string name;
	ColumnType column_type;
	int index; // source column 1-based
	int list_levels;
	std::string format; // datetime format string
	double bytes_per_value; // estimate for strings
	bool is_json;
	uint8_t i_digits;
	uint8_t f_digits;
	std::vector<IngestColumnDefinition> fields; // nested columns for Struct type
};

enum SchemaStatus { STATUS_OK = 0, STATUS_INVALID_FILE = 1 };

class StructJsonMap;

class Schema
{
public:
	std::vector<IngestColumnDefinition> fields;
	SchemaStatus status = STATUS_OK;
	bool remove_null_strings = true; // "NULL" and "null" strings signify null values
	bool has_truncated_string = false; // if a string longer than allowed limit was truncated
	size_t nrows = 0; // estimated number of rows to reserve memory for
	virtual ~Schema() = default;
	virtual const StructJsonMap *GetJsonMap();
	void FromJson(yyjson_val *json);
};

class CSVSchema : public Schema
{
public:
	char delimiter;
	char quote_char; // \0 if not used
	char escape_char; // \0 if not used
	std::string newline; // empty string means any combination of \r\n
	std::string comment;
	std::string charset;
	int header_row;
	size_t first_data_row; // 1-based ignoring empty text rows
	size_t comment_lines_skipped_in_parsing = 0;
	virtual ~CSVSchema() = default;
	virtual const StructJsonMap *GetJsonMap() override;
};

class XLSSchema : public Schema
{
public:
	std::string comment;
	int header_row;
	size_t first_data_row;
	size_t comment_lines_skipped_in_parsing = 0;
	virtual ~XLSSchema() = default;
	virtual const StructJsonMap *GetJsonMap() override;
};

class JSONSchema : public Schema
{
public:
	std::vector<std::string> start_path;
	virtual ~JSONSchema() = default;
	virtual const StructJsonMap *GetJsonMap() override;
};

class Parser
{
public:
	static Parser* get_parser(const std::string& filename, ClientContext &context);
	virtual ~Parser() = default;
	virtual bool infer_schema() = 0;
	virtual Schema* get_schema() = 0; // returns pointer to instance member, do not delete
	virtual bool open() = 0;
	virtual void close() = 0;
	virtual void BuildColumns() = 0;
	virtual void BindSchema(std::vector<LogicalType> &return_types, std::vector<string> &names) = 0;
	virtual idx_t FillChunk(DataChunk &output) = 0;
	virtual int get_percent_complete() = 0;
	virtual size_t get_sheet_count() = 0;
	virtual std::vector<std::string> get_sheet_names() = 0;
	virtual bool select_sheet(const std::string_view &sheet_name) = 0;
	virtual bool select_sheet(size_t sheet_number) = 0;
	virtual size_t get_file_count() = 0;
	virtual std::vector<std::string> get_file_names() = 0;
	virtual bool select_file(const std::string_view &file_name) = 0;
	virtual bool select_file(size_t file_number) = 0;
	bool select_path(const std::string_view &path);
	bool select_path(yyjson_val *path);
public:
	bool is_finished = false;
};

}

#endif
