#include <ctype.h>
#include <cstdlib>
#include <cctype>
#include <cstring>
#include <cmath>
#include <ctime>
#include <type_traits>
#include <utility>
#include <functional>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <unordered_map>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <regex>

#define BOOST_NO_CXX98_FUNCTION_BASE

#include <boost/iterator/indirect_iterator.hpp>
#include <boost/unordered_map.hpp>

#include "duckdb/common/string_util.hpp"
#include "column.hpp"

#include "xls/xlscommon.h"

#include "inferrer_impl.h"
#include "file_reader.h"
#include "csv_reader.h"
#include "xls_reader.h"
#include "zip_reader.h"
#include "json_reader.h"
#include "xml_reader.h"
#include "utility.h"
#include "type_conv.h"
#include "wkt.h"

using namespace std::literals;
using namespace std::string_literals;

namespace std
{
	template<> struct hash<duckdb::CellRawDate>
	{
		size_t operator() (const duckdb::CellRawDate& v) const noexcept
		{
			return std::hash<double>()(v.d);
		}
	};
}

namespace duckdb {

const InferrerErrorType InferrerErrorType::NoErrorValue = {};
	
/* ========= merge with xls_common from master ========= */
static const double pow_10[] =
{
	1e+0,
	1e+1,  1e+2,  1e+3,  1e+4,  1e+5,  1e+6,  1e+7,  1e+8,  1e+9,  1e+10, 1e+11, 1e+12, 1e+13, 1e+14, 1e+15, 1e+16, 1e+17, 1e+18, 1e+19, 1e+20,
	1e+21, 1e+22
}; 

double fast_strtod(const char* str, char **endptr)
{
	const char* s = str;
	uint64_t n = 0;
	int exp = 0;
	int frac_length = 0;
	size_t c;

	while (*s == ' ' || *s == '\t' || *s == '\r' || *s == '\n')
		++s;

	bool minus = *s == '-';
	if (minus || *s == '+')
		++s;

	while ((c = size_t(*s - '0')) <= 9)
	{
		n = n * 10 + c;
		++s;
	}

	if (*s == '.')
	{
		++s;
		const char* frac_start = s;
		while ((c = size_t(*s - '0')) <= 9)
		{
			n = n * 10 + c;
			++s;
		}
		frac_length = s - frac_start;
	}

	if (*s == 'E' || *s == 'e')
	{
		++s;
		bool exp_minus = *s == '-';
		if (exp_minus || *s == '+')
			++s;
		while ((c = size_t(*s - '0')) <= 9)
		{
			exp = exp * 10 + c;
			++s;
		}
		if (exp_minus)
			exp = -exp;
	}

	exp -= frac_length;
	double d = (double)n;

	if (exp > 22 && exp < 22 + 16)
	{
		d *= pow_10[exp - 22];
		exp = 22;
	}

	if (exp >= -22 && exp <= 22 && d <= 9007199254740991.0)
	{
		if (exp < 0)
			d /= pow_10[-exp];
		else
			d *= pow_10[exp];
		if(endptr) *endptr = (char*)s;
		return minus ? -d : d;
	}
	return strtod(str, endptr);
}
/* ========== end of to-merge ============= */

static bool cell_empty(const CellRaw& cell)
{
	const std::string* s = std::get_if<std::string>(&cell);
	return s && s->empty();
}

static bool cell_null_str(const CellRaw& cell)
{
	const std::string* s = std::get_if<std::string>(&cell);
	return s && (s->empty() || *s == "NULL" || *s == "null");
}

Parser* Parser::get_parser(const std::string& filename, ClientContext &context)
{
	return ParserImpl::get_parser_from_reader(std::make_shared<FileReader>(filename, context));
}

template <class Parser>
ParserImpl* create_parser(std::shared_ptr<BaseReader> reader)
{
	return new Parser(reader);
}

static const std::unordered_map<std::string, decltype(&create_parser<CSVParser>)> parser_extensions {
	{"xls", create_parser<XLSParser>},
	{"xlt", create_parser<XLSParser>},
	{"xlsx", create_parser<XLSXParser>},
	{"xlsm", create_parser<XLSXParser>},
	{"xltx", create_parser<XLSXParser>},
	{"xltm", create_parser<XLSXParser>},
	{"json", create_parser<JSONParser>},
	{"geojson", create_parser<JSONParser>},
	{"xml", create_parser<XMLParser>},
	{"zip", create_parser<ZIPParser>},
	{"wkt", create_parser<WKTParser>},
};

static const std::regex _re_is_xml_file(R"(\s*<(\?xml|!--|!DOCTYPE|\w+(\s|>|/>)))", std::regex::nosubs);
static const std::regex _re_is_json_file(R"(\s*(\{\s*"([^"\\]|\\.)*"\s*:|\[)\s*(["{[0-9.\-]|true|false|null))", std::regex::nosubs);

ParserImpl* ParserImpl::get_parser_from_reader(std::shared_ptr<BaseReader> reader)
{
	ParserImpl* parser = nullptr;
	const std::string& fname = reader->filename();
	size_t pos_ext = fname.find_last_of("./\\");
	if (pos_ext == std::string::npos || fname[pos_ext] == '/' || fname[pos_ext] == '\\') // no extension
	{
		if (!reader->open())
			return nullptr;
		reader->skip_prefix("\xEF\xBB\xBF"sv);
		const size_t preview_size = 100;
		char data[preview_size+1];
		data[reader->read(data, preview_size)] = 0;
		reader->close();
		if (std::regex_search(data, _re_is_xml_file, std::regex_constants::match_continuous))
			return new XMLParser(reader);
		if (std::regex_search(data, _re_is_json_file))
			return new JSONParser(reader);
		return new CSVParser(reader);
	}

	std::string ext = fname.substr(pos_ext+1);
	std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
	auto it = parser_extensions.find(ext);
	if (it == parser_extensions.end())
		return create_parser<CSVParser>(reader);
	return it->second(reader);
}

void ParserImpl::close() {}

static int ConvertToString(CellRaw& src, Cell& dst, const IngestColumnDefinition& format)
{
	std::visit(overloaded{
	[&](std::string& s) { dst = std::move(s); },
	[&](int64_t v) { dst = std::to_string(v); },
	[&](bool v) { dst.emplace<std::string>(v ? "true" : "false"); },
	[&](double v) { std::ostringstream ss; ss << v; dst = ss.str(); },
	[&](const CellRawDate& v)
	{
		double value = v.d;
		const char* fmt;
		std::tm tm = {};
		if (value >= 1.0)
		{
			int l = (int)value + 68569 + 2415019;
			int n = 4 * l / 146097;
			l -= (146097 * n + 3) / 4;
			int i = 4000 * (l + 1) / 1461001;
			l -= 1461 * i / 4 - 31;
			int j = 80 * l / 2447;
			tm.tm_mday = l - 2447 * j / 80;
			l = j / 11;
			tm.tm_mon = j + 2 - 12 * l - 1;
			tm.tm_year = 100 * (n - 49) + i + l - 1900;
			/*tm.tm_year = 400;
			tm.tm_mday = (int)value - 1;
			std::mktime(&tm);
			tm.tm_year -= 400;*/
		}
		if (value != std::trunc(value))
		{
			int time = std::lround((value - int(value)) * 86400.0);
			tm.tm_hour = time / 3600;
			time %= 3600;
			tm.tm_min = time / 60;
			tm.tm_sec = time % 60;
			fmt = value > 1.0 ? "%Y-%m-%d %H:%M:%S" : "%H:%M:%S";
		}
		else
			fmt = "%Y-%m-%d";
		std::ostringstream ss;
		ss << std::put_time(&tm, fmt);
		dst = ss.str();
	}
	}, src);
	return 1;
}

static std::string ConvertRawToString(CellRaw& src)
{
	Cell tmp;
	ConvertToString(src, tmp, IngestColumnDefinition());
	return std::move(std::get<std::string>(tmp));
}

bool ParserImpl::open()
{
	is_finished = false;
	return true;
}

IngestColBase* BuildColumn(const IngestColumnDefinition &col, idx_t &cur_row) {
	switch(col.column_type) {
	case ColumnType::String: return new IngestColVARCHAR(col.column_name, cur_row);
	case ColumnType::Boolean: return new IngestColBOOLEAN(col.column_name, cur_row);
	case ColumnType::Integer: return new IngestColBIGINT(col.column_name, cur_row);
	case ColumnType::Decimal: return new IngestColDOUBLE(col.column_name, cur_row);
	case ColumnType::Date: return new IngestColDATE(col.column_name, cur_row, col.format);
	case ColumnType::Time: return new IngestColTIME(col.column_name, cur_row, col.format);
	case ColumnType::Datetime: return new IngestColTIMESTAMP(col.column_name, cur_row, col.format);
	case ColumnType::Bytes:
		if (col.format == "base64") {
			return new IngestColBLOBBase64(col.column_name, cur_row);
		}
		return new IngestColBLOBHex(col.column_name, cur_row);
	case ColumnType::Numeric: return new IngestColNUMERIC(col.column_name, cur_row, col.i_digits, col.f_digits);
	case ColumnType::Geography: return new IngestColGEO(col.column_name, cur_row);
	default:
		D_ASSERT(false);
		return new IngestColBase(col.column_name, cur_row);
	}
}

class IngestColList : public IngestColBase {
public:
	using IngestColBase::Write;

	IngestColList(const IngestColumnDefinition &col, idx_t &cur_row)
	    : IngestColBase(col.column_name, cur_row), buffer(nullptr), child(BuildColumn(col, list_row)) {
	}
	virtual void SetVector(Vector *new_vec) noexcept override {
		IngestColBase::SetVector(new_vec);
		buffer = (VectorListBuffer *)(new_vec->GetAuxiliary().get());
		child->SetVector(&buffer->GetChild());
	}
	virtual LogicalType GetType() const override {
		return LogicalType::LIST(child->GetType());
	};

	bool Write(string_t v) override {
		string s = v.GetString();
		size_t pos, last_pos, new_pos, end_pos;
		if (s.front() == '<' && s.back() == '>')
		{
			pos = 1;
			last_pos = s.size() - 1;
			while (pos < last_pos && StringUtil::CharacterIsSpace(s[pos]))
				++pos;
			while (pos < last_pos && StringUtil::CharacterIsSpace(s[last_pos - 1]))
				--last_pos;
			if (pos == last_pos) // empty list "<   >"
			{
				Writer().SetNull();
				return true;
			}
		}
		else
		{
			pos = 0;
			last_pos = s.size();
		}
		auto &entry = Writer().GetList();
		entry.offset = buffer->GetSize();
		entry.length = 0;
		while (true)
		{
			new_pos = end_pos = s.find(',', pos);
			if (end_pos == std::string::npos)
				end_pos = last_pos;
			while (pos < end_pos && StringUtil::CharacterIsSpace(s[pos]))
				++pos;
			while (pos < end_pos && StringUtil::CharacterIsSpace(s[end_pos - 1]))
				--end_pos;
			string value = s.substr(pos, end_pos - pos);

			list_row = buffer->GetSize();
			buffer->Reserve(list_row + 1);
			buffer->SetSize(list_row + 1);
			++entry.length;

			if (value.empty() || !child->Write(value)) {
				child->WriteNull();
			}
			if (new_pos == std::string::npos)
				break;
			pos = new_pos + 1;
		}
		return true;
	}

private:
	VectorListBuffer *buffer;
	std::unique_ptr<IngestColBase> child;
	idx_t list_row;
};

class IngestColWKTList : public IngestColBase {
public:
	using IngestColBase::Write;

	IngestColWKTList(const IngestColumnDefinition &col, idx_t &cur_row)
	    : IngestColBase(col.column_name, cur_row), buffer(nullptr), child(col.column_name, list_row) {
	}
	virtual void SetVector(Vector *new_vec) noexcept override {
		IngestColBase::SetVector(new_vec);
		buffer = (VectorListBuffer *)(new_vec->GetAuxiliary().get());
		child.SetVector(&buffer->GetChild());
	}
	virtual LogicalType GetType() const override {
		return LogicalType::LIST(child.GetType());
	};

	bool Write(string_t v) override {
		const char* begin = v.GetData();
		const char* end = begin + v.GetSize();
		if (*begin == '<')
		{
			if (*--end != '>')
				return false;
			while (StringUtil::CharacterIsSpace(*++begin));
		}
		std::vector<string> values;
		while (begin < end)
		{
			if (!wkt_to_bytes(begin, end, values.emplace_back()))
				return false;
			while (StringUtil::CharacterIsSpace(*begin) || *begin == ',')
				++begin;
		}
		if (begin != end)
			return false;
		VectorListWriter writer = Writer().SetList();
		for (const auto &s : values) {
			writer.Append().SetString(s);
		}
		return true;
	}

private:
	VectorListBuffer *buffer;
	IngestColGEO child;
	idx_t list_row;
};

class IngestColNestedJSON : public IngestColBase {
public:
	using IngestColBase::Write;

	IngestColNestedJSON(const IngestColumnDefinition &col, idx_t &cur_row, JSONDispatcher &dispatcher)
	    : IngestColBase(col.column_name, cur_row), dispatcher(dispatcher), handler(JSONBuildColumn(col, cur_row)) {
	}

	virtual void SetVector(Vector *new_vec) noexcept override {
		IngestColBase::SetVector(new_vec);
		handler->column.SetVector(new_vec);
	}
	virtual LogicalType GetType() const override {
		return handler->column.GetType();
	};

	bool Write(string_t v) override {
		return dispatcher.parse_string(v.GetString(), handler.get());
	}

private:
	JSONDispatcher &dispatcher;
	std::unique_ptr<JSONValue> handler;
};

class IngestColNestedXML : public IngestColBase {
public:
	using IngestColBase::Write;

	IngestColNestedXML(const IngestColumnDefinition &col, idx_t &cur_row, XMLParseHandler &dispatcher)
	    : IngestColBase(col.column_name, cur_row), dispatcher(dispatcher) {
		handler.assign(XMLBuildColumn(col, cur_row));
	}

	virtual void SetVector(Vector *new_vec) noexcept override {
		IngestColBase::SetVector(new_vec);
		((XMLValueBase *)handler.m_root.get())->column.SetVector(new_vec);
	}
	virtual LogicalType GetType() const override {
		return ((XMLValueBase *)handler.m_root.get())->column.GetType();
	};

	bool Write(string_t v) override {
		return dispatcher.convert_cell(v, &handler);
	}

private:
	XMLParseHandler &dispatcher;
	XMLRoot handler;
};

void ParserImpl::BuildColumns() {
	Schema *schema = get_schema();
	IngestColBase* column;
	for (const auto &col : schema->columns) {
		if (col.is_json) {
			column = new IngestColNestedJSON(col, cur_row, json_dispatcher);
		} else if (col.format == "XML") {
			column = new IngestColNestedXML(col, cur_row, xml_handler);
		}
		else if (col.list_levels) {
			if (col.column_type == ColumnType::Geography) {
				column = new IngestColWKTList(col, cur_row);
			} else {
				column = new IngestColList(col, cur_row);
			}
		} else {
			column = BuildColumn(col, cur_row);
		}
		m_columns.push_back(std::unique_ptr<IngestColBase>(column));
	}
}

void ParserImpl::BindSchema(std::vector<LogicalType> &return_types, std::vector<string> &names) {
	for (auto &col : m_columns) {
		names.push_back(col->GetName());
		return_types.push_back(col->GetType());
	}
}

idx_t ParserImpl::FillChunk(DataChunk &output) {
	size_t n_columns = m_columns.size();
	D_ASSERT(output.data.size() == n_columns);
	for (size_t i = 0; i < n_columns; ++i) {
		m_columns[i]->SetVector(&output.data[i]);
	}

	const Schema& schema = *get_schema();
	RowRaw raw_row;

	for (cur_row = 0; cur_row < STANDARD_VECTOR_SIZE; ++cur_row) {
		int64_t row_number = get_next_row_raw(raw_row);
		if (row_number < 0) {
			close();
			is_finished = true;
			return cur_row;
		}

		for (size_t i_col = 0; i_col < n_columns; ++i_col)
		{
			const IngestColumnDefinition& col = schema.columns[i_col];
			auto &cnv = *m_columns[i_col];
			if (col.index >= 0)
			{
				CellRaw& cell = raw_row[col.index];
				if (col.index >= (int)raw_row.size() || schema.remove_null_strings && cell_null_str(cell))
				{
					cnv.WriteNull();
					continue;
				}
				bool res = std::visit(overloaded{
				[&](const CellRawDate& v) -> bool { return cnv.WriteExcelDate(v.d); },
				[&](auto v) -> bool { return cnv.Write(v); },
				}, cell);
				if (!res) {
					cnv.WriteNull();
				}
			} else {// if (col.index == COL_ROWNUM)
				cnv.Write(row_number);
			}
		}
	}
	return STANDARD_VECTOR_SIZE;
}

int ParserImpl::get_percent_complete()
{
	return 0;
}

size_t ParserImpl::get_sheet_count()
{
	return 0;
}

std::vector<std::string> ParserImpl::get_sheet_names()
{
	return std::vector<std::string>();
}

bool ParserImpl::select_sheet(const std::string_view &sheet_name)
{
	return false;
}

bool ParserImpl::select_sheet(size_t sheet_number)
{
	return false;
}

size_t ParserImpl::get_file_count()
{
	return 0;
}

std::vector<std::string> ParserImpl::get_file_names()
{
	return std::vector<std::string>();
}

bool ParserImpl::select_file(const std::string_view &file_name)
{
	return false;
}

bool ParserImpl::select_file(size_t file_number)
{
	return false;
}

namespace {

template<ColumnType column_type>
class TType
{
public:
	bool create_schema(IngestColumnDefinition& col) const
	{
		if (m_valid)
			col.column_type = column_type;
		return m_valid;
	}

	int infer(const CellRaw& cell) = delete;

	bool m_valid = true;
};

template<>
int TType<ColumnType::Boolean>::infer(const CellRaw& cell)
{
	if (!m_valid)
		return 0;
	return m_valid = std::visit(overloaded{
	[](const std::string& s) -> bool { return bool_dict.find(s) != bool_dict.end(); },
	[](bool v) -> bool { return true; },
	[](int64_t v) -> bool { return v == 0 || v == 1; },
	[](auto v) -> bool { return false; },
	}, cell);
}

static const std::regex _re_check_integer(R"(0|-?[1-9]\d*)");
template<>
int TType<ColumnType::Integer>::infer(const CellRaw& cell)
{
	if (!m_valid)
		return 0;
	return m_valid = std::visit(overloaded{
	[](const std::string& s) -> bool { return std::regex_match(s, _re_check_integer); },
	[](double v) -> bool { return is_integer(v); },
	[](auto v) -> bool { return std::is_integral_v<decltype(v)>; },
	}, cell);
}

static const std::regex _re_check_decimal(R"([+-]?(0|[1-9]\d*|\d+\.|\d*\.\d+)(?:[eE][+-]?\d+)?)");

class TDecimal
{
public:
	int infer(const CellRaw& cell)
	{
		if (!m_valid)
			return 0;
		return m_valid = std::visit(overloaded{
		[this](const std::string& s) -> bool
		{
			std::smatch m;
			if (!std::regex_match(s, m, _re_check_decimal))
				return false;
			if (m.length(1) > 18)
				m_type = ColumnType::Numeric;
			if (m_type == ColumnType::Numeric) {
				if (!string_to_decimal(s.data(), &s[s.size()], buffer))
					return false;
				m_idigits = std::max(m_idigits, (uint8_t)buffer[0]);
				m_fdigits = std::max(m_fdigits, (uint8_t)buffer[1]);
			}
			return true;
		},
		[](auto v) -> bool { return std::is_arithmetic_v<decltype(v)>; },
		}, cell);
	}

	bool create_schema(IngestColumnDefinition& col) const
	{
		if (m_valid) {
			col.column_type = m_type;
			col.i_digits = m_idigits;
			col.f_digits = m_fdigits;
		}
		return m_valid;
	}

	bool m_valid = true;
	ColumnType m_type = ColumnType::Decimal;
	uint8_t m_idigits = 0;
	uint8_t m_fdigits = 0;
	string buffer;
};

static const std::unordered_map<std::string, char> _dt_tokens {
	{"utc", 'Z'}, {"gmt", 'Z'},
	{"am", 'p'}, {"pm", 'p'},
	{"sunday", 'a'}, {"monday", 'a'}, {"tuesday", 'a'}, {"wednesday", 'a'}, {"thursday", 'a'}, {"friday", 'a'}, {"saturday", 'a'},
	{"sun", 'a'}, {"mon", 'a'}, {"tue", 'a'}, {"wed", 'a'}, {"thu", 'a'}, {"fri", 'a'}, {"sat", 'a'},
	{"january", 'b'}, {"february", 'b'}, {"march", 'b'}, {"april", 'b'}, {"may", 'b'}, {"june", 'b'}, {"july", 'b'}, {"august", 'b'}, {"september", 'b'}, {"october", 'b'}, {"november", 'b'}, {"december", 'b'},
	{"jan", 'b'}, {"feb", 'b'}, {"mar", 'b'}, {"apr", 'b'}, {"may", 'b'}, {"jun", 'b'}, {"jul", 'b'}, {"aug", 'b'}, {"sep", 'b'}, {"oct", 'b'}, {"nov", 'b'}, {"dec", 'b'}
};

// time | number | word | non-word
static const std::regex _re_dt_components(R"((\d{1,4}\s*:\s*\d\d(?:\s*:\s*\d\d(?:[.,]\d{1,6})?)?(?!\d))|(\d+)|[a-zA-Z]+|[^\da-zA-Z]+)");
static const std::regex _re_dt_interval(R"((\d{1,4}\s+)?(\d{1,4}\s*:\s*\d\d(?:\s*:\s*\d\d(?:[.,]\d{1,6})?)?))");

// substitute all numbers in time with corresponding format codes
static void _build_time_format(std::string& fmt_s, const std::ssub_match& m)
{
	static const char time_fmt[] = { 'H', 'M', 'S', 'f' };
	size_t i_fmt = 0;
	bool in_digits = false;
	for (auto i = m.first, end = m.second; i < end; ++i) // substitute all numbers in time with corresponding format codes
	{
		bool is_digit = std::isdigit(*i);
		if (!is_digit)
			fmt_s += *i;
		else if (!in_digits)
			fmt_s += {'%', time_fmt[i_fmt++]};
		in_digits = is_digit;
	}
}

class TDateTime
{
public:
	bool infer_dt_format(const std::string& input_string, bool month_first = true)
	{
		double t = 0.0;
		std::string fmt_s; // current format string with "%_" placeholders for d/m/y tokens

		std::smatch m;
		if (std::regex_match(input_string, m, _re_dt_interval))
		{
			_build_time_format(fmt_s, m[2]);
			if (!m[1].matched && strptime(input_string, fmt_s, t))
				m_formats.push_back(fmt_s);
			fmt_s[1] = 'J';
			if (strptime(input_string, fmt_s, t))
				m_formats.push_back(fmt_s);
			m_have_time = true;
			return !m_formats.empty();
		}

		char c;
		std::vector<std::pair<int, int>> dmy_pos_length; // positions of '_' placeholders in fmt_s and lengths of corresponding d/m/y tokens (1- or 2- digits)

		int where_hour = -1; // position of 'H' in fmt_s
		int where_year = -1; // if found definite year token - how many d/m/y placeholders were found before it
		bool have_ampm = false, have_month = false;
		// separate time, numbers, words and delimiters, time is H:MM[:SS[.FFFFFF]]
		for (std::sregex_iterator m(input_string.begin(), input_string.end(), _re_dt_components); m != std::sregex_iterator(); ++m)
		{
			if ((*m)[2].matched) // token is number
			{
				int lng = (int)m->length();
				if ((lng == 4 || lng == 6) && m->position() > 0 && (c = input_string[m->position() - 1], c == '+' || c == '-') && // may be timezone offset -0100
						!(lng == 4 && std::stoi(m->str()) > 1500)) // though it also may be year. Limit offset to 1500 in the hopes that Samoa or Kiribati won't shift further east.
				{
					fmt_s.back() = '%'; // instead of previous '+|-'
					fmt_s += 'z';
				}
				else
				{
					if (lng == 8 && "19000101" <= (*m)[0] && (*m)[0] <= "20991231") // YYYYMMDD
					{
						if (where_year >= 0)
							return false;
						fmt_s += "%Y%m";
						where_year = (int)dmy_pos_length.size();
						have_month = true;
						lng = 2; // let day be handled by regular algorithm
					}
					if (lng == 4) // four digits is year
					{
						if (where_year >= 0)
							return false;
						fmt_s += "%Y";
						where_year = (int)dmy_pos_length.size();
					}
					else if (lng < 3) // found a d/m/y token, split format string here
					{
						fmt_s += "%_";
						dmy_pos_length.emplace_back((int)fmt_s.size() - 1, lng); // length is 1 or 2
					}
					else // wrong number of digits
						return false;
				}
			}
			else if ((*m)[1].matched) // valid time sequence, encode as 't'
			{
				if (where_hour >= 0)
					return false;
				where_hour = (int)fmt_s.size() + 1; // first token to append is '%H'
				_build_time_format(fmt_s, (*m)[0]);
			}
			else
			{
				std::string s = m->str();
				std::transform(s.begin(), s.end(), s.begin(), ::tolower);
				auto new_s = _dt_tokens.find(s); // find a format code of a word
				if (new_s == _dt_tokens.end())
				{
					for (auto i = (*m)[0].first, end = (*m)[0].second; i < end; ++i)
					{
						if (*i == '%') // escape literal %
							fmt_s += '%';
						fmt_s += *i;
					}
				}
				else
				{
					if (new_s->second == 'p')
						have_ampm = true;
					else if (new_s->second == 'b') // || new_s->second == 'B')
						have_month = true;
					fmt_s += {'%', new_s->second};
				}
			}
		}

		if (have_ampm && where_hour >= 0) // use 12-hour format if am/pm found
			fmt_s[where_hour] = 'I';

		size_t n_dmy = dmy_pos_length.size(), n_tokens;
		const char* dmys;
		if (n_dmy > 0) // sequence of applied d/m/y combinations in priority order
		{
			if (have_month) // month in fixed position
				if (where_year < 0)
					dmys = "dy" "yd", n_tokens = 2;
				else
					dmys = "d", n_tokens = 1;
			else
				if (where_year < 0)
					if (month_first)
						dmys = "mdy" "dmy" "ymd" "ydm" "myd" "dym", n_tokens = 3;
					else
						dmys = "dmy" "mdy" "ymd" "ydm" "myd" "dym", n_tokens = 3;
				else if (!month_first && where_year >= 2) // month_first=False only applies when year is last
					dmys = "dm" "md", n_tokens = 2;
				else
					dmys = "md" "dm", n_tokens = 2;
			if (n_tokens != n_dmy) // wrong number of d/m/y tokens
				return false;

			for (const char* dmy = dmys; *dmy; dmy += n_dmy) // try all possible combinations of d/m/y
			{
				for (size_t i = 0; i < n_dmy; ++i) // substitute each token
				{
					if (dmy[i] == 'y' && dmy_pos_length[i].second == 1) // year can only be 2-digit, skip this combination
						goto continue_dmy_loop;
					fmt_s[dmy_pos_length[i].first] = dmy[i]; // put format specifier into its place
				}
				if (strptime(input_string, fmt_s, t))
					m_formats.push_back(fmt_s);
			continue_dmy_loop:;
			}

			m_have_date = true;
			if (where_hour >= 0 && t - int(t) != 0.0)
				m_have_time = true;
		}
		else // only time
		{
			if (where_hour < 0) // neither time nor date found
				return false;
			if (strptime(input_string, fmt_s, t))
				m_formats.push_back(fmt_s);
			m_have_time = true;
		}
		return !m_formats.empty();
	}

	int infer(const CellRaw& cell)
	{
		if (!m_valid)
			return 0;
		return m_valid = std::visit(overloaded{
		[this](const std::string& input_string) -> bool
		{
			if (m_formats.empty())
				return infer_dt_format(input_string);
			for (auto it = m_formats.end(); it > m_formats.begin();)
			{
				--it;
				double t;
				if (!strptime(input_string, *it, t))
					it = m_formats.erase(it);
				else if (t - int(t) != 0.0)
					m_have_time = true;
			}
			return !m_formats.empty();
		},
		[this](const CellRawDate& v) -> bool
		{
			double value = v.d;
			if (!m_have_time && value != std::trunc(value))
				m_have_time = true;
			if (!m_have_date && value >= 417.0) // up to 10000 hours is duration, not a date
				m_have_date = true;
			return true;
		},
		[](auto v) -> bool { return false; },
		}, cell);
	}

	bool create_schema(IngestColumnDefinition& col) const
	{
		if (m_valid)
		{
			if (m_have_date)
				col.column_type = m_have_time ? ColumnType::Datetime : ColumnType::Date;
			else
				col.column_type = ColumnType::Time;
			if (!m_formats.empty())
				col.format = std::move(m_formats[0]);
		}
		return m_valid;
	}

	bool m_valid = true;
	bool m_have_time = false;
	bool m_have_date = false;
	std::vector<std::string> m_formats;
};

class TBytes
{
public:
	int infer(const CellRaw& cell)
	{
		if (!m_valid)
			return 0;
		return m_valid = std::visit(overloaded{
		[this](const std::string& s) -> bool
		{
			if (s.size() < 4 || s.size() % 2 != 0)
				return false;
			bool has_prefix = (s[1] == 'x' || s[1] == 'X') && s[0] == '0';
			if (has_prefix)
				m_have_digits = m_have_letters = true;
			else if (s.size() < 32)
				return false;
			for (size_t i = has_prefix * 2; i < s.size(); ++i)
			{
				char ch = s[i];
				if (ch >= '0' && ch <= '9')
					m_have_digits = true;
				else if (ch >= 'A' && ch <= 'F' || ch >= 'a' && ch <= 'f')
					m_have_letters = true;
				else
					return false;
			}
			return true;
		},
		[](auto v) -> bool { return false; },
		}, cell);
	}

	bool create_schema(IngestColumnDefinition& col) const
	{
		if (m_valid && m_have_digits && m_have_letters)
		{
			col.column_type = ColumnType::Bytes;
			return true;
		}
		return false;
	}

	bool m_valid = true;
	bool m_have_digits = false;
	bool m_have_letters = false;
};

class TBytesBase64
{
public:
	int infer(const CellRaw& cell)
	{
		if (!m_valid)
			return 0;
		return m_valid = std::visit(overloaded{
		[this](const std::string& s) -> bool
		{
			if (s.size() < 16 || s.size() % 4 != 0)
				return false;
			for (size_t i = 0; i < s.size(); ++i)
			{
				char ch = s[i];
				if (!(
					ch >= '0' && ch <= '9' ||
					ch >= 'A' && ch <= 'Z' ||
					ch >= 'a' && ch <= 'z' ||
					ch == '+' || ch == '/'))
					if (ch == '=' && i >= s.size() - 2)
						m_have_padding = true;
					else
						return false;
			}
			return true;
		},
		[](auto v) -> bool { return false; },
		}, cell);
	}

	bool create_schema(IngestColumnDefinition& col) const
	{
		if (m_valid && m_have_padding)
		{
			col.column_type = ColumnType::Bytes;
			col.format = "base64";
			return true;
		}
		return false;
	}

	bool m_valid = true;
	bool m_have_padding = false;
};

class TList
{
public:
	int infer(const CellRaw& cell)
	{
		if (!m_valid)
			return 0;
		const std::string* sp = std::get_if<std::string>(&cell);
		if (!sp)
			return m_valid = false;
		const std::string& s = *sp;

		size_t pos, last_pos, new_pos, end_pos;
		if (s.front() == '<' && s.back() == '>')
		{
			pos = 1;
			last_pos = s.size() - 1;
			while (pos < last_pos && StringUtil::CharacterIsSpace(s[pos]))
				++pos;
			while (pos < last_pos && StringUtil::CharacterIsSpace(s[last_pos - 1]))
				--last_pos;
			if (pos == last_pos) // empty list "<   >"
				return 1;
		}
		else
		{
			pos = 0;
			last_pos = s.size();
		}
		while (true)
		{
			new_pos = end_pos = s.find(',', pos);
			if (end_pos == std::string::npos)
				end_pos = last_pos;
			while (pos < end_pos && StringUtil::CharacterIsSpace((unsigned char)s[pos]))
				++pos;
			while (pos < end_pos && StringUtil::CharacterIsSpace((unsigned char)s[end_pos - 1]))
				--end_pos;
			CellRaw value = s.substr(pos, end_pos - pos);
			if (std::apply([&value](auto&& ... args) { return (args.infer(value) + ...); }, m_types) == 0)
				return m_valid = false;
			if (new_pos == std::string::npos)
				break;
			pos = new_pos + 1;
		}
		return 1;
	}

	bool create_schema(IngestColumnDefinition& col)
	{
		if (m_valid)
		{
			m_valid = std::apply([&col](auto&& ... args) { return (args.create_schema(col) || ...); }, m_types);
			if (m_valid)
				col.list_levels = 1;
		}
		return m_valid;
	}

	bool m_valid = true;
	std::tuple<
		TType<ColumnType::Boolean>,
		TType<ColumnType::Integer>,
		TDecimal,
		TBytes,
		TBytesBase64,
		TDateTime
	> m_types;
};

static const std::regex _re_not_list_item(R"(\.(\W|$))", std::regex::nosubs);

class TStringList
{
public:
	int infer(const CellRaw& cell)
	{
		if (!m_valid)
			return 0;
		const std::string* sp = std::get_if<std::string>(&cell);
		if (!sp)
			return m_valid = false;
		const std::string& s = *sp;
		return m_valid =
			s.front() == '<' && s.back() == '>' ||
			s.find(',') != std::string::npos && !std::regex_search(s, _re_not_list_item);
	}

	bool create_schema(IngestColumnDefinition& col) const
	{
		if (m_valid)
		{
			col.column_type = ColumnType::String;
			col.list_levels = 1;
		}
		return m_valid;
	}

	bool m_valid = true;
};

static const std::regex _re_is_wkt(R"(<?\s*(POINT|LINESTRING|CIRCULARSTRING|COMPOUNDCURVE|CURVEPOLYGON|POLYGON|TRIANGLE|MULTIPOINT|MULTICURVE|MULTILINESTRING|MULTISURFACE|MULTIPOLYGON|POLYHEDRALSURFACE|TIN|GEOMETRYCOLLECTION)(\s+(Z|M|ZM))?\s*(\(|EMPTY)[\sa-zA-Z0-9()+\-.,>]*)", std::regex::nosubs | std::regex::icase);

class TWKT
{
public:
	int infer(const CellRaw& cell)
	{
		if (!m_valid)
			return 0;
		const std::string* sp = std::get_if<std::string>(&cell);
		if (!sp)
			return m_valid = false;
		m_valid = std::regex_match(*sp, _re_is_wkt);
		if (!m_valid || m_is_list)
			return m_valid;
		if (sp->front() == '<')
			m_is_list = true;
		else
		{
			int level = 0;
			for (char c : *sp)
				if (c == '(')
				{
					if (level < 0)
					{
						m_is_list = true;
						break;
					}
					++level;
				}
				else if (c == ')' && --level == 0)
					level = -1;
		}
		return true;
	}

	bool create_schema(IngestColumnDefinition& col) const
	{
		if (m_valid)
		{
			col.column_type = ColumnType::Geography;
			col.format = "WKT";
			col.list_levels = m_is_list;
		}
		return m_valid;
	}

	bool m_valid = true;
	bool m_is_list = false;
};

template <class T>
class infer_children
{
	typedef boost::unordered_map<std::string, T, std::hash<std::string>> map_type;
	typedef std::vector<typename map_type::value_type*> list_type;
public:
	typedef boost::indirect_iterator<typename list_type::iterator> iterator;
	typedef boost::indirect_iterator<typename list_type::const_iterator> const_iterator;
	std::pair<T*, bool> insert(std::string_view key)
	{
		auto it = m_map.find(key, std::hash<std::string_view>(), std::equal_to<>());
		bool is_new = it == m_map.end();
		if (is_new)
		{
			it = m_map.emplace(std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple()).first;
			m_list.push_back(&(*it));
		}
		return {&it->second, is_new};
	}
	const T* find(std::string_view key) const
	{
		auto it = m_map.find(key, std::hash<std::string_view>(), std::equal_to<>());
		return it == m_map.end() ? nullptr : &it->second;
	}
	bool empty() const noexcept { return m_map.empty(); }
	auto size() const noexcept { return m_map.size(); }
	const T& front() const noexcept { return m_list.front()->second; }
	iterator begin() noexcept { return m_list.begin(); }
	iterator end() noexcept { return m_list.end(); }
	const_iterator begin() const noexcept { return m_list.begin(); }
	const_iterator end() const noexcept { return m_list.end(); }

private:
	map_type m_map;
	list_type m_list;
};

class XMLInferValue
{
public:
	XMLInferValue* new_object(const char* name)
	{
		auto [value, res] = m_children.insert(name);
		if (!res)
		{
			if (value->m_saw_tag)
				value->m_is_list = true;
			else
				value->m_saw_tag = true;
			value->reset_values();
		}
		return value;
	}

	void new_value(std::string&& s)
	{
		CellRaw cell = std::move(s);
		std::apply([&cell](auto&& ... args) { (args.infer(cell), ...); }, m_types);
		m_has_single_value = true;
	}

	void reset_values()
	{
		for (auto& p : m_children)
			p.second.m_saw_tag = false;
	}

	bool create_column_schema(IngestColumnDefinition& col) const
	{
		if (m_children.empty())
			return false;
		const XMLInferValue& root = m_children.front();
		if (root.m_children.empty())
			return false;
		root.create_schema(col);
		return true;
	}

	bool create_file_schema(std::vector<IngestColumnDefinition>& fields) const
	{
		if (m_children.empty())
			return false;
		const XMLInferValue& root = m_children.front();
		for (const auto& [_, tag] : root.m_children)
			if (tag.m_is_list && !tag.m_children.empty())
			{
				tag.create_schema(fields);
				return true;
			}
		if (!root.m_children.empty() && !root.m_children.front().m_children.empty())
		{
			root.m_children.front().create_schema(fields);
			return true;
		}
		return false;
	}

private:
	void create_schema(IngestColumnDefinition& col) const
	{
		if (!m_children.empty())
		{
			col.column_type = ColumnType::Struct;
			create_schema(col.fields, (int)col.index);
		}
		else if (m_has_single_value)
			std::apply([&col](auto&& ... args) { return (args.create_schema(col) || ...); }, m_types);
		col.list_levels = m_is_list;
	}

	void create_schema(std::vector<IngestColumnDefinition>& fields, int col_index = 0) const
	{
		for (const auto& [key, value] : m_children)
		{
			fields.push_back({ key, ColumnType::String, col_index, false });
			value.create_schema(fields.back());
		}
		if (m_has_single_value)
		{
			fields.push_back({ "#text", ColumnType::String, col_index, false });
			IngestColumnDefinition& col = fields.back();
			std::apply([&col](auto&& ... args) { return (args.create_schema(col) || ...); }, m_types);
		}
	}

	std::tuple<
		TType<ColumnType::Boolean>,
		TType<ColumnType::Integer>,
		TDecimal,
		TBytes,
		TBytesBase64,
		TDateTime
	> m_types;
	bool m_has_single_value = false;
	bool m_is_list = false;
	bool m_saw_tag = true;
	infer_children<XMLInferValue> m_children;
};

class XMLInferHandler : public XMLHandler<XMLInferHandler>
{
public:
	XMLInferHandler() : m_current(&m_value) {}

	bool start_tag(const char* name, const char** atts)
	{
		m_stack.push_back(m_current);
		m_current = m_current->new_object(name);
		if (atts)
			for (; *atts; atts += 2)
				m_current->new_object(*atts)->new_value(std::string(atts[1]));
		return true;
	}

	void end_tag()
	{
		m_current = m_stack.back();
		m_stack.pop_back();
	}

	bool new_text()
	{
		m_current->new_value(std::move(m_cur_text));
		m_cur_text.clear();
		return true;
	};

	bool parse(const std::string& s)
	{
		m_current = &m_value;
		m_value.reset_values();
		m_stack.clear();
		m_cur_text.clear();
		return parse_string(s);
	}

	const XMLInferValue& get_value() const { return m_value; }

private:
	XMLInferValue m_value;
	XMLInferValue* m_current;
	std::vector<XMLInferValue*> m_stack;
};

static const std::regex _re_is_xml(R"(^<(?:\?xml|(\w+)(?:[^]+</\1|/)>$))");

class TXML
{
public:
	int infer(const CellRaw& cell)
	{
		if (!m_valid)
			return 0;
		const std::string* sp = std::get_if<std::string>(&cell);
		if (!sp || !std::regex_search(*sp, _re_is_xml) || !m_handler.parse(*sp))
			return m_valid = false;
		return true;
	}

	bool create_schema(IngestColumnDefinition& col) const
	{
		if (m_valid)
		{
			if (m_handler.get_value().create_column_schema(col))
				col.format = "XML";
			else
				return false;
		}
		return m_valid;
	}

	bool m_valid = true;
	XMLInferHandler m_handler;
};

class JSONInferValue;

class JSONInferObject : public JSONHandler
{
public:
	virtual ~JSONInferObject() = default;
	virtual bool Null() override { return true; }
	virtual bool Key(const char* s, int length, bool copy, JSONDispatcher* dispatcher) override;
	bool create_schema(IngestColumnDefinition& col) const;
	bool create_geometry(IngestColumnDefinition& col) const;
	void create_schema(std::vector<IngestColumnDefinition>& fields, int col_index = 0) const;

	infer_children<JSONInferValue> m_children;
};

class JSONInferValue : public JSONHandler
{
public:
	virtual ~JSONInferValue() = default;
	virtual bool Null() override { return true; }
	virtual bool Bool(bool b) override { m_value_types |= ValueBool; test_value(b); return true; }
	virtual bool Int(int i) override { m_value_types |= ValueNumber; test_value((int64_t)i); return true; }
	virtual bool Uint(unsigned i) override { m_value_types |= ValueNumber; test_value((int64_t)i); return true; }
	virtual bool Int64(int64_t i) override { m_value_types |= ValueNumber; test_value(i); return true; }
	virtual bool Uint64(uint64_t i) override { m_value_types |= ValueNumber; test_value(INT64_MAX); return true; }
	virtual bool Double(double d) override { m_value_types |= ValueNumber; test_value(d); return true; }
	virtual bool String(const char* s, int length, bool copy) override { m_value_types |= ValueString; test_value(std::string(s, length)); return true; }
	virtual bool StartObject(JSONDispatcher* dispatcher) override
	{
		update_level(ValueObject);
		dispatcher->push(&m_obj);
		return true;
	}
	virtual bool StartArray(JSONDispatcher* dispatcher) override
	{
		if (m_level++ == 0)
		{
			dispatcher->push(this);
			dispatcher->m_value = this;
			m_flags |= ValueArray;
		}
		return true;
	}
	virtual bool EndArray(JSONDispatcher* dispatcher) override
	{
		if (--m_level == 0)
			dispatcher->pop();
		return true;
	}

	template<typename T> void test_value(T&& value)
	{
		CellRaw cell = std::move(value);
		std::apply([&cell](auto&& ... args) { (args.infer(cell), ...); }, m_types);
		update_level(ValueSingle);
	}

	void update_level(unsigned type) {
		if (m_level == 0)
			m_flags |= type;
		else if (m_value_level != m_level) {
			if (m_value_level == 0) {
				m_value_level = m_level;
			} else {
				m_value_level = -1;
			}
		}
	}

	void create_schema(IngestColumnDefinition& col) const
	{
		if ((m_flags & (m_flags - 1)) != 0)
			col.column_type = ColumnType::Variant;
		else if ((m_flags & ValueArray) && (m_value_level <= 0 || !m_obj.m_children.empty() && m_value_types > 0))
		{
			col.column_type = ColumnType::Variant;
			col.list_levels = 1;
		}
		else
		{
			m_obj.create_schema(col) || m_value_types != 0 &&
				std::apply([&col](auto&& ... args) { return (args.create_schema(col) || ...); }, m_types);
			if (m_flags & ValueArray)
				col.list_levels = m_value_level;
		}
	}

	bool create_top_schema(JSONSchema& schema, size_t level = 0) const
	{
		if ((m_flags == ValueArray) && !m_obj.m_children.empty() && m_value_level == 1)
		{
			m_obj.create_schema(schema.columns);
			schema.start_path.resize(level);
			return true;
		}
		for (const auto& [key, value] : m_obj.m_children)
			if (value.create_top_schema(schema, level + 1))
			{
				schema.start_path[level] = key;
				return true;
			}
		return false;
	}

public:
	std::tuple<
		TType<ColumnType::Boolean>,
		TType<ColumnType::Integer>,
		TDecimal,
		TBytes,
		TBytesBase64,
		TDateTime,
		TWKT
	> m_types;
	enum ValueTypes { ValueString = 1, ValueNumber = 2, ValueBool = 4, ValueSingle = 8, ValueObject = 16, ValueArray = 32 };
	unsigned m_value_types = 0;
	unsigned m_flags = 0;
	int m_level = 0;
	int m_value_level = 0;
	JSONInferObject m_obj;
};

bool JSONInferObject::Key(const char* s, int length, bool copy, JSONDispatcher* dispatcher)
{
	JSONInferValue* new_value = m_children.insert(std::string_view(s, length)).first;
	new_value->m_level = 0;
	dispatcher->m_value = new_value;
	return true;
}

bool JSONInferObject::create_schema(IngestColumnDefinition& col) const
{
	if (m_children.empty())
		return false;
	col.column_type = ColumnType::Struct;
	create_schema(col.fields, (int)col.index);
	return true;
}

bool JSONInferObject::create_geometry(IngestColumnDefinition& col) const
{
	if (m_children.size() < 2 || m_children.size() > 3 || !m_children.find("type"))
		return false;
	size_t cnt = 0;
	const JSONInferValue* p = m_children.find("coordinates");
	if (p)
		if (!(p->m_flags & JSONInferValue::ValueArray))
			return false;
		else
			++cnt;
	p = m_children.find("geometries");
	if (p)
		if (!(p->m_flags & JSONInferValue::ValueArray))
			return false;
		else
			++cnt;
	if (m_children.size() - 1 != cnt)
		return false;
	col.column_type = ColumnType::Geography;
	return true;
}

void JSONInferObject::create_schema(std::vector<IngestColumnDefinition>& fields, int col_index) const
{
	for (const auto& [key, value] : m_children)
	{
		fields.push_back({ key, ColumnType::String, col_index, false });
		IngestColumnDefinition& col = fields.back();
		if (key == "geometry" && value.m_obj.create_geometry(col))
		{
			if (value.m_flags & JSONInferValue::ValueArray)
				col.list_levels = 1;
		}
		else
			value.create_schema(col);
	}
}

//#if defined(_MSC_VER) && defined(_WIN64)
static const std::regex _re_is_json(R"(\{\s*\}$|\[\s*\]$|(\{\s*"([^"\\]|\\.)*"\s*:|\[)\s*(["{[0-9.\-]|true|false|null))", std::regex::nosubs);
//#else
//static const std::regex _re_is_json(R"((\[|\{\s*"([^"\\]|\\.)*"\s*:\s*((?=")|[{[0-9.\-]|true|false|null))([,:{}\[\]0-9.\-+Eaeflnr-u \n\r\t]|"([^"\\]|\\.)*")*[}\]]|\{\s*\})", std::regex::nosubs);
//#endif

static const std::regex _re_is_geojson(R"~("type"\s*:\s*"(Point|MultiPoint|LineString|MultiLineString|Polygon|MultiPolygon|GeometryCollection|Feature|FeatureCollection)"|^\{\s*\}$)~", std::regex::nosubs);

class TJSON
{
public:
	int infer(const CellRaw& cell)
	{
		if (!m_valid)
			return 0;
		const std::string* sp = std::get_if<std::string>(&cell);
		if (!sp || !std::regex_search(*sp, _re_is_json, std::regex_constants::match_continuous) ||
			(m_value.m_level = 0, !m_json.parse_string(*sp, &m_value)))
			return m_valid = false;
		if (m_is_geo && !std::regex_search(*sp, _re_is_geojson))
			m_is_geo = false;
		return true;
	}

	bool create_schema(IngestColumnDefinition& col) const
	{
		if (m_valid)
		{
			if (m_is_geo)
				col.format = "GeoJSON";
			else
			{
				m_value.create_schema(col);
				if (col.column_type == ColumnType::Struct)
					col.format = "JSON";
				col.is_json = true;
			}
		}
		return m_valid;
	}

	bool m_valid = true;
	bool m_is_geo = true;
	JSONDispatcher m_json;
	JSONInferValue m_value;
};

}

class Column
{
public:
	bool infer(const CellRaw& cell)
	{
		if (!cell_empty(cell))
		{
			empty = false;
			return std::apply([&cell](auto&& ... args) { return (args.infer(cell) + ...); }, m_types) > 0;
		}
		return true;
	}
	void create_schema(IngestColumnDefinition& col)
	{
		if (!empty) {
			std::apply([&col](auto&& ... args) { (args.create_schema(col) || ...); }, m_types);
			col.bytes_per_value = bytes_per_value;
		}
	}
	bool is_typed() const
	{
		return std::apply([](auto&& ... args) { return (args.m_valid || ...); }, m_types);
	}
	std::string name;
	bool empty = true;
	double bytes_per_value;
private:
	std::tuple<
		TType<ColumnType::Boolean>,
		TType<ColumnType::Integer>,
		TDecimal,
		TBytes,
		TBytesBase64,
		TDateTime,
		TXML,
		TJSON,
		TWKT,
		TList,
		TStringList
	> m_types;
};

void ParserImpl::build_column_info(std::vector<Column>& columns)
{
	Schema& schema = *get_schema();
	std::unordered_set<std::string> have_columns; // columns we already have
	std::string prefix, new_name;
	for (size_t i_col = 0; i_col < columns.size(); ++i_col)
	{
		const std::string* col_name = &columns[i_col].name;
		size_t no_start = 0;
		if (col_name->empty())
			prefix = "Field_", no_start = i_col + 1;
		else if (!have_columns.insert(StringUtil::Lower(*col_name)).second)
			prefix = *col_name + '_', no_start = 2;
		if (no_start > 0)
			for (size_t no = no_start; ; ++no)
			{
				new_name = prefix + std::to_string(no);
				if (have_columns.insert(StringUtil::Lower(new_name)).second)
				{
					col_name = &new_name;
					break;
				}
			}
		schema.columns.push_back({ *col_name, ColumnType::String, (int)i_col, false });
		columns[i_col].create_schema(schema.columns.back());
	}
}

void ParserImpl::infer_table(const std::string* comment)
{
	std::vector<RowRawNumbered> rows(INFER_MAX_ROWS);
	for (size_t i_row = 0; i_row < rows.size(); ++i_row) {
		if ((rows[i_row].row_no = get_next_row_raw(rows[i_row])) < 0)
		{
			rows.resize(i_row);
			break;
		}
	}
	if (!rows.empty())
		do_infer_table(comment, rows);
}

void ParserImpl::do_infer_table(const std::string* comment, std::vector<RowRawNumbered>& rows) {
	for (size_t i_row = 0; i_row < rows.size(); ++i_row) {
		for (CellRaw& cell : rows[i_row]) {
			if (get_schema()->remove_null_strings && cell_null_str(cell))
				std::get<std::string>(cell).clear();
			else if (std::holds_alternative<std::string>(cell)) {
				trim(std::get<std::string>(cell));
			}
		}
	}

	size_t comment_lines_skipped_in_parsing = 0;
	// remove trailing columns which are empty across all lines
	size_t n_columns = 0;
	for (const auto& row : rows)
		for (size_t i_col = row.size(); i_col > 0; --i_col)
			if (!cell_empty(row[i_col - 1]))
			{
				if (i_col > n_columns)
					n_columns = i_col;
				break;
			}
	std::unordered_map<size_t, size_t> cnt_columns;
	for (auto& row : rows)
	{
		if (row.size() > n_columns)
			row.resize(n_columns);
		++cnt_columns[row.size()];
	}
	// most common number of columns
	n_columns = std::max_element(cnt_columns.begin(), cnt_columns.end(),
		[](auto p1, auto p2) { return p1.second < p2.second || (p1.second == p2.second && p1.first < p2.first); }
		)->first;
	if (n_columns == 0)
		return;

	std::vector<Column> columns(n_columns);

	size_t header_row = 0, data_row = 0;
	bool found_header;
	if (rows.size() == 1)
	{
		found_header = true;
	}
	else
	{
		int header_score = 0;
		for (size_t i_row = 0; i_row < rows.size(); ++i_row)
		{
			const auto& row = rows[i_row];
			int sep_value = row.size() == n_columns;
			int nonblanks = std::none_of(row.begin(), row.end(), cell_empty);
			int row_offset = INFER_MAX_ROWS - (int)i_row;
			int num_uniques = (int)std::unordered_set<CellRaw>(row.begin(), row.end()).size();
			int num_strings = (int)std::count_if(row.begin(), row.end(),
				[](const CellRaw& cell)
				{
					const std::string* sp = std::get_if<std::string>(&cell);
					return sp && !sp->empty() && std::isalpha((unsigned char)(*sp)[0]);
				});
			int score = sep_value * nonblanks + (row_offset + (num_strings * 2) + (num_uniques * 2));
			if (score > header_score)
				header_score = score, header_row = i_row;
		}
		auto& header = rows[header_row];
		header.resize(n_columns);
		if (comment != nullptr)
		{
			std::string* sp = std::get_if<std::string>(&header[0]);
			if (sp && startswith(*comment)(*sp)) // found a comment
				sp->erase(0, comment->size()); // strip comment_char
		}

		if (header_row > 0)
			rows[0] = std::move(header);
		size_t iw = 1;
		data_row = 0;
		for (size_t i_row = header_row + 1; i_row < rows.size(); ++i_row)
		{
			auto& row = rows[i_row];
			if (!std::all_of(row.begin(), row.end(), cell_empty)) // exclude empty lines
			{
				if (comment != nullptr)
				{
					const std::string* sp = std::get_if<std::string>(&row[0]);
					if (sp && startswith(*comment)(*sp)) // found a comment)
					{
						++comment_lines_skipped_in_parsing;
						continue;
					}
				}
				if (row.size() == n_columns)
				{
					if (data_row == 0)
						data_row = i_row;
					if (i_row != iw)
						rows[iw] = std::move(row);
					++iw;
				}
			}
		}
		rows.resize(iw);

		for (size_t i_col = 0; i_col < n_columns; ++i_col)
			for (size_t i_row = 1; i_row < rows.size(); ++i_row) // ignore potential header for now
				if (!columns[i_col].infer(rows[i_row][i_col])) // no consistent type
					break;

		found_header = false;
		std::vector<Column> columns_header = columns;
		for (size_t i_col = 0; i_col < n_columns; ++i_col) // check if types of first row inconsistent with the rest
		{
			if (!columns[i_col].is_typed())
				continue;
			const CellRaw& v = rows[0][i_col];
			if (cell_empty(v))
				continue;
			// include header into type detection
			if (!columns_header[i_col].infer(v) && !columns[i_col].empty) // do not detect header for empty columns
			{
				found_header = true;
				break;
			}
		}

		if (!found_header && std::all_of(columns_header.begin(), columns_header.end(), [](const Column& col) { return col.empty || !col.is_typed(); }))
			found_header = true; // assume first row is header
		if (!found_header)
			columns = std::move(columns_header); // no header, include refined types from first row

		for (size_t i_row = 1; i_row < rows.size(); ++i_row)
			for (size_t i_col = 0; i_col < n_columns; ++i_col)
				if(const std::string* sp = std::get_if<std::string>(&rows[i_row][i_col]))
					columns[i_col].bytes_per_value += sp->size();
		for (size_t i_col = 0; i_col < n_columns; ++i_col)
			columns[i_col].bytes_per_value /= rows.size()-1;
	}
	header_row = rows[0].row_no;
	if (found_header) {
		data_row = data_row == 0 ? header_row + 1 : rows[1].row_no;
		for (size_t i = 0; i < columns.size(); ++i)
			columns[i].name = ConvertRawToString(rows[0][i]);
	} else {
		data_row = header_row;
		header_row = -1;
	}

	build_column_info(columns);
	if (CSVSchema* schema = dynamic_cast<CSVSchema*>(get_schema()))
	{
		schema->header_row = header_row;
		schema->first_data_row = data_row;
		schema->comment_lines_skipped_in_parsing = comment_lines_skipped_in_parsing;
	}
	else if (XLSSchema* schema = dynamic_cast<XLSSchema*>(get_schema()))
	{
		schema->header_row = header_row;
		schema->first_data_row = data_row;
		schema->comment_lines_skipped_in_parsing = comment_lines_skipped_in_parsing;
	}
}

bool JSONParser::do_infer_schema()
{
	const size_t SAMPLE_SIZE = 1024 * 1024 * 5;

	m_schema.columns.clear();
	if (!m_reader->is_file() || !m_reader->open())
		return false;
	std::string sample(SAMPLE_SIZE, '\0');
	sample.resize(m_reader->read(&sample[0], SAMPLE_SIZE));
	close();

	JSONDispatcher json;
	JSONInferValue value;
	json.parse_string(sample, &value);
	return value.create_top_schema(m_schema);
}

bool XMLParser::do_infer_schema()
{
	const size_t SAMPLE_SIZE = 1024 * 1024 * 5;

	m_schema.columns.clear();
	if (!open())
		return false;
	std::string sample(SAMPLE_SIZE, '\0');
	sample.resize(m_reader->read(&sample[0], SAMPLE_SIZE));
	close();

	XMLInferHandler handler;
	handler.parse(sample);
	return handler.get_value().create_file_schema(get_schema()->columns);
}

bool ParserImpl::infer_schema()
{
	Schema& schema = *get_schema();
	if (!do_infer_schema())
	{
		schema.status = STATUS_INVALID_FILE;
		return false;
	}
	schema.status = STATUS_OK;
	schema.columns.push_back({ "__rownum__", ColumnType::Integer, COL_ROWNUM, false });

	struct BuildColIndices
	{
		int dest_index;
		void traverse(IngestColumnDefinition& parent)
		{
			for (IngestColumnDefinition& col : parent.fields)
			{
				col.dest_index = dest_index++;
				if (col.column_type == ColumnType::Struct)
					traverse(col);
			}
		}
	} col_indices {(int)schema.columns.size()};

	for (size_t i_col = 0; i_col < schema.columns.size(); ++i_col)
	{
		IngestColumnDefinition& col = schema.columns[i_col];
		if (col.column_type == ColumnType::Struct)
		{
			col.dest_index = i_col;
			col_indices.traverse(col);
		}
	}
	return true;
}

}
