#include "datadocs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/main/extension_util.hpp"
#include "inferrer.h"
#include "type_conv.h"

#include <regex>


// former contents of type_conv.cpp are dumped here
// this file used VariantCell procedures which makes no sense, this is the old variant type used in Perspective
#include <string>

#include <stdint.h>
#include <cctype>
#include <limits>
#include <vector>
#include <string>
#include <unordered_map>
#include <charconv>
#include <regex>

#include "duckdb/common/string_util.hpp"

#include "inferrer.h"
#include "wkt.h"
#include "type_conv.h"

namespace duckdb {

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

static constexpr unsigned pow10i[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000};

static const std::regex re_variant_integer(R"(0|-?[1-9]\d*)");
static const std::regex re_variant_float(R"(-?(0|[1-9]\d*|\d+\.|\d*\.\d+)(?:[eE][+-]?\d+)?)");

static bool string_to_variant_number(const char* begin, const char* end, VariantCell& cell)
{
	if (std::regex_match(begin, end, re_variant_integer))
	{
		if (*begin == '-')
		{
			int64_t res;
			if (std::from_chars(begin, end, res, 10).ec == std::errc())
			{
				if (res < std::numeric_limits<int>::min())
					cell.assign<VariantCell::Integer64>(res);
				else
					cell.assign<VariantCell::Integer>((int)res);
				return true;
			}
		}
		else
		{
			uint64_t res;
			if (std::from_chars(begin, end, res, 10).ec == std::errc())
			{
				if (res > std::numeric_limits<unsigned>::max())
					cell.assign<VariantCell::Unsigned64>(res);
				else
					cell.assign<VariantCell::Unsigned>((unsigned)res);
				return true;
			}
		}
	}
	else
	{
		std::cmatch m;
		if (!std::regex_match(begin, end, m, re_variant_float))
			return false;
		if (m.length(1) <= 18)
		{
			char* str_end;
			double res = std::strtod(begin, &str_end);
			if (str_end != end)
				return false;
			cell.assign<VariantCell::Float>(res);
			return true;
		}
	}
	if (!string_to_decimal(begin, end, cell.data))
		return false;
	cell.type = VariantCell::Numeric;
	return true;
}

static const std::regex re_variant_dt_components(R"((\d{1,4})\s*:\s*(\d\d)(?:\s*:\s*(\d\d)(?:[.,](\d{1,6}))?)?(?!\d)|(\d+)|([a-zA-Z]+))");

static const std::unordered_map<std::string, int> variant_dt_tokens {
	{"utc", 0}, {"gmt", 0}, {"t", 0}, {"z", 0},
	{"am", 'a'}, {"pm", 'p'},
	{"sunday", 0}, {"monday", 0}, {"tuesday", 0}, {"wednesday", 0}, {"thursday", 0}, {"friday", 0}, {"saturday", 0},
	{"sun", 0}, {"mon", 0}, {"tue", 0}, {"wed", 0}, {"thu", 0}, {"fri", 0}, {"sat", 0},
	{"january", 1}, {"february", 2}, {"march", 3}, {"april", 4}, {"may", 5}, {"june", 6}, {"july", 7}, {"august", 8}, {"september", 9}, {"october", 10}, {"november", 11}, {"december", 12},
	{"jan", 1}, {"feb", 2}, {"mar", 3}, {"apr", 4}, {"may", 5}, {"jun", 6}, {"jul", 7}, {"aug", 8}, {"sep", 9}, {"oct", 10}, {"nov", 11}, {"dec", 12}
};

static bool string_to_variant_date(const char* begin, const char* end, VariantCell& cell, bool month_first = true)
{
	char c;
	int yy = -1, mm = -1, HH = -1, MM = 0, SS = 0;
	int ampm = -1, tz_offset = 0;
	int where_year = -1; // if found definite year token - how many d/m/y were found before it
	double dt, FF = 0;
	VariantCell::VariantTypeId cell_type;
	std::vector<int> dmy; // d/m/y tokens (1- or 2- digits)
	std::vector<bool> dmy1; // true if token is 1-digit (cannot be year)

	// separate time, numbers, words, ignore delimiters, time is H:MM[:SS[.FFFFFF]]
	for (std::cregex_iterator m(begin, end, re_variant_dt_components); m != std::cregex_iterator(); ++m)
	{
		if ((*m)[5].matched) // token is number
		{
			int lng = (int)m->length();
			if ((lng == 4 || lng == 6) && m->position() > 0 && (c = begin[m->position() - 1], c == '+' || c == '-') && // may be timezone offset -0100
				!(lng == 4 && m->str() > "1500")) // though it also may be year. Limit offset to 1500 in the hopes that Samoa or Kiribati won't shift further east.
			{
				int tz_h = 0;
				const char* s = begin + m->position();
				std::from_chars(s, s+2, tz_h, 10);
				tz_offset = 0;
				std::from_chars(s+2, s+4, tz_offset, 10);
				tz_offset += tz_h * 60;
				if (s[-1] == '+')
					tz_offset = -tz_offset;
			}
			else
			{
				if (lng == 4) // four digits is year
				{
					if (yy >= 0)
						return false;
					yy = std::stoi(m->str());
					where_year = dmy.size();
				}
				else if (lng < 3) // found a d/m/y token
				{
					dmy.push_back(std::stoi(m->str())); // length is 1 or 2
					dmy1.push_back(m->length() == 1);
				}
				else // wrong number of digits
					return false;
			}
		}
		else if ((*m)[1].matched) // valid time sequence
		{
			if (HH >= 0)
				return false;
			HH = std::stoi(m->str(1));
			MM = std::stoi(m->str(2));
			if ((*m)[3].matched)
			{
				SS = std::stoi(m->str(3));
				if ((*m)[4].matched)
				{
					FF = std::stoi(m->str(4));
					FF /= pow10i[m->length(4)];
				}
			}
		}
		else // a word
		{
			std::string s = m->str();
			std::transform(s.begin(), s.end(), s.begin(), ::tolower);
			auto token = variant_dt_tokens.find(s);
			if (token == variant_dt_tokens.end())
				return false;
			if (token->second == 'a') // am
				ampm = 0;
			else if (token->second == 'p') // pm
				ampm = 1;
			else if (token->second > 0) // month
				mm = token->second;
		}
	}

	const char* dmyi = nullptr; // possible indices in dmy for day, month, year in priority order
	if (dmy.size() > 0)
	{
		if (mm > 0) // month in fixed position
		{
			if (where_year < 0)
			{
				dmy.push_back(mm);
				dmyi = "021" "120"; // "dy", "yd"
			}
			else
			{
				dmy.push_back(mm);
				dmy.push_back(yy);
				dmyi = "012"; // "d"
			}
		}
		else
		{
			if (where_year < 0)
			{
				if (month_first)
					dmyi = "102" "012" "210" "120" "201" "021"; // "mdy", "dmy", "ymd", "ydm", "myd", "dym"
				else
					dmyi = "012" "102" "210" "120" "201" "021"; // "dmy", "mdy", "ymd", "ydm", "myd", "dym"
			}
			else if (!month_first && where_year >= 2) // month_first=false only applies when year is last
			{
				dmy.push_back(yy);
				dmyi = "012" "102"; // "dm", "md"
			}
			else
			{
				dmy.push_back(yy);
				dmyi = "102" "012"; // "md", "dm"
			}
		}
		if (dmy.size() != 3) // wrong number of d/m/y tokens
			return false;

		int date = -1;
		while (*dmyi) // try all possible combinations of d/m/y
		{
			size_t id = *dmyi++ - '0';
			size_t im = *dmyi++ - '0';
			size_t iy = *dmyi++ - '0';
			if (iy < dmy1.size() && dmy1[iy]) // false or undefined can be years
				continue;
			int day = dmy[id], month = dmy[im], y = dmy[iy];
			if (month == 0 || month > 12 || day == 0)
				continue;
			int max_day;
			if (month == 4 || month == 6 || month == 9 || month == 11)
				max_day = 30;
			else if (month == 2)
				max_day = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) ? 29 : 28;
			else
				max_day = 31;
			if (day > max_day)
				continue;
			if (y < 100)
				y += y < 68 ? 2000 : 1900;
			y -= month <= 2;
			int era = (y >= 0 ? y : y - 399) / 400;
			unsigned yoe = (unsigned)(y - era * 400); // [0, 399]
			unsigned doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1; // [0, 365]
			unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
			date = era * 146097 + (int)doe - 719468 + 25569;
			break;
		}
		if (date < 0)
			return false;
		if (HH < 0)
		{
			cell.assign<VariantCell::Date>(date);
			return true;
		}
		cell_type = VariantCell::Datetime;
		dt = date;
	}
	else
	{
		if (HH < 0)
			return false;
		dt = 0;
		cell_type = VariantCell::Time;
	}

	if (HH < 13)
	{
		if (ampm == 0)
		{
			if (HH == 12) HH = 0;
		}
		else if (ampm == 1)
		{
			if (HH < 12) HH += 12;
		}
	}
	dt += (HH * 3600 + (MM + tz_offset) * 60 + SS + FF) / 86400.0;
	if (tz_offset != 0 && cell_type == VariantCell::Time)
	{
		dt = dt - int(dt);
		if (dt < 0)
			dt += 1.0;
	}
	cell.assign<VariantCell::Float>(dt);
	cell.type = cell_type;
	return true;
}

static const std::unordered_map<std::string, bool> variant_bool_dict {
	{"false", false}, {"False", false}, {"FALSE", false}, {"true", true}, {"True", true}, {"TRUE", true}
};

static bool string_to_variant_inner(const char* begin, const char* end, VariantCell& cell)
{
	while (StringUtil::CharacterIsSpace(*begin))
		if (++begin >= end)
			return false;
	while (StringUtil::CharacterIsSpace(end[-1])) --end;
	if (string_to_variant_number(begin, end, cell))
		return true;
	size_t length = end - begin;
	if (length == 4 || length == 5)
	{
		auto it = variant_bool_dict.find(std::string(begin, end));
		if (it != variant_bool_dict.end())
		{
			cell.assign<VariantCell::Boolean>(it->second);
			return true;
		}
	}
	if (string_to_variant_date(begin, end, cell))
		return true;
	if (length % 2 == 0 && begin[0] == '0' && begin[1] == 'x')
	{
		length = (length - 2) / 2;
		cell.type = VariantCell::Bytes;
		cell.data.resize(length);
		return string0x_to_bytes(begin + 2, end, cell.data.data());
	}
	cell.data.clear();
	if (wkt_to_bytes(begin, end, cell.data) && begin == end)
	{
		cell.type = VariantCell::Geography;
		return true;
	}
	return false;
}

static void string_to_variant(const char* src, int src_length, VariantCell& cell)
{
	if (!string_to_variant_inner(src, src + src_length, cell))
	{
		cell.type = VariantCell::String;
		cell.data.assign(src, src_length);
	}
}

}
// contents of type_conv.cpp end here



namespace duckdb {
static const std::string digit = "[0-9]";
static const std::string space = "\\s";
static const std::string sign = "[+-]";
static const std::string comma = "[,]";
static const std::string floating_point = "[.]";
static const std::string E = "[eE]";
static const std::string percentage_symbol = "[%]";
static const std::string dollar_sign = "[$]";
static const std::string euro_sign = "€";
static const std::string percentage_currency_symbol =
    StringUtil::Format("(%s|%s|%s)", percentage_symbol, dollar_sign, euro_sign);
static const std::string exponent = StringUtil::Format("(%s[+-]?%s+)", E, digit);
static const std::string sign_component = StringUtil::Format("(%s(%s)*)", sign, space);
static const std::string comma_integer = StringUtil::Format("(%s%s{3}%s*)", comma, digit, digit);
static const std::string group_integer = StringUtil::Format("([,]%s{3}%s*)", digit, digit);
static const std::string integer_group_regex_str =
    StringUtil::Format("(%s?((%s+)?%s+))", sign_component, digit, group_integer);
static const std::string integer_regex_str = StringUtil::Format("(%s?%s+)", sign_component, digit);
static const std::string integer_comma_regex_str =
    StringUtil::Format("(%s?((%s+)?%s+))", sign_component, digit, comma_integer);
static const std::string decimal_regex_str = StringUtil::Format("(%s?((%s+%s%s*)|(%s%s+)))", sign_component, digit,
                                                                floating_point, digit, floating_point, digit);
static const std::string decimal_group_regex_str =
    StringUtil::Format("(%s%s%s*)", integer_group_regex_str, floating_point, digit);
static const std::string float_regex_str =
    StringUtil::Format("(%s?((%s+%s%s*)|(%s?%s+))(%s|%s))", sign_component, digit, floating_point, digit,
                       floating_point, digit, exponent, E);
static const std::string float_group_str =
    StringUtil::Format("(%s(%s%s*)?(%s|%s))", integer_group_regex_str, floating_point, digit, exponent, E);
static const std::string percentage_currency_regex_str =
    StringUtil::Format("(%s(.*))|((.*)%s)", percentage_currency_symbol, percentage_currency_symbol);

static const std::regex re_integer(integer_regex_str);
static const std::regex re_integer_comma(integer_comma_regex_str);
bool infer_int(string input, VariantCell &cell) {
	if (input.size() == 0) {
		return false;
	}
	bool success = false;
	auto begin = input.c_str(), end = input.c_str() + input.size();
	std::string str = input;
	if (std::regex_match(begin, end, re_integer)) {
		str.erase(std::remove(str.begin(), str.end(), ' '), str.end());
		string_to_variant(str.c_str(), str.size(), cell);
		success = true;
	} else if (std::regex_match(begin, end, re_integer_comma)) {
		str.erase(std::remove(str.begin(), str.end(), ' '), str.end());
		str.erase(std::remove(str.begin(), str.end(), ','), str.end());
		string_to_variant(str.c_str(), str.size(), cell);
		success = true;
	}
	return success;
}

bool variant_cell_update_double(std::string input, VariantCell &cell) {
	double res = 0.0;
	switch (cell.type) {
	case VariantCell::Integer: {
		res = *((int *)cell.data.c_str());
	} break;

	case VariantCell::Unsigned: {
		res = *((unsigned int *)cell.data.c_str());
	} break;

	case VariantCell::Integer64: {
		res = *((int64_t *)cell.data.c_str());
	} break;

	case VariantCell::Unsigned64: {
		res = *((uint64_t *)cell.data.c_str());
	} break;

	case VariantCell::Float: {
		res = *((double *)cell.data.c_str());
	} break;

	case VariantCell::Numeric: {
		auto str = input;
		str.erase(std::remove(str.begin(), str.end(), ' '), str.end());
		str.erase(std::remove(str.begin(), str.end(), ','), str.end());
		auto i_digits = (int)cell.data[0];
		auto f_digits = (int)cell.data[1];
		Value val(str);
		auto doub_val = val.DefaultCastAs(LogicalType::DOUBLE);
		auto dec_val = val.DefaultCastAs(LogicalType::DECIMAL(i_digits + f_digits, f_digits));
		res = doub_val.GetValue<double>();
	} break;

	default:
		// Do nothing
		return false;
	}
	cell.assign<VariantCell::Float>(res);
	return true;
}

static const std::regex re_decimal(decimal_regex_str);
static const std::regex re_decimal_comma(decimal_group_regex_str);
bool infer_decimal(string input, VariantCell &cell) {
	if (input.size() == 0) {
		return false;
	}
	bool success = false;
	auto begin = input.c_str(), end = input.c_str() + input.size();
	std::string str = input;
	if (std::regex_match(begin, end, re_decimal)) {
		str.erase(std::remove(str.begin(), str.end(), ' '), str.end());
		string_to_variant(str.c_str(), str.size(), cell);
		success = true;
	} else if (std::regex_match(begin, end, re_decimal_comma)) {
		str.erase(std::remove(str.begin(), str.end(), ' '), str.end());
		str.erase(std::remove(str.begin(), str.end(), ','), str.end());
		string_to_variant(str.c_str(), str.size(), cell);
		success = true;
	}
	if (success && cell.type == VariantCell::Numeric) {
		Value val(str);
		auto doub_val = val.DefaultCastAs(LogicalType::DOUBLE);
		cell.assign<VariantCell::Float>(doub_val.GetValue<double>());
	}
	return success;
}

static const std::regex re_float(StringUtil::Format("(%s|%s|%s)", integer_regex_str, float_regex_str,
                                                    decimal_regex_str));
static const std::regex re_float_comma(StringUtil::Format("(%s|%s|%s)", float_group_str, integer_group_regex_str,
                                                          decimal_group_regex_str));
bool infer_float(string input, VariantCell &cell) {
	if (input.size() == 0) {
		return false;
	}
	bool success = false;
	auto begin = input.c_str(), end = input.c_str() + input.size();
	std::string str = input;
	if (std::regex_match(begin, end, re_float)) {
		str.erase(std::remove(str.begin(), str.end(), ' '), str.end());
		string_to_variant(str.c_str(), str.size(), cell);
		success = true;
	} else if (std::regex_match(begin, end, re_float_comma)) {
		str.erase(std::remove(str.begin(), str.end(), ' '), str.end());
		str.erase(std::remove(str.begin(), str.end(), ','), str.end());
		string_to_variant(str.c_str(), str.size(), cell);
		success = true;
	}
	if (success && cell.type == VariantCell::Numeric) {
		cell.assign<VariantCell::Float>(std::stod(str));
	}
	return success;
}

bool infer_number(string input, VariantCell &cell) {
	if (input.size() == 0) {
		return false;
	}

	bool success = true;
	success = infer_int(input, cell);
	if (success)
		return success;
	success = infer_decimal(input, cell);
	if (success) {
		return success;
	}
	success = infer_float(input, cell);
	return success;
}

static const std::regex re_currency_percentage(percentage_currency_regex_str);
bool infer_currency_or_percentage_number(string input, VariantCell &cell) {
	auto begin = input.c_str(), end = input.c_str() + input.size();
	bool success = false;
	std::cmatch m;
	if (!std::regex_match(begin, end, m, re_currency_percentage)) {
		return success;
	}
	if (m.size() != 7) {
		return success;
	}
	auto symbol = m.str(2) != "" ? m.str(2) : m.str(6);
	auto num = m.str(3) != "" ? m.str(3) : m.str(5);
	if (!symbol.empty()) {
		success = infer_number(num, cell);
		if (success) {
			success = variant_cell_update_double(input, cell);
			if (success) {
				if (symbol == "%") {
					cell.assign<VariantCell::Float>(*((double *)cell.data.c_str()) / 100);
				}
			}
		}
	}
	return success;
}

static inline double ParseNum(const string_t &input) {
	VariantCell cell;
	bool success = infer_currency_or_percentage_number(input.GetString(), cell);
	if (!success) {
		success = infer_number(input.GetString(), cell);
	}
	if (success) {
		success = variant_cell_update_double(input.GetString(), cell);
		if (success && cell.type == VariantCell::Float) {
			return *((double *)cell.data.c_str());
		} else {
			throw ConversionException(StringUtil::Format("Expected NUMBER, but got %s", input.GetString()));
			return 0.0;
		}
	} else {
		throw ConversionException(StringUtil::Format("Expected NUMBER, but got %s", input.GetString()));
		return 0.0;
	}
}

static void ParseNumFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[0].GetType() == LogicalType::VARCHAR);
	D_ASSERT(args.data.size() == 1);
	UnaryExecutor::Execute<string_t, double>(args.data[0], result, args.size(), ParseNum);
}

void DatadocsExtension::LoadParseNum(DatabaseInstance &inst) {
	ExtensionUtil::RegisterFunction(
	    inst, ScalarFunction("parse_num", {LogicalType::VARCHAR}, LogicalType::DOUBLE, ParseNumFunc));
}

} // namespace duckdb
