#include "datadocs_extension.hpp"
#include "duckdb.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#endif
#include "inferrer.h"
#include "type_conv.h"

#include <regex>

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

void DatadocsExtension::LoadParseNum(Connection &con) {
	auto &context = *con.context;
	auto &catalog = Catalog::GetSystemCatalog(context);

	CreateScalarFunctionInfo parse_num_info(
	    ScalarFunction("parse_num", {LogicalType::VARCHAR}, LogicalType::DOUBLE, ParseNumFunc));
	catalog.CreateFunction(context, parse_num_info);
}

} // namespace duckdb
