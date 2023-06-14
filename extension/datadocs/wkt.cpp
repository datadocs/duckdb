#include <stddef.h>
#include <stdint.h>
#include <string>

#define BOOST_NO_CXX98_FUNCTION_BASE

#include <boost/predef/other/endian.h>
#ifdef BOOST_NO_EXCEPTIONS
#include <boost/spirit/home/x3/support/traits/is_variant.hpp>
#include <boost/spirit/home/x3/support/traits/tuple_traits.hpp>
#include "boost_exc/spirit/home/x3/auxiliary/guard.hpp"
#include "boost_exc/spirit/home/x3/nonterminal/detail/rule.hpp"
#endif
#include <boost/spirit/home/x3.hpp>

#include "wkt.h"

namespace duckdb {

namespace x3 = boost::spirit::x3;
namespace ascii = boost::spirit::x3::ascii;

namespace {

class WKBBuilder
{
public:
	explicit WKBBuilder(std::string& data) noexcept : m_data(data) {}

	void add_header(uint32_t type)
	{
		m_data.push_back(endianness);
		m_data.append((char*)(&type), 4);
	}

	size_t add_counter()
	{
		size_t old_size = m_data.size();
		m_data.append(4, '\0');
		return old_size;
	}

	void add_double(double d)
	{
		m_data.append((char*)(&d), 8);
	}

	void increment(size_t m_index)
	{
		++*(uint32_t*)&m_data[m_index];
	}

private:
#if BOOST_ENDIAN_BIG_BYTE
	static constexpr uint8_t endianness = 0;
#else
	static constexpr uint8_t endianness = 1;
#endif
	std::string& m_data;
};

#define WKT_OBJECT(NAME) constexpr x3::rule<struct NAME, size_t> NAME(""); constexpr auto NAME##_def

namespace wkt_parser
{
	using x3::eps, x3::no_case, x3::double_, x3::_val, x3::_attr;
	struct builder {};

	constexpr auto add_double = [](auto& ctx) { x3::get<builder>(ctx).add_double(_attr(ctx)); };
	constexpr auto add_cnt = [](auto& ctx) { _val(ctx) = x3::get<builder>(ctx).add_counter(); };
	constexpr auto inc_cnt = [](auto& ctx) { x3::get<builder>(ctx).increment(_val(ctx)); };

	constexpr auto add_object(uint32_t type)
	{
		auto action = [type](auto& ctx) { x3::get<builder>(ctx).add_header(type); };
		return eps[action];
	};

	constexpr auto add_multi(uint32_t type)
	{
		return [type](auto& ctx)
		{
			x3::get<builder>(ctx).add_header(type);
			_val(ctx) = x3::get<builder>(ctx).add_counter();
		};
	};

	constexpr auto empty = no_case["EMPTY"];

	constexpr auto point_data = double_[add_double] >> double_[add_double];
	constexpr auto point_header = add_object(1) >> point_data;
	constexpr auto point_header_paren = '(' >> point_header >> ')';
	constexpr auto point = no_case["POINT"] >> point_header_paren;

	WKT_OBJECT(linestring_data) = eps[add_cnt] >> ('(' >> point_data[inc_cnt] % ',' >> ')' | empty);
	constexpr auto linestring_header = add_object(2) >> linestring_data;
	constexpr auto linestring = no_case["LINESTRING"] >> linestring_header;

	WKT_OBJECT(polygon_data) = eps[add_cnt] >> ('(' >> linestring_data[inc_cnt] % ',' >> ')' | empty);
	constexpr auto polygon_header = add_object(3) >> polygon_data;
	constexpr auto polygon = no_case["POLYGON"] >> polygon_header;

	WKT_OBJECT(multipoint) = no_case["MULTIPOINT"][add_multi(4)] >>
		('(' >> (point_header_paren[inc_cnt] % ',' | point_header[inc_cnt] % ',') >> ')' | empty);

	WKT_OBJECT(multilinestring) = no_case["MULTILINESTRING"][add_multi(5)] >>
		('(' >> linestring_header[inc_cnt] % ',' >> ')' | empty);

	WKT_OBJECT(multipolygon) = no_case["MULTIPOLYGON"][add_multi(6)] >>
		('(' >> polygon_header[inc_cnt] % ',' >> ')' | empty);

	constexpr auto collection_object = 
		point |
		linestring |
		polygon |
		multipoint |
		multilinestring |
		multipolygon;

	WKT_OBJECT(geometrycollection) = no_case["GEOMETRYCOLLECTION"][add_multi(7)] >>
		('(' >> collection_object[inc_cnt] % ',' >> ')' | empty);

	constexpr x3::rule<struct root> root(""); constexpr auto root_def = collection_object | geometrycollection;

	BOOST_SPIRIT_DEFINE(linestring_data, polygon_data, multipoint, multilinestring, multipolygon, geometrycollection, root);
}

}

bool wkt_to_bytes(const char*& begin, const char* end, std::string& data)
{
	const auto parser = x3::with<wkt_parser::builder>(WKBBuilder(data))[wkt_parser::root];
	return x3::phrase_parse(begin, end, parser, ascii::space);
}

}
