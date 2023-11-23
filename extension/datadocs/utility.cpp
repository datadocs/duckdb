#include <stddef.h>
#include <stdint.h>
#include <ctype.h>
#include <cctype>
#include <cmath>
#include <limits>
#include <regex>

#include "duckdb/common/string_util.hpp"
#include "utility.h"

namespace duckdb {

void rtrim(std::string& s)
{
	size_t sz;
	for (sz = s.size(); sz > 0; --sz)
		if (!StringUtil::CharacterIsSpace((unsigned char)s[sz - 1]))
			break;
	s.resize(sz);
}

void rtrim(std::string_view s)
{
	while (!s.empty() && StringUtil::CharacterIsSpace((unsigned char)s.back()))
		s.remove_suffix(1);
}

void trim(std::string& s)
{
	rtrim(s);
	size_t i = 0;
	while (i < s.size() && StringUtil::CharacterIsSpace((unsigned char)s[i])) ++i;
	if (i > 0)
		s.erase(0, i);
}

bool is_integer(double v)
{ return v >= std::numeric_limits<int64_t>::min() && v <= std::numeric_limits<int64_t>::max() && v == std::trunc(v); }

static const char* _weekdays[] = { "sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday" };
static const char* _months[] = { "january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december" };

static const std::regex _re_z(R"(([+-])(\d\d):?([0-5]\d)(?::?[0-5]\d(?:\.\d{1,6})?)?)");

inline static char _to_lower(char c)
{
	return c >= 'A' && c <= 'Z' ? c - ('A' - 'a') : c;
}

static bool find_string(const char*& src, const char** lookup, size_t size, int& result)
{
	for (size_t n = 0; n < size; ++n)
	{
		const char* cmp = lookup[n];
		for (size_t i = 0; ; ++i)
		{
			char c = _to_lower(src[i]);
			if (c != cmp[i])
			{
				if (i < 3)
					break;
				result = (int)n + 1;
				src += cmp[i] == '\0' ? i : 3; // matched whole string or first three chars
				return true;
			}
			else if (c == '\0') // matched to the end of src
			{
				result = (int)n + 1;
				src += i;
				return true;
			}
		}
	}
	return false;
}

static int read_number(const char*& src, size_t limit, int& result)
{
	int n = 0;
	size_t i;
	for (i = 0; i < limit; ++i)
	{
		char c = src[i];
		if (c < '0' || c > '9')
			break;
		n *= 10;
		n += c - '0';
	}
	if (i > 0)
	{
		result = n;
		src += i;
	}
	return (int)i;
}

bool strptime(const std::string& s_src, const std::string& s_fmt, double& dt)
{
	int day = 1;
	int month = 1;
	int year = 1904;
	bool has_date = false;
	int h = 0;
	bool h_12 = false;
	int m = 0;
	int s = 0;
	int ms = 0;
	int weekday = 0;
	bool h_pm = false;
	int tz_offset = 0;
	const char* src = s_src.data();
	for (const char* fmt = s_fmt.data(); *fmt != 0; ++fmt)
	{
		switch (*fmt)
		{
		case '%':
			switch (*++fmt)
			{
			case 'a':
			case 'A':
				if (!find_string(src, _weekdays, 7, weekday)) return false;
				break;
			case 'd':
				if (!read_number(src, 2, day) || day == 0) return false;
				has_date = true;
				break;
			case 'b':
			case 'B':
				if (!find_string(src, _months, 12, month)) return false;
				has_date = true;
				break;
			case 'm':
				if (!read_number(src, 2, month) || month == 0 || month > 12) return false;
				has_date = true;
				break;
			case 'y':
				if (read_number(src, 2, year) != 2) return false;
				year += year < 68 ? 2000 : 1900;
				has_date = true;
				break;
			case 'Y':
				if (read_number(src, 4, year) != 4 || year == 0) return false;
				has_date = true;
				break;
			case 'H':
				if (!read_number(src, 2, h) || h > 23) return false;
				h_12 = false;
				break;
			case 'I':
				if (!read_number(src, 2, h) || h == 0 || h > 12) return false;
				if (h == 12)
					h = 0;
				h_12 = true;
				break;
			case 'J':
				if (!read_number(src, 4, h)) return false;
				if (StringUtil::CharacterIsSpace(*src))
				{
					while (StringUtil::CharacterIsSpace(*++src));
					int h2 = 0;
					if (read_number(src, 2, h2))
					{
						if (h2 > 23) return false;
						h = h * 24 + h2;
					}
				}
				h_12 = false;
				break;
			case 'p':
				if (_to_lower(src[1]) != 'm')
					return false;
				if (char c = _to_lower(*src); c == 'p')
					h_pm = true;
				else if (c != 'a')
					return false;
				src += 2;
				break;
			case 'M':
				if (!read_number(src, 2, m) || m > 59) return false;
				break;
			case 'S':
				if (!read_number(src, 2, s) || s > 59) return false;
				break;
			case 'f':
				if (int i; !(i = read_number(src, 6, ms))) return false;
				else
					while (i < 6)
						ms *= 10, ++i;
				break;
			case 'z':
				{
					std::cmatch mm;
					if (!std::regex_search(src, mm, _re_z, std::regex_constants::match_continuous))
						return false;
					const char* s;
					int tz_h;
					s = mm[2].first; read_number(s, 2, tz_h);
					s = mm[3].first; read_number(s, 2, tz_offset);
					tz_offset += tz_h * 60;
					if (*src == '+')
						tz_offset = -tz_offset;
					src += mm.length();
				}
				break;
			case 'Z':
				while (std::isalnum(*src)) ++src;
				break;
			case '%':
				if (*src++ != '%')
					return false;
				break;
			default:
				return false;
			}
			break;
		case ' ':
		case '\t':
		case '\r':
		case '\n':
		case '\f':
		case '\v':
			while (StringUtil::CharacterIsSpace(*src))
				++src;
			break;
		default:
			if (*src++ != *fmt)
				return false;
			break;
		}
	}
	if (*src != '\0')
		return false;

	if (h_12 && h_pm)
		h += 12;
	dt = (h * 3600 + (m + tz_offset) * 60 + s + ms / 1000000.0) / 86400.0;

	if (has_date)
	{
		int max_day;
		if (month == 4 || month == 6 || month == 9 || month == 11)
			max_day = 30;
		else if (month == 2)
			max_day = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) ? 29 : 28;
		else
			max_day = 31;
		if (day > max_day)
			return false;

		int y = year;
		y -= month <= 2;
		int era = (y >= 0 ? y : y - 399) / 400;
		unsigned yoe = (unsigned)(y - era * 400); // [0, 399]
		unsigned doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1; // [0, 365]
		unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]

		dt += era * 146097 + (int)doe - 719468 + 25569;
	}
	else if (tz_offset != 0)
	{
		dt = dt - int(dt);
		if (dt < 0)
			dt += 1.0;
	}
	return true;
}

//double strptime(const std::string& src, const std::string& fmt)
//{
//	double dt;
//	if (!strptime(src.data(), fmt.data(), dt))
//		throw std::invalid_argument("invalid value");
//	return dt;
//}

}
