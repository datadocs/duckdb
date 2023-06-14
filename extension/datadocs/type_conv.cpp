#include <stdint.h>
#include <cctype>
#include <limits>
#include <vector>
#include <string>
#include <unordered_map>
#include <charconv>
#include <regex>

#include "inferrer.h"
#include "wkt.h"
#include "type_conv.h"

namespace duckdb {

const std::unordered_map<string, bool> bool_dict {
	{"0", false}, {"1", true},
	{"false", false}, {"False", false}, {"FALSE", false}, {"true", true}, {"True", true}, {"TRUE", true},
	{"n", false}, {"N", false}, {"no", false}, {"No", false}, {"NO", false},
	{"y", true}, {"Y", true}, {"yes", true}, {"Yes", true}, {"YES", true}
};

static constexpr unsigned pow10i[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000};

bool string0x_to_bytes(const char* begin, const char* end, char* dst)
{
	while (begin < end)
	{
		char ch = *begin++;
		char off;
		if (ch >= '0' && ch <= '9') off = '0';
		else if (ch >= 'A' && ch <= 'F') off = 'A' - 10;
		else if (ch >= 'a' && ch <= 'f') off = 'a' - 10;
		else return false;
		char hi = (ch - off) << 4;
		ch = *begin++;
		if (ch >= '0' && ch <= '9') off = '0';
		else if (ch >= 'A' && ch <= 'F') off = 'A' - 10;
		else if (ch >= 'a' && ch <= 'f') off = 'a' - 10;
		else return false;
		*dst++ = hi | (ch - off);
	}
	return true;
}

bool string_to_decimal(const char* begin, const char* end, std::string& data)
{
	typedef uint32_t group_type;
	constexpr unsigned group_bytes = sizeof(group_type);
	constexpr unsigned group_digits = 9;
	constexpr unsigned max_groups = 9;
	static constexpr unsigned bytes_per_digits[group_digits] = {0, 1, 1, 2, 2, 3, 3, 4, 4};

	group_type mask = *begin == '-' ? -1 : 0;
	if (mask != 0 || *begin == '+')
		++begin;

	unsigned i_digits, f_digits;
	const char* s = begin;
	while (*s - '0' <= 9u) ++s;
	i_digits = s - begin;

	const char* point = s;
	if (*s == '.')
	{
		while (*++s - '0' <= 9u);
		f_digits = s - point - 1;
	}
	else
		f_digits = 0;

	if (i_digits == 0 && f_digits == 0)
		return false;

	const char* m_end = s;
	const char* begin_nz = begin;
	while (*begin_nz == '0' || *begin_nz == '.') ++begin_nz;
	if (begin_nz == m_end)
		mask = 0;

	unsigned head_zeros = 0, tail_zeros = 0;
	if (*s == 'E' || *s == 'e')
	{
		bool exp_minus = *++s == '-';
		if (exp_minus || *s == '+')
			++s;
		const char* exp_begin = s;
		int exp = 0;
		unsigned c;
		while ((c = *s - '0') <= 9u)
		{
			exp = exp * 10 + c;
			++s;
		}
		c = s - exp_begin;
		if (c == 0 || c > 9)
			return false;
		if (exp_minus)
			exp = -exp;

		if (begin_nz == m_end)
		{
			i_digits = tail_zeros = 1;
			f_digits = 0;
		}
		else
		{
			begin = begin_nz;
			do --m_end; while (*m_end == '0' || *m_end == '.');
			f_digits = m_end - begin;
			if (m_end < point || point < begin)
				++f_digits;
			i_digits = 0;
			exp += point - begin + (point < begin);
			if (exp <= 0)
			{
				head_zeros = -exp;
				f_digits += head_zeros;
			}
			else if (exp <= f_digits)
			{
				i_digits = exp;
				f_digits -= exp;
			}
			else
			{
				tail_zeros = exp - f_digits;
				i_digits = exp;
				f_digits = 0;
			}
		}
	}

	if (s != end)
		return false;

	unsigned n_groups = i_digits / group_digits + f_digits / group_digits;
	unsigned head_digits = i_digits % group_digits, head_bytes = bytes_per_digits[head_digits];
	unsigned tail_digits = f_digits % group_digits, tail_bytes = bytes_per_digits[tail_digits];
	if (n_groups + (head_bytes > 0) + (tail_bytes > 0) > max_groups)
		return false;

	unsigned total_bytes = n_groups * group_bytes + head_bytes + tail_bytes;
	data.resize(2 + total_bytes);
	char* dst = data.data();
	dst[0] = (char)i_digits;
	dst[1] = (char)f_digits;

	s = begin;
	char* w = dst + 2;

	if (head_zeros > 0)
	{
		unsigned zero_groups = head_zeros / group_digits;
		head_zeros %= group_digits;
		n_groups -= zero_groups;
		if (n_groups > 0)
		{
			--n_groups;
			head_digits = group_digits - head_zeros;
			head_bytes = group_bytes;
		}
		else
			tail_digits -= head_zeros;
		for (unsigned i = 0; i < zero_groups; ++i, w += group_bytes)
			*(group_type*)w = mask;
	}
	else if (tail_zeros > 0)
	{
		unsigned zero_groups = tail_zeros / group_digits;
		tail_zeros %= group_digits;
		n_groups -= zero_groups;
		if (n_groups > 0)
		{
			--n_groups;
			tail_digits = group_digits - tail_zeros;
			tail_bytes = group_bytes;
		}
		else
		{
			tail_digits = head_digits - tail_zeros;
			tail_bytes = head_bytes;
			head_digits = head_bytes = 0;
		}
		group_type* wz = (group_type*)(w + total_bytes);
		for (unsigned i = 0; i < zero_groups; ++i)
			*--wz = mask;
	}

	group_type x;

	auto read_digits = [&x, &s](unsigned n_digits)
	{
		x = 0;
		while (n_digits --> 0)
		{
			if (*s == '.') ++s;
			x = x * 10 + *s++ - '0';
		}
	};

	auto write_digits = [&x, &w, mask](unsigned n_bytes)
	{
		x ^= mask;
		for (unsigned i = n_bytes; i --> 0; x >>= 8)
			w[i] = (char)x;
	};

	read_digits(head_digits);
	write_digits(head_bytes);
	w += head_bytes;

	while (n_groups --> 0)
	{
		read_digits(group_digits);
		write_digits(group_bytes);
		w += group_bytes;
	}

	read_digits(tail_digits);
	if (tail_zeros > 0)
		x *= pow10i[tail_zeros];
	write_digits(tail_bytes);

	dst[2] ^= 0x80;
	return true;
}

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
	while (std::isspace(*begin))
		if (++begin >= end)
			return false;
	while (std::isspace(end[-1])) --end;
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

void string_to_variant(const char* src, int src_length, VariantCell& cell)
{
	if (!string_to_variant_inner(src, src + src_length, cell))
	{
		cell.type = VariantCell::String;
		cell.data.assign(src, src_length);
	}
}

}
