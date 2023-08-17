#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <cstring>
#include <regex>

#include "xls/xlscommon.h"

namespace xls {

static const std::regex _re_is_date_format(R"((?:\[[^\]]*\])|(?:"[^"]*")|(;)|([dmhysDMHYS]))");

bool is_date_format(const char* fmt)
{
	const char* fmt_end = fmt + std::strlen(fmt);
	for (std::cregex_iterator m(fmt, fmt_end, _re_is_date_format); m != std::cregex_iterator(); ++m)
		if ((*m)[1].matched)
			return false;
		else if ((*m)[2].matched)
			return true;
	return false;
}

static const double pow_10[] =
{
	1e+0,
	1e+1,  1e+2,  1e+3,  1e+4,  1e+5,  1e+6,  1e+7,  1e+8,  1e+9,  1e+10, 1e+11, 1e+12, 1e+13, 1e+14, 1e+15, 1e+16, 1e+17, 1e+18, 1e+19, 1e+20,
	1e+21, 1e+22
}; 

bool parse_number(const char* str, CellValue& value, bool is_date)
{
	const char* s = str;
#if 0
	bool is_int = !is_date;
	if (!is_date)
		for (; *s; ++s)
			if (size_t(*s - '0') > 9)
			{
				is_int = false;
				break;
			}
	if (is_int)
	{
		value.type = CellType::Integer;
		value.value_i = atoll(str);
	}
	else
	{
		value.type = is_date ? CellType::Date : CellType::Double;
		value.value_d = atof(str);
	}
	return;
#endif
	uint64_t n = 0;
	double d;
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
		if (n >= 1844674407370955161u && (n != 1844674407370955161u || c > 5))
			goto strtod_conversion;
		n = n * 10 + c;
		++s;
	}

	if (*s == '.')
	{
		++s;
		const char* frac_start = s;
		while ((c = size_t(*s - '0')) <= 9)
		{
			if (n >= 1844674407370955161u && (n != 1844674407370955161u || c > 5))
				goto strtod_conversion;
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
			if (exp >= 58)
				goto strtod_conversion;
			++s;
		}
		if (exp_minus)
			exp = -exp;
	}

	if (!is_date && exp == 0 && frac_length == 0)
	{
		value.type = CellType::Integer;
		if (minus)
		{
			value.value_i = -(int64_t)n;
			if (value.value_i < 0)
				return *s == '\0';
		}
		else
		{
			value.value_i = (int64_t)n;
			if (value.value_i >= 0)
				return *s == '\0';
		}
	}

	exp -= frac_length;
	d = (double)n;

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
		value.value_d = minus ? -d : d;
		value.type = is_date ? CellType::Date : CellType::Double;
		return *s == '\0';
	}

strtod_conversion:
	value.type = is_date ? CellType::Date : CellType::Double;
	char* endptr;
	value.value_d = std::strtod(str, &endptr);
	return *endptr == '\0';
}

}

extern "C" {

char* convert_utf16_to_utf8(const char* s_in, size_t len)
{
	const uint8_t* s = (const uint8_t*)s_in;
	size_t new_len = 0;
	for (size_t i = 1; i < len; i += 2)
		if (uint8_t c = s[i]; c == 0)
			new_len += (s[i-1] >> 7) + 1;
		else if ((c &~ 0x07) == 0)
			new_len += 2;
		else if ((c &~ 0x03) == 0xD8 && i+2 < len && (s[i+2] &~ 0x03) == 0xDC)
			new_len += 4, i += 2;
		else
			new_len += 3;

	uint8_t* w = (uint8_t*)malloc(new_len + 1);
	if (!w) return nullptr;

	for (size_t i = 0, j = 0; i < len; i += 2)
	{
		uint16_t c = s[i+1] << 8 | s[i];
		if ((c &~ 0x03FF) == 0xD800 && i+3 < len && (s[i+3] &~ 0x03) == 0xDC)
		{
			uint32_t cp = (c & 0x03FF) << 10;
			i += 2;
			c = s[i+1] << 8 | s[i];
			cp |= c & 0x3FF;
			cp += 0x10000;
			if (j+3 < new_len)
			{
				w[j++] = 0xF0 | cp >> 18;
				w[j++] = 0x80 | (cp >> 12) & 0x3F;
				w[j++] = 0x80 | (cp >> 6) & 0x3F;
				w[j++] = 0x80 | cp & 0x3F;
			}
		}
		else if (c <= 0x7F)
		{
			if (j < new_len)
				w[j++] = (uint8_t)c;
		}
		else if (c <= 0x7FF)
		{
			if (j+1 < new_len)
			{
				w[j++] = 0xC0 | c >> 6;
				w[j++] = 0x80 | c & 0x3F;
			}
		}
		else
		{
			if (j+2 < new_len)
			{
				w[j++] = 0xE0 | c >> 12;
				w[j++] = 0x80 | (c >> 6) & 0x3F;
				w[j++] = 0x80 | c & 0x3F;
			}
		}
	}
	w[new_len] = 0;
	return (char*)w;
}

}