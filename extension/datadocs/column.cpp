#include <cmath>
#include <unordered_map>

#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/operator/string_cast.hpp"

#include "column.hpp"
#include "utility.h"
#include "type_conv.h"
#include "vector_proxy.hpp"
#include "wkt.h"

namespace duckdb {

bool IngestColVARCHAR::Write(string_t v) {
	Writer().SetString(v);
	return true;
}

bool IngestColVARCHAR::Write(int64_t v) {
	Writer().SetVectorString(StringCast::Operation(v, GetVector()));
	return true;
}

bool IngestColVARCHAR::Write(bool v) {
	Writer().SetVectorString(StringCast::Operation(v, GetVector()));
	return true;
}

bool IngestColVARCHAR::Write(double v) {
	Writer().SetVectorString(StringCast::Operation(v, GetVector()));
	return true;
}

bool IngestColVARCHAR::WriteExcelDate(double v) {
	int32_t date[3], time[4];
	idx_t date_length, year_length, time_length, length;
	bool add_bc;
	char micro_buffer[6];
	bool have_date = v >= 1.0;
	bool have_time = v != std::trunc(v);
	if (have_date)
	{
		Date::Convert(date_t((int)v - 25569), date[0], date[1], date[2]);
		length = date_length = have_time + DateToStringCast::Length(date, year_length, add_bc);
	} else {
		length = 0;
	}
	if (have_time)
	{
		long t = std::lround((v - int(v)) * 86400.0);
		time[0] = t / 3600;
		t %= 3600;
		time[1] = t / 60;
		time[2] = t % 60;
		time[3] = 0;
		time_length = TimeToStringCast::Length(time, micro_buffer);
		length += time_length;
	}
	string_t &result = Writer().ReserveString(length);
	char *data = result.GetDataWriteable();
	if (have_date) {
		DateToStringCast::Format(data, date, year_length, add_bc);
		data += date_length;
		if (have_time) {
			data[-1] = ' ';
		}
	}
	if (have_time) {
		TimeToStringCast::Format(data, time_length, time, micro_buffer);
	}
	result.Finalize();
	return true;
}

bool IngestColBOOLEAN::Write(string_t v) {
	auto it = bool_dict.find(string(v));
	if (it == bool_dict.end())
		return false;
	Writer().Set(it->second);
	return true;
}

bool IngestColBOOLEAN::Write(int64_t v) {
	if (v != 0 && v != 1) {
		return false;
	}
	Writer().Set((bool)v);
	return true;
}

bool IngestColBOOLEAN::Write(bool v) {
	Writer().Set(v);
	return true;
}

bool IngestColBIGINT::Write(string_t v) {
	int64_t result;
	if (!TryCast::Operation(v, result, true)) {
		return false;
	}
	Writer().Set(result);
	return true;
}

bool IngestColBIGINT::Write(int64_t v) {
	Writer().Set(v);
	return true;
}

bool IngestColBIGINT::Write(bool v) {
	Writer().Set((int64_t)v);
	return true;
}

bool IngestColBIGINT::Write(double v) {
	if (!is_integer(v)) {
		return false;
	}
	Writer().Set((int64_t)v);
	return true;
}

bool IngestColDOUBLE::Write(string_t v) {
	double result;
	if (!TryCast::Operation(v, result, true)) {
		return false;
	}
	Writer().Set(result);
	return true;
}

bool IngestColDOUBLE::Write(int64_t v) {
	Writer().Set((double)v);
	return true;
}

bool IngestColDOUBLE::Write(bool v) {
	Writer().Set((double)v);
	return true;
}

bool IngestColDOUBLE::Write(double v) {
	Writer().Set(v);
	return true;
}

bool IngestColDATE::Write(string_t v) {
	double t;
	if (!strptime(string(v), format, t)) {
		return false;
	}
	return WriteExcelDate(t);
}

bool IngestColDATE::WriteExcelDate(double v) {
	Writer().Set((int32_t)v - 25569);
	return true;
}

bool IngestColTIME::Write(string_t v) {
	double t;
	if (!strptime(string(v), format, t)) {
		return false;
	}
	return WriteExcelDate(t);
}

bool IngestColTIME::WriteExcelDate(double v) {
	Writer().Set(int64_t(v * Interval::MICROS_PER_DAY));
	return true;
}

bool IngestColTIMESTAMP::Write(string_t v) {
	double t;
	if (!strptime(string(v), format, t)) {
		return false;
	}
	return WriteExcelDate(t);
}

bool IngestColTIMESTAMP::WriteExcelDate(double v) {
	Writer().Set(int64_t((v - 25569) * Interval::MICROS_PER_DAY));
	return true;
}

static inline const uint8_t _base64tbl[256] = {
	0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
	0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
	0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 62, 63, 62, 62, 63, // +,-./
	52, 53, 54, 55, 56, 57, 58, 59, 60, 61,  0,  0,  0,  0,  0,  0, // 0-9
	0,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, // a-o
	15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,  0,  0,  0,  0, 63, // p-z _
	0, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, // A-O
	41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51                      // P-Z
};

bool IngestColBLOBBase64::Write(string_t v) {
	idx_t size = v.GetSize();
	if (size == 0) {
		Writer().SetString(v);
		return true;
	}
	const unsigned char *s = (const unsigned char*)v.GetDataUnsafe();
	while (s[size - 1] == '=')
		--size;
	size_t len_tail = size % 4;
	if (len_tail == 1)
		return false;
	size -= len_tail;
	char *res = Writer().ReserveString(size / 4 * 3 + (len_tail > 0 ? len_tail-1 : 0)).GetDataWriteable();
	size_t i_write = 0;
	for (size_t i_read = 0; i_read < size; i_read += 4)
	{
		uint32_t n = _base64tbl[s[i_read]] << 18 | _base64tbl[s[i_read + 1]] << 12 | _base64tbl[s[i_read + 2]] << 6 | _base64tbl[s[i_read + 3]];
		res[i_write++] = n >> 16;
		res[i_write++] = (n >> 8) & 0xFF;
		res[i_write++] = n & 0xFF;
	}
	if (len_tail > 0)
	{
		uint32_t n = _base64tbl[s[size]] << 18 | _base64tbl[s[size + 1]] << 12;
		res[i_write++] = n >> 16;
		if (len_tail == 3)
		{
			n |= _base64tbl[s[size + 2]] << 6;
			res[i_write] = (n >> 8) & 0xFF;
		}
	}
	return true;
}

bool IngestColBLOBHex::Write(string_t v) {
	idx_t size = v.GetSize();
	if (size == 0) {
		Writer().SetString(v);
		return true;
	}
	if (size % 2 != 0)
		return false;
	const char *s = v.GetDataUnsafe();
	size_t i_read = (s[1] == 'x' || s[1] == 'X') && s[0] == '0' ? 2 : 0;
	char *res = Writer().ReserveString((size - i_read) / 2).GetDataWriteable();
	if (!string0x_to_bytes(s+i_read, s+size, res))
		return false;
	return true;
}

IngestColNUMERIC::IngestColNUMERIC(string name, idx_t &cur_row, uint8_t i_digits, uint8_t f_digits) noexcept
    : IngestColBase(std::move(name), cur_row) {
	int need_width = i_digits + f_digits;
	if (need_width <= Decimal::MAX_WIDTH_INT16) {
		width = Decimal::MAX_WIDTH_INT16;
		storage_type = 0;
	} else if (need_width <= Decimal::MAX_WIDTH_INT32) {
		width = Decimal::MAX_WIDTH_INT32;
		storage_type = 1;
	} else if (need_width <= Decimal::MAX_WIDTH_INT64) {
		width = Decimal::MAX_WIDTH_INT64;
		storage_type = 2;
	} else {
		width = Decimal::MAX_WIDTH_DECIMAL;
		storage_type = 3;
		if (need_width > Decimal::MAX_WIDTH_DECIMAL) {
			f_digits = MaxValue(0, Decimal::MAX_WIDTH_DECIMAL - i_digits);
		}
	}
	scale = f_digits;
}

bool IngestColNUMERIC::Write(string_t v) {
	string message;
	switch (storage_type) {
	case 0: return TryCastToDecimal::Operation(v, Writer().Get<int16_t>(), &message, width, scale);
	case 1: return TryCastToDecimal::Operation(v, Writer().Get<int32_t>(), &message, width, scale);
	case 2: return TryCastToDecimal::Operation(v, Writer().Get<int64_t>(), &message, width, scale);
	default: return TryCastToDecimal::Operation(v, Writer().Get<hugeint_t>(), &message, width, scale);
	}
}

bool IngestColNUMERIC::Write(int64_t v) {
	string message;
	switch (storage_type) {
	case 0: return TryCastToDecimal::Operation(v, Writer().Get<int16_t>(), &message, width, scale);
	case 1: return TryCastToDecimal::Operation(v, Writer().Get<int32_t>(), &message, width, scale);
	case 2: return TryCastToDecimal::Operation(v, Writer().Get<int64_t>(), &message, width, scale);
	default: return TryCastToDecimal::Operation(v, Writer().Get<hugeint_t>(), &message, width, scale);
	}
}

bool IngestColNUMERIC::Write(bool v) {
	return Write((int64_t)v);
}

bool IngestColNUMERIC::Write(double v) {
	string message;
	switch (storage_type) {
	case 0: return TryCastToDecimal::Operation(v, Writer().Get<int16_t>(), &message, width, scale);
	case 1: return TryCastToDecimal::Operation(v, Writer().Get<int32_t>(), &message, width, scale);
	case 2: return TryCastToDecimal::Operation(v, Writer().Get<int64_t>(), &message, width, scale);
	default: return TryCastToDecimal::Operation(v, Writer().Get<hugeint_t>(), &message, width, scale);
	}
}

bool IngestColGEO::Write(string_t v) {
	string res;
	const char* begin = v.GetDataUnsafe();
	const char* end = begin + v.GetSize();
	if (!(wkt_to_bytes(begin, end, res) && begin == end)) {
		return false;
	}
	Writer().SetString(res);
	return true;
}

} // namespace duckdb
