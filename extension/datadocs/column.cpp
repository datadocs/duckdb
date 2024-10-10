#include <cmath>
#include <unordered_map>

#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/operator/string_cast.hpp"

#include "column.hpp"
#include "utility.h"
#include "converters.hpp"
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
		std::string buffer;
		if (!parse_money(v.GetData(), v.GetSize(), buffer) || !TryCast::Operation(string_t(buffer), result, true)) {
			return false;
		}
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
	if (!TryCast::Operation(v, result, false)) {
		std::string buffer;
		if (!parse_money(v.GetData(), v.GetSize(), buffer) || !TryCast::Operation(string_t(buffer), result, true)) {
			return false;
		}
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
	int32_t &dt = Writer().Get<int32_t>();
	return strptime(string(v), format, &dt, nullptr);
}

bool IngestColDATE::WriteExcelDate(double v) {
	Writer().Set((int32_t)v - 25569);
	return true;
}

bool IngestColTIME::Write(string_t v) {
	int64_t &micros = Writer().Get<int64_t>();
	return strptime(string(v), format, nullptr, &micros);
}

bool IngestColTIME::WriteExcelDate(double v) {
	Writer().Set(int64_t(v * Interval::MICROS_PER_DAY));
	return true;
}

bool IngestColTIMESTAMP::Write(string_t v) {
	int32_t dt;
	int64_t &micros = Writer().Get<int64_t>();
	if (!strptime(string(v), format, &dt, &micros)) {
		return false;
	}
	micros += dt * Interval::MICROS_PER_DAY;
	return true;
}

bool IngestColTIMESTAMP::WriteExcelDate(double v) {
	Writer().Set(int64_t((v - 25569) * Interval::MICROS_PER_DAY));
	return true;
}

bool IngestColINTERVAL::Write(string_t v) {
	string error_message;
	interval_t result;
	if (!Interval::FromCString(v.GetData(), v.GetSize(), result, &error_message, false)) {
		return false;
	}
	Writer().Set(result);
	return true;
}

bool IngestColINTERVAL::WriteExcelDate(double v) {
	Writer().Set(interval_t {0, 0, int64_t(v * Interval::MICROS_PER_DAY)});
	return true;
}

bool IngestColINTERVALFormat::Write(string_t v) {
	return strptime_interval(string(v), format, Writer().Get<interval_t>());
}

bool IngestColINTERVALFormat::WriteExcelDate(double v) {
	Writer().Set(interval_t {0, 0, int64_t(v * Interval::MICROS_PER_DAY)});
	return true;
}

bool IngestColINTERVALISO::Write(string_t v) {
	interval_t result;
	if (!IntervalFromISOString(v.GetData(), v.GetSize(), result)) {
		return false;
	}
	Writer().Set(result);
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
	const unsigned char *s = (const unsigned char*)v.GetData();
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
	const char *s = v.GetData();
	size_t i_read = (s[1] == 'x' || s[1] == 'X') && s[0] == '0' ? 2 : 0;
	char *res = Writer().ReserveString((size - i_read) / 2).GetDataWriteable();
	if (!string0x_to_bytes(s+i_read, s+size, res))
		return false;
	return true;
}

bool IngestColGEO::Write(string_t v) {
	string res;
	const char* begin = v.GetData();
	const char* end = begin + v.GetSize();
	if (!(wkt_to_bytes(begin, end, res) && begin == end)) {
		return false;
	}
	Writer().SetString(res);
	return true;
}

bool IngestColJSON::Write(string_t v) {
	alc.Reset();
	if (!JSONCommon::ReadDocumentUnsafe(v, JSONCommon::READ_FLAG, alc.GetYYAlc())) {
		return false;
	}
	Writer().SetString(v);
	return true;
}

bool IngestColJSON::Write(int64_t v) {
	Writer().SetVectorString(StringCast::Operation(v, GetVector()));
	return true;
}

bool IngestColJSON::Write(bool v) {
	Writer().SetVectorString(StringCast::Operation(v, GetVector()));
	return true;
}

bool IngestColJSON::Write(double v) {
	Writer().SetVectorString(StringCast::Operation(v, GetVector()));
	return true;
}

bool IngestColJSON::WriteExcelDate(double v) {
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
	string_t &result = Writer().ReserveString(length + 2);
	char *data = result.GetDataWriteable();
	data[length+1] = '"';
	*data++ = '"';
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

bool IngestColVariant::Write(string_t v) {
	auto writer = Writer();
	return VariantWriteValue(writer, Value(v));
}

bool IngestColVariant::Write(int64_t v) {
	auto writer = Writer();
	return VariantWriteValue(writer, Value(v));
}

bool IngestColVariant::Write(bool v) {
	auto writer = Writer();
	return VariantWriteValue(writer, Value::BOOLEAN(v));
}

bool IngestColVariant::Write(double v) {
	auto writer = Writer();
	return VariantWriteValue(writer, Value(v));
}

bool IngestColVariant::WriteExcelDate(double v) {
	bool have_date = v >= 1.0;
	bool have_time = v != std::trunc(v);
	Value value;
	if (have_date && have_time) {
		value = Value::TIMESTAMP(timestamp_t((v - 25569) * Interval::MICROS_PER_DAY));
	} else if (have_date) {
		value = Value::DATE(date_t((int32_t)v - 25569));
	} else {
		value = Value::TIME(dtime_t(v * Interval::MICROS_PER_DAY));
	}
	auto writer = Writer();
	return VariantWriteValue(writer, value);
}

IngestColBase *ColumnBuilder::Build(const IngestColumnDefinition &col, idx_t &cur_row) {
	switch(col.column_type) {
	case ColumnType::Geography:
		return new IngestColGEO(col.name, cur_row);
	case ColumnType::JSON:
		return new IngestColJSON(col.name, cur_row);
	default:
		return BuildColumn<ColumnBuilder>(col, cur_row);
	}
}

} // namespace duckdb
