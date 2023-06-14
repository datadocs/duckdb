#ifndef XLSCOMMON_H
#define XLSCOMMON_H

#include <cstdint>
#include <string>

namespace xls {

struct MemBuffer
{
	char* buffer = nullptr;
	size_t size = 0;
	size_t pos = 0;
	~MemBuffer() { clear(); }
	void clear() { delete[] buffer; buffer = nullptr; }
};

enum class CellType { Empty, String, Integer, Double, Date, Bool, Error };

struct CellValue
{
	CellType type;
	std::string value_s;
	union
	{
		int64_t value_i;
		double value_d;
		bool value_b;
	};
	CellValue& operator = (const std::string& value) { type = CellType::String; value_s = value; return *this; }
	CellValue& operator = (char*   value) { type = CellType::String;  value_s = value; return *this; }
	CellValue& operator = (double  value) { type = CellType::Double;  value_d = value; return *this; }
	CellValue& operator = (int64_t value) { type = CellType::Integer; value_i = value; return *this; }
	CellValue& operator = (bool    value) { type = CellType::Bool;    value_b = value; return *this; }
};

bool is_date_format(const char* s);
bool parse_number(const char* s, CellValue& value, bool is_date);

}

extern "C" {

char* convert_utf16_to_utf8(const char* s_in, size_t len);

}

#endif
