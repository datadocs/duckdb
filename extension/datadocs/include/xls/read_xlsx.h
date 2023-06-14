#ifndef READ_XLSX_H
#define READ_XLSX_H

#include <memory>

#include "xlscommon.h"

namespace xls {

class WorkSheetX
{
public:
	WorkSheetX();
	WorkSheetX(const WorkSheetX&) = delete;
	WorkSheetX& operator = (const WorkSheetX&) = delete;
	WorkSheetX(WorkSheetX&&);
	WorkSheetX& operator = (WorkSheetX&&);
	~WorkSheetX();
	void close();
	explicit operator bool();
	int nrows();
	bool next_row();
	bool next_cell(CellValue& value);

private:
	friend class WorkBookX;
	struct Impl;
	WorkSheetX(Impl* ws);
	std::unique_ptr<Impl> d;
};

class WorkBookX
{
public:
	typedef WorkSheetX WorkSheetType;

	WorkBookX();
	WorkBookX(const WorkBookX&) = delete;
	WorkBookX& operator = (const WorkBookX&) = delete;
	WorkBookX(WorkBookX&& other);
	WorkBookX& operator = (WorkBookX&& other);
	~WorkBookX();
	bool open(const std::string& filename);
	bool open(MemBuffer* buffer);
	void close();
	explicit operator bool();
	size_t sheet_count();
	std::string sheet_name(size_t sheet_number);
	WorkSheetX sheet(size_t sheet_number);

	struct Impl;
private:
	std::unique_ptr<Impl> d;
};

}

#endif
