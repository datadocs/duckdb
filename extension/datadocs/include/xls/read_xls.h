#ifndef READ_XLS_H
#define READ_XLS_H

#include <memory>

#include "xlscommon.h"

namespace xls {

class WorkSheet
{
public:
	WorkSheet();
	WorkSheet(const WorkSheet&) = delete;
	WorkSheet& operator = (const WorkSheet&) = delete;
	WorkSheet(WorkSheet&&);
	WorkSheet& operator = (WorkSheet&&);
	~WorkSheet();
	void close();
	explicit operator bool();
	int nrows();
	bool next_row();
	bool next_cell(CellValue& value);

private:
	friend class WorkBook;
	struct Impl;
	WorkSheet(Impl* ws);
	std::unique_ptr<Impl> d;
};

class WorkBook
{
public:
	typedef WorkSheet WorkSheetType;

	WorkBook();
	WorkBook(const WorkBook&) = delete;
	WorkBook& operator = (const WorkBook&) = delete;
	WorkBook(WorkBook&& other);
	WorkBook& operator = (WorkBook&& other);
	~WorkBook();
	bool open(const std::string& filename);
	bool open(MemBuffer* buffer);
	void close();
	explicit operator bool();
	size_t sheet_count();
	std::string sheet_name(size_t sheet_number);
	WorkSheet sheet(size_t sheet_number);

private:
	struct Impl;
	std::unique_ptr<Impl> d;
};

}

#endif
