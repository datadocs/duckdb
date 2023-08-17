#include <cstdlib>
#include <cstring>
#include <utility>

#include "xls/read_xls.h"

#include "xls.h"

namespace xls {

struct WorkSheet::Impl
{
	Impl(libxls::xlsWorkBook* wb, size_t num) :
		ws(),
		irow(-1),
		icol(-1)
	{
		ws.filepos = wb->sheets.sheet[num].filepos;
		ws.workbook = wb;
	}

	~Impl()
	{
		if (ws.rows.row)
		{
			for(size_t j = 0; j <= ws.rows.lastrow; ++j)
			{
				auto row = &ws.rows.row[j];
				for(size_t i = 0; i < row->cells.count; ++i)
					std::free(row->cells.cell[i].str);
				std::free(row->cells.cell);
			}
			std::free(ws.rows.row);
		}
		std::free(ws.colinfo.col);
	}

	libxls::xlsWorkSheet ws;
	int irow;
	int icol;
};

WorkSheet::WorkSheet() = default;
WorkSheet::WorkSheet(WorkSheet&&) = default;
WorkSheet& WorkSheet::operator = (WorkSheet&&) = default;
WorkSheet::~WorkSheet() = default;
WorkSheet::WorkSheet(WorkSheet::Impl* ws) : d(ws) {}

void WorkSheet::close()
{
	d.reset();
}

WorkSheet::operator bool()
{
	return static_cast<bool>(d);
}

int WorkSheet::nrows()
{
	return d ? d->ws.rows.lastrow + 1 : -1;
}

bool WorkSheet::next_row()
{
	if (!d || d->irow >= (int)d->ws.rows.lastrow)
		return false;
	++d->irow;
	d->icol = -1;
	return true;
}

bool WorkSheet::next_cell(CellValue& value)
{
	if (!d ||
		d->irow < 0 ||
		d->irow > d->ws.rows.lastrow ||
		d->icol >= (int)d->ws.rows.lastcol ||
		!d->ws.rows.row)
		return false;
	++d->icol;
	const libxls::xlsCell* cell = &d->ws.rows.row[d->irow].cells.cell[d->icol];
	if (!cell)
	{
		value.type = CellType::Empty;
		return true;
	}
	bool is_double = false;
	switch (cell->id)
	{
	case XLS_RECORD_RK:
	case XLS_RECORD_MULRK:
	case XLS_RECORD_NUMBER:
		is_double = true;
		break;
	case XLS_RECORD_BOOLERR:
		if (!std::strcmp(cell->str, "error"))
			value.type = CellType::Error;
		else
			value = cell->d == 1.0;
		break;
	case XLS_RECORD_FORMULA:
	case XLS_RECORD_FORMULA_ALT:
		if (cell->l == 0)
			is_double = true;
		else if (!std::strcmp(cell->str, "bool"))
			value = cell->d == 1.0;
		else if (!std::strcmp(cell->str, "error"))
			value.type = CellType::Error;
		else
			value = cell->str;
		break;
	default:
		if (cell->str != nullptr)
			value = cell->str;
		else
			value.type = CellType::Empty;
		break;
	}
	if (is_double)
	{
		libxls::xlsWorkBook* wb = d->ws.workbook;
		if (cell->xf < wb->xfs.count)
		{
			libxls::WORD fmt = wb->xfs.xf[cell->xf].format;
			if (
				fmt >= 14 && fmt <= 22 ||
				fmt >= 27 && fmt <= 36 ||
				fmt >= 45 && fmt <= 47 ||
				fmt >= 50 && fmt <= 58 ||
				fmt >= 71 && fmt <= 81 ||
				fmt > 163 && false)
			{
				value.type = CellType::Date;
				value.value_d = cell->d;
				if (wb->is1904)
					value.value_d += 1462;
				return true;
			}
		}
		value = cell->d;
	}
	return true;
}


struct WorkBook::Impl
{
public:
	Impl(libxls::OLE2Stream* olestr) :
		wb()
	{
		wb.olestr = olestr;
	}

	~Impl()
	{
		if (wb.olestr)
		{
			libxls::OLE2* ole = wb.olestr->ole;
			libxls::ole2_fclose(wb.olestr);
			libxls::ole2_close(ole);
		}

		for(size_t i = 0; i < wb.sheets.count; ++i)
			std::free(wb.sheets.sheet[i].name);
		std::free(wb.sheets.sheet);

		for(size_t i = 0; i < wb.sst.count; ++i)
			std::free(wb.sst.string[i].str);
		std::free(wb.sst.string);

		std::free(wb.xfs.xf);

		for(size_t i = 0; i < wb.fonts.count; ++i)
			std::free(wb.fonts.font[i].name);
		std::free(wb.fonts.font);

		for(size_t i = 0; i < wb.formats.count; ++i)
			std::free(wb.formats.format[i].value);
		std::free(wb.formats.format);
	}

	static std::unique_ptr<Impl> create(libxls::OLE2* ole)
	{
		if (!ole)
			return nullptr;

		libxls::OLE2Stream* olestr;
		if (!(olestr = libxls::ole2_fopen(ole, "Workbook")) && !(olestr = libxls::ole2_fopen(ole, "Book")))
		{
			libxls::ole2_close(ole);
			return nullptr;
		}

		std::unique_ptr<Impl> d = std::make_unique<Impl>(olestr);
		if (libxls::xls_parseWorkBook(&d->wb) == libxls::LIBXLS_OK)
			return d;
		return nullptr;
	}

	libxls::xlsWorkBook wb;
};

WorkBook::WorkBook() = default;
WorkBook::WorkBook(WorkBook&&) = default;
WorkBook& WorkBook::operator = (WorkBook&&) = default;
WorkBook::~WorkBook() = default;

bool WorkBook::open(duckdb::BaseReader *reader)
{
	d.reset();
	reader->open();
	MemBuffer* buffer = reader->read_all();
	d = Impl::create(libxls::ole2_open_buffer(buffer->buffer, buffer->size));
	return static_cast<bool>(d);
}

bool WorkBook::open(MemBuffer* buffer)
{
	d.reset();
	d = Impl::create(libxls::ole2_open_buffer(buffer->buffer, buffer->size));
	return static_cast<bool>(d);
}

void WorkBook::close()
{
	d.reset();
}

WorkBook::operator bool()
{
	return static_cast<bool>(d);
}

size_t WorkBook::sheet_count()
{
	return d ? d->wb.sheets.count : 0;
}

std::string WorkBook::sheet_name(size_t sheet_number)
{
	return d && sheet_number < d->wb.sheets.count ? d->wb.sheets.sheet[sheet_number].name : std::string();
}

WorkSheet WorkBook::sheet(size_t sheet_number)
{
	if (sheet_number < sheet_count())
	{
		WorkSheet::Impl* ws = new WorkSheet::Impl(&d->wb, sheet_number);
		if (libxls::xls_parseWorkSheet(&ws->ws) == libxls::LIBXLS_OK)
			return WorkSheet(ws);
		delete ws;
	}
	return WorkSheet();
}

}
