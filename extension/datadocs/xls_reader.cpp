#include <type_traits>
#include <utility>

#include "xls_reader.h"
#include "utility.h"

using namespace std::literals;

namespace duckdb {

//template <> constexpr std::string_view XLParser<xls::WorkBook>::file_signature = "\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"sv;
//template <> constexpr std::string_view XLParser<xls::WorkBookX>::file_signature = "PK\x03\x04"sv;

template<class TWorkBook>
XLParser<TWorkBook>::XLParser(std::shared_ptr<BaseReader> reader) :
	m_reader(reader),
	m_selected_sheet(0),
	m_row_number(0)
{
	m_schema.header_row = -1;
	m_schema.first_data_row = 1;
}

template<class TWorkBook>
XLParser<TWorkBook>::~XLParser()
{
	close();
}

template<class TWorkBook>
bool XLParser<TWorkBook>::do_infer_schema()
{
	m_schema.columns.clear();
	if (!open())
		return false;
	m_schema.header_row = -1;
	m_schema.first_data_row = 1;
	infer_table(nullptr);
	int n_rows = m_ws.nrows();
	if (n_rows > 0)
		m_schema.nrows = n_rows;
	return true;
}

template<class TWorkBook>
bool XLParser<TWorkBook>::open()
{
	if (!do_open_wb())
		return false;
	m_ws.close();
	m_ws = m_wb.sheet(m_selected_sheet);
	if (!m_ws)
		return false;
	m_row_number = 0;
	m_schema.comment_lines_skipped_in_parsing = 0;
	return ParserImpl::open();
}

template<class TWorkBook>
void XLParser<TWorkBook>::close()
{
	m_ws.close();
	m_wb.close();
}

template<class TWorkBook>
int XLParser<TWorkBook>::get_percent_complete()
{
	int nrows = m_ws.nrows();
	return nrows <= 0 ? 0 : m_row_number * 100 / nrows;
}

template<class TWorkBook>
size_t XLParser<TWorkBook>::get_sheet_count()
{
	do_open_wb();
	return m_wb.sheet_count();
}

template<class TWorkBook>
std::vector<std::string> XLParser<TWorkBook>::get_sheet_names()
{
	size_t sheet_cnt = get_sheet_count();
	std::vector<std::string> ret;
	ret.reserve(sheet_cnt);
	for (size_t i = 0; i < sheet_cnt; ++i)
		ret.push_back(m_wb.sheet_name(i));
	return ret;
}

template<class TWorkBook>
bool XLParser<TWorkBook>::select_sheet(const std::string_view &sheet_name)
{
	size_t sheet_cnt = get_sheet_count();
	for (size_t i = 0; i < sheet_cnt; ++i)
		if (sheet_name == m_wb.sheet_name(i))
			return select_sheet(i);
	return false;
}

template<class TWorkBook>
bool XLParser<TWorkBook>::select_sheet(size_t sheet_number)
{
	if (sheet_number >= get_sheet_count())
		return false;
	if (m_selected_sheet != sheet_number)
	{
		m_selected_sheet = sheet_number;
		m_ws.close();
	}
	return true;
}

template<class TWorkBook>
bool XLParser<TWorkBook>::do_open_wb()
{
	if (m_wb)
		return true;
	if (m_reader->is_file())
		return m_wb.open(m_reader.get());
	else
		return m_wb.open(m_reader->read_all());
}

template<class TWorkBook>
int64_t XLParser<TWorkBook>::get_next_row_raw(RowRaw& row)
{
	row.clear();
	do {
		if (!m_ws.next_row())
			return -1;
	} while (++m_row_number < m_schema.first_data_row); // skip first rows
	xls::CellValue value;
	while (m_ws.next_cell(value))
	{
		switch (value.type)
		{
		case xls::CellType::Empty:
		case xls::CellType::Error:
			row.emplace_back(std::in_place_type<std::string>);
			break;
		case xls::CellType::String:
			row.push_back(std::move(value.value_s));
			break;
		case xls::CellType::Double:
			if (std::is_same<TWorkBook, xls::WorkBook>::value && is_integer(value.value_d))
				row.emplace_back((int64_t)value.value_d);
			else
				row.emplace_back(value.value_d);
			break;
		case xls::CellType::Integer:
			row.emplace_back(value.value_i);
			break;
		case xls::CellType::Date:
			row.emplace_back(CellRawDate{value.value_d});
			break;
		case xls::CellType::Bool:
			row.emplace_back(value.value_b);
			break;
		}
	}
	return m_row_number;
}


template class XLParser<xls::WorkBook>;
template class XLParser<xls::WorkBookX>;

}
