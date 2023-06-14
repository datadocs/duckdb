#ifndef XLS_READER_H
#define XLS_READER_H

#include <string_view>

#include "xls/read_xls.h"
#include "xls/read_xlsx.h"

#include "inferrer_impl.h"
#include "file_reader.h"

namespace duckdb {

template<class TWorkBook>
class XLParser : public ParserImpl
{
public:
	XLParser(std::shared_ptr<BaseReader> reader);
	virtual ~XLParser() override;
	virtual bool do_infer_schema() override;
	virtual Schema* get_schema() override { return &m_schema; }
	virtual bool open() override;
	virtual void close() override;
	virtual int get_percent_complete() override;
	virtual size_t get_sheet_count() override;
	virtual std::vector<std::string> get_sheet_names() override;
	virtual bool select_sheet(const std::string& sheet_name) override;
	virtual bool select_sheet(size_t sheet_number) override;

protected:
	bool do_open_wb();
	virtual int64_t get_next_row_raw(RowRaw& row) override;

	XLSSchema m_schema;
	std::shared_ptr<BaseReader> m_reader;
	TWorkBook m_wb;
	typename TWorkBook::WorkSheetType m_ws;
	size_t m_selected_sheet;
	int m_row_number;

	//static const std::string_view file_signature;
	static const std::string_view file_extensions[];
};

typedef XLParser<xls::WorkBook> XLSParser;
typedef XLParser<xls::WorkBookX> XLSXParser;

}

#endif
