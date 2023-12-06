#ifndef INFERRER_IMPL_H
#define INFERRER_IMPL_H

#include <memory>

#include "json.h"
#include "xml.h"
#include "column.hpp"
#include "inferrer.h"

namespace duckdb {

const int INFER_MAX_ROWS = 100;

struct CellRawDate {
	double d;
	bool operator== (const CellRawDate& other) const { return other.d == d; }
};
typedef std::variant<std::string, int64_t, bool, double, CellRawDate> CellRaw;
typedef std::vector<CellRaw> RowRaw;

class RowRawNumbered : public RowRaw {
public:
	using RowRaw::RowRaw;
	int64_t row_no;
};

class Column;
class BaseReader;

IngestColBase* BuildColumn(const IngestColumnDefinition &col, idx_t &cur_row);

class ParserImpl : public Parser
{
public:
	virtual ~ParserImpl() = default;
	virtual bool infer_schema() override;
	virtual bool open() override;
	virtual void close() override;
	virtual void BuildColumns() override;
	virtual void BindSchema(std::vector<LogicalType> &return_types, std::vector<string> &names) override;
	virtual idx_t FillChunk(DataChunk &output) override;
	virtual int get_percent_complete() override;
	virtual size_t get_sheet_count() override;
	virtual std::vector<std::string> get_sheet_names() override;
	virtual bool select_sheet(const std::string& sheet_name) override;
	virtual bool select_sheet(size_t sheet_number) override;
	virtual size_t get_file_count() override;
	virtual std::vector<std::string> get_file_names() override;
	virtual bool select_file(const std::string& file_name) override;
	virtual bool select_file(size_t file_number) override;

	static ParserImpl* get_parser_from_reader(std::shared_ptr<BaseReader> reader);

protected:
	friend class ZIPParser;
	virtual bool do_infer_schema() = 0;
	virtual int64_t get_next_row_raw(RowRaw& row) { return -1; };
	void build_column_info(std::vector<Column>& columns);
	void infer_table(const std::string* comment);
	void do_infer_table(const std::string* comment, std::vector<RowRawNumbered>& rows);

	std::vector<std::unique_ptr<IngestColBase>> m_columns;
	idx_t cur_row;
	JSONDispatcher json_dispatcher;
	XMLParseHandler xml_handler;
};

}

#endif
