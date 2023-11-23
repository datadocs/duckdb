#ifndef CSV_READER_H
#define CSV_READER_H

#include "inferrer_impl.h"
#include "file_reader.h"

namespace duckdb {

class ucvt_streambuf;
const size_t MAX_STRING_SIZE = 1024 * 1024; // not accounting for terminating 0

class LimitedString : public std::string {
public:
	LimitedString &AppendChunk(char *start, char *end) {
		size_t sz = end - start;
		if (size() + sz > MAX_STRING_SIZE) {
			sz = MAX_STRING_SIZE - size();
		}
		append(start, sz);
		return *this;
	}
	void AppendChar(char c) {
		if (size() < MAX_STRING_SIZE)
			push_back(c);
	}
};

class CSVParser : public ParserImpl
{
public:
	CSVParser(std::shared_ptr<BaseReader> reader);
	virtual ~CSVParser() override;
	virtual bool do_infer_schema() override;
	virtual Schema* get_schema() override { return &m_schema; }
	virtual bool open() override;
	virtual void close() override;
	virtual int get_percent_complete() override;
	virtual idx_t FillChunk(DataChunk &output) override;

protected:
	bool next_char(char& c);
	bool check_next_char(char c);
	bool underflow();
	bool is_newline(char c);
	virtual int64_t get_next_row_raw(RowRaw& row) override;
	void write_value(size_t i_col);

	CSVSchema m_schema;
	std::shared_ptr<BaseReader> m_reader;
	std::unique_ptr<ucvt_streambuf> m_cvt_buf;
	size_t m_row_number;
	LimitedString tmp_string;
	unsafe_unique_array<char> buffer;
	bool read_finished;
	bool optimized;
	char *cur, *end;
	char *value_start, *value_end;
};

class WKTParser : public CSVParser
{
public:
	WKTParser(std::shared_ptr<BaseReader> reader);
	virtual bool do_infer_schema() override { return true; };
};

}

#endif
