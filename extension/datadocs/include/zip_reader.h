#ifndef ZIP_READER_H
#define ZIP_READER_H

#include "inferrer_impl.h"
#include "file_reader.h"

namespace duckdb {

class ZIPParser : public ParserImpl
{
public:
	ZIPParser(std::shared_ptr<BaseReader> reader);
	virtual ~ZIPParser() override;
	virtual bool do_infer_schema() override;
	virtual Schema* get_schema() override;
	virtual bool open() override;
	virtual void close() override;
	virtual void BuildColumns() override;
	virtual void BindSchema(std::vector<LogicalType> &return_types, std::vector<string> &names) override;
	virtual int get_percent_complete() override;
	virtual size_t get_sheet_count() override;
	virtual std::vector<std::string> get_sheet_names() override;
	virtual bool select_sheet(const std::string& sheet_name) override;
	virtual bool select_sheet(size_t sheet_number) override;
	virtual size_t get_file_count() override;
	virtual std::vector<std::string> get_file_names() override;
	virtual bool select_file(const std::string& file_name) override;
	virtual bool select_file(size_t file_number) override;
	virtual idx_t FillChunk(DataChunk &output) override;

protected:
	bool do_open_zip();

	Schema m_invalid_schema;
	std::shared_ptr<BaseReader> m_reader;
	void* m_zip;
	std::vector<std::string> m_files;
	std::unique_ptr<ParserImpl> m_parser;
};

}

#endif
