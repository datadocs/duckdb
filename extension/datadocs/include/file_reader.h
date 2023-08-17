#ifndef FILE_READER_H
#define FILE_READER_H

#include <cstdio>
#include <string>

#include "duckdb.hpp"

#include "xls/xlscommon.h"

namespace duckdb {

class BaseReader
{
public:
	static constexpr size_t buf_size = 4096;

	BaseReader(const std::string& filename);
	virtual ~BaseReader();
	const std::string& filename() { return m_filename; }
	size_t filesize() { return m_content.size; }
	virtual bool is_file() = 0;
	bool open();
	void close();
	void skip_prefix(const std::string_view& prefix);
	const char* peek_start(size_t length);
	bool next_char(char& c);
	bool peek(char& c);
	bool check_next_char(char c);
	size_t read(char* buffer, size_t size);
	xls::MemBuffer* read_all();
	size_t tell() const;
	int pos_percent();

protected:
	bool underflow();
	virtual bool do_open() = 0;
	virtual void do_close() = 0;
	virtual int do_read(char* buffer, size_t size) = 0;

	std::string m_filename;
	xls::MemBuffer m_content;

	const char* m_read_pos;
	const char* m_read_end;
	char m_buffer[buf_size];
	size_t m_cnt_read;
};

class FileReader : public BaseReader
{
public:
	FileReader(const std::string& filename, ClientContext &context);
	virtual bool is_file() override { return true; }

protected:
	virtual bool do_open() override;
	virtual void do_close() override;
	virtual int do_read(char* buffer, size_t size) override;

	FileSystem &fs;
public:
	unique_ptr<FileHandle> file_handle;
};

}

#endif
