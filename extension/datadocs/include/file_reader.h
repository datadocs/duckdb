#ifndef FILE_READER_H
#define FILE_READER_H

#include <cstdio>
#include <string>

#include "duckdb.hpp"

#include "xls/xlscommon.h"

#ifdef DATADOCS_DEBUG_FILE_IO
#include "debug.h"
#define debug_do_read_result(expected_bytes, actual_bytes) {\
	if (actual_bytes <= 0) {\
		console_log("do_read(%zu) pos=%zu FAILED", expected_bytes, m_position);\
	} else {\
		console_log("do_read(%zu) pos=%zu to=%zu len=%d", \
		expected_bytes, m_position, m_position + actual_bytes, actual_bytes);\
	}}
#define debug_file_io(...) console_log(__VA_ARGS__)
#else
#define debug_do_read_result(expected_bytes, actual_bytes)
#define debug_file_io(...)
#endif

namespace duckdb {

///
///  BaseReader
///    |---- derive ---> FileReader
///    |                    |---- Call ---> DuckDB::FileHandle for I/O operations
///    |
///    |---- derive ----> ZIPReader
///
/// ----------------------------------------------
///
///  XLParser<xls::WorkBook> -- alias --> XLSXParser
///    |
///    |---- call ---> WorkBookX::open(BaseReader *reader)
///                      |
///                      |---- call ---> unzOpenFS(reader)
///                                        |
///                                        ↓
///                 It creates a mini zip instance for reading the zip file,
///                 and binds reader's methods to this instance via `ioapi` with
///                 adapters

class BaseReader
{
public:
	static constexpr size_t buf_size = 4096;

	BaseReader(const std::string& filename);
	virtual ~BaseReader();
	const std::string& filename() { return m_filename; }
	size_t filesize() { return m_content.size; }
	virtual bool is_file() = 0;

	// basic file I/O methods
	bool open();
	void close();
	size_t read(char* buffer, size_t size);
	size_t tell() const;
	bool seek(size_t location);

	const char* peek_start(size_t length);
	bool next_char(char& c);
	bool peek(char& c);
	bool check_next_char(char c);
	xls::MemBuffer* read_all();
	int pos_percent();

protected:
	bool underflow();
	virtual bool do_open() = 0;
	virtual void do_close() = 0;
	virtual bool do_seek(size_t location) = 0;
	virtual int do_read(char *buffer, size_t size) = 0;

	std::string m_filename;
	xls::MemBuffer m_content;

	const char* m_read_pos;
	const char* m_read_end;
	char m_buffer[buf_size];
	size_t m_position;
	/// @brief Reset pointers related to the buffer
	void reset_buffer();
};

class FileReader : public BaseReader
{
public:
	FileReader(const std::string& filename, ClientContext &context);
	virtual bool is_file() override { return true; }

protected:
	virtual bool do_open() override;
	virtual void do_close() override;
	virtual bool do_seek(size_t location) override;
	virtual int do_read(char *buffer, size_t size) override;

	FileSystem &fs;
public:
	unique_ptr<FileHandle> file_handle;
};

}

#endif
