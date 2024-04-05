#ifndef FILE_READER_H
#define FILE_READER_H

#include "duckdb.hpp"
#include "xls/xlscommon.h"

#include <cstdio>
#include <string>

#ifdef DATADOCS_DEBUG_FILE_IO
#include "debug.h"
#define debug_do_read_result(expected_bytes, actual_bytes)                                                             \
	{                                                                                                                  \
		if (actual_bytes < 0) {                                                                                       \
			console_log("do_read(%zu) pos=%zu FAILED", expected_bytes, m_position_next_read);                          \
		} else {                                                                                                       \
			console_log("do_read(%zu) pos=%zu to=%zu len=%d", expected_bytes, m_position_next_read,                    \
			            m_position_next_read + actual_bytes, actual_bytes);                                            \
		}                                                                                                              \
	}
#define debug_file_io(...) console_log(__VA_ARGS__)
#else
#define debug_do_read_result(expected_bytes, actual_bytes)
#define debug_file_io(...)
#endif

#define BASEREADER_READ_FLAG_NO_BUF 0x01

namespace duckdb {

// todo file_reader_buff.h BaseReaderBuffer { m_buffer, m_pos_read, m_pos_end, ...  }

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
	static constexpr size_t default_buf_size = 1024;

	BaseReader(const std::string& filename);
	virtual ~BaseReader();
	const std::string& filename() { return m_filename; }
	size_t filesize() { return m_content.size; }
	bool is_open() { return m_is_open; }
	virtual bool is_file() = 0;

	// basic file I/O methods
	bool open();
	void close();
	size_t read(char* buffer, size_t size, uint8_t flag = 0x00);
	size_t tell() const;
	bool seek(size_t location);

	bool skip_prefix(const std::string_view &prefix);
	const char* peek_start(size_t length);
	bool next_char(char& c);
	bool peek(char& c);
	bool check_next_char(char c);
	xls::MemBuffer* read_all();
	int pos_percent();
	/// @warning Please only enable this feature for the reader that supports `do_seek`
	void enable_async_seek() {
		m_enabled_async_seek = true;
	}
	bool set_buffer_size(size_t size);

protected:
	bool underflow();
	/// @brief Attempts to refill the internal buffer of the BaseReader object, if necessary,
	///        to ensure that at least `min(buf_size, desired_bytes)` are available for reading.
	bool underflow(size_t desired_bytes);
	virtual bool do_open() = 0;
	virtual void do_close() = 0;
	virtual bool do_seek(size_t location) = 0;
	virtual int do_read(char *buffer, size_t size) = 0;

	std::string m_filename;
	xls::MemBuffer m_content;
	bool m_is_open = false;

private:
	/// @brief (Invalidate the buffer) Reset pointers related to the buffer
	/// @param position_buf (The new position value for the members:
	///        `m_position_buf` and `m_position_next_read`)
	void reset_buffer(size_t position_buf);
	size_t consume_buffer(char* dest, size_t max_bytes);

	const char *m_read_pos;
	const char *m_read_end;
	inline size_t remaining_bytes_in_buffer() const {
		return m_read_end - m_read_pos;
	}
	inline size_t current_buffer_size() const {
		return m_read_end - m_buffer;
	}
	char *m_buffer;
	size_t m_buf_size;
	/// @brief The position of `m_buffer[0]` in the original file .
	size_t m_position_buf;
	size_t m_position_next_read;

	uint8_t m_optimization_read_backward;

	bool m_enabled_async_seek;
	long m_pending_async_seek;
	bool handle_async_seek();
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
