#include "file_reader.h"

#include "debug.h"

#include <algorithm>
#include <string.h>

namespace duckdb {

BaseReader::BaseReader(const std::string &filename)
    : m_filename(filename), m_optimization_read_backward(0), m_enabled_async_seek(false) {
	m_buffer = 0;
	m_buf_size = default_buf_size;
	reset_buffer(0);
}

BaseReader::~BaseReader() {
	if (m_buffer)
		delete[] m_buffer;
	if (m_is_open)
		close();
}

void BaseReader::reset_buffer(size_t position_buf) {
	m_position_buf = m_position_next_read = position_buf;
	m_read_pos = m_read_end = m_buffer;
	m_pending_async_seek = -1;
}

bool BaseReader::set_buffer_size(size_t size) {
	if (m_buffer) {
		debug_file_io("BaseReader::set_buffer_size(%zu) WARN: the buffer has been initialized(size=%zu)", //
		              size, m_buf_size);
		return false;
	}
	m_buffer = new char[size];
	m_buf_size = size;
	reset_buffer(0);
	debug_file_io("BaseReader::set_buffer_size(%zu)", m_buf_size);
	return true;
}

bool BaseReader::handle_async_seek() {
	if (m_pending_async_seek >= 0) {
		if (!do_seek(m_pending_async_seek))
			return false;
		m_pending_async_seek = -1;
	}
	return true;
}

bool BaseReader::open() {
	if (m_is_open && do_seek(0)) {
		debug_file_io("BaseReader::open() WARN: the reader has been opened. "
					"seeking to the zero position for backward compatibility.");
		reset_buffer(0);
		return true;
	}

	reset_buffer(0);
	bool ok = do_open();
	debug_file_io("BaseReader::open(%s, size=%zu, %s%s)", //
	              filename().c_str(), filesize(),         //
	              m_enabled_async_seek ? "async_seek, " : "", ok ? "OK" : "FAILED");
	m_is_open = ok;
	return ok;
}

void BaseReader::close() {
	if (!m_is_open) {
		debug_file_io("BaseReader::close() WARN: the reader has been closed");
		return;
	}

	debug_file_io("BaseReader::close(%s)", filename().c_str());
	do_close();
	reset_buffer(0);
	m_is_open = false;
}

bool BaseReader::skip_prefix(const std::string_view &prefix) {
	if (!underflow(prefix.length()))
		return false;
	for (char c : prefix)
		if (m_read_pos >= m_read_end || *m_read_pos++ != c) {
			m_read_pos = m_buffer;
			return false;
		}
	return true;
}

const char *BaseReader::peek_start(size_t length) {
	if (!underflow(length))
		return nullptr;
	return (m_read_pos + length <= m_read_end) ? m_read_pos : nullptr;
}

bool BaseReader::next_char(char& c)
{
	if (!underflow())
		return false;
	c = *m_read_pos++;
	return true;
}

bool BaseReader::peek(char& c)
{
	if (!underflow())
		return false;
	c = *m_read_pos;
	return true;
}

bool BaseReader::check_next_char(char c)
{
	if (!underflow())
		return false;
	if (*m_read_pos != c)
		return false;
	++m_read_pos;
	return true;
}


size_t BaseReader::consume_buffer(char *dest, size_t max_bytes) {
	size_t available_bytes_in_buffer = m_read_end - m_read_pos;
	if(available_bytes_in_buffer == 0)
		return 0;

	size_t bytes_from_buffer = std::min(max_bytes, available_bytes_in_buffer);
	memcpy(dest, m_read_pos, bytes_from_buffer);
	m_read_pos += bytes_from_buffer;
	return bytes_from_buffer;
}

size_t BaseReader::read(char *buffer, size_t size, uint8_t flag) {
	size_t bytes_remaining = size;
	size_t bytes_read = 0;

#define CONSUME_BUFFER_AND_UPDATE_STATE_VARIABLES()                                                                    \
	{                                                                                                                  \
		size_t bytes_from_buffer = consume_buffer(buffer, bytes_remaining);                                            \
		buffer += bytes_from_buffer;                                                                                   \
		bytes_read += bytes_from_buffer;                                                                               \
		bytes_remaining -= bytes_from_buffer;                                                                          \
	}

	if ((flag & BASEREADER_READ_FLAG_NO_BUF) || bytes_remaining >= m_buf_size) {
		debug_file_io("BaseReader::read(%zu, NO_BUF)", size);
		// too many bytes need to be read
		CONSUME_BUFFER_AND_UPDATE_STATE_VARIABLES();

		handle_async_seek();
		int sz = do_read(buffer, bytes_remaining);
		debug_do_read_result(bytes_remaining, sz);
		if (sz > 0) {
			m_position_next_read += sz;
			bytes_read += sz;
		}
	} else {
		debug_file_io("BaseReader::read(%zu, WITH_BUF)", size);
		while (bytes_remaining > 0) {
			if (!underflow())
				return bytes_read;
			CONSUME_BUFFER_AND_UPDATE_STATE_VARIABLES();
		}
	}
	return bytes_read;
}

/// ```
/// file: |-- ... ----------|--------------|---- ... --|
///      head               ↑              |           tail
///              m_position_next_read      |
///                  |<---->|              |
///                  |   ↑  |              |
///                  | read_back           |
///                  |<------ m_buffer --->|
/// ```
bool BaseReader::underflow() {
	if (m_read_pos >= m_read_end) {
		/// Read some bytes backward for optimization.
		size_t read_back = 0;

		if(m_position_next_read > 0) {
			// The optimization for reading the tail
			size_t len_to_the_tail = filesize() - m_position_next_read;
			if (len_to_the_tail < m_buf_size)
				read_back = m_buf_size - len_to_the_tail;
			// The optimization for backward many times
			else if (m_optimization_read_backward >= 2) {
				read_back = m_buf_size >> 1; // 1/2 of the buffer
				m_optimization_read_backward = 0;
			}

			if (read_back > 0) {
				read_back = std::min(read_back, m_position_next_read);

				size_t location = (size_t)m_position_next_read - read_back;
				debug_file_io("BaseReader::seek(read_back at %zu) -%zu", m_position_next_read, read_back);

				if (do_seek(location)) {
					reset_buffer(location);
				} else {
					// revert the `read_back` operation due to failed seek attempt
					// The reader may not support `do_seek`
					read_back = 0;
				}
			}
		}

		handle_async_seek();
		if (!m_buffer)
			set_buffer_size(default_buf_size);
		int sz = do_read(m_buffer, m_buf_size);
		debug_do_read_result(m_buf_size, sz);

		bool ok = sz > (0 + read_back);
		if(sz > 0) {
			m_position_buf = m_position_next_read;
			m_position_next_read += sz;
			m_read_end = m_buffer + sz;
			m_read_pos = m_buffer + read_back;
		}
		return ok;
	}
	return true;
}
bool BaseReader::underflow(size_t desired_bytes) {
	size_t remaining_bytes = remaining_bytes_in_buffer();
	if (remaining_bytes == 0 || !m_buffer)
		return underflow();
	if (remaining_bytes >= desired_bytes)
		return true; // The buffer is full, no need to be filled

	debug_file_io("underflow(desired_bytes=%zu, remaining_bytes=%zu)", desired_bytes, remaining_bytes);

	// With memcpy, the destination cannot overlap the source at all.
	// With memmove it can. This means that memmove might be very slightly slower than memcpy,
	// as it cannot make the same assumptions.
	// https://stackoverflow.com/questions/1201319
	memmove(m_buffer, m_read_pos, remaining_bytes);
	m_position_buf += m_read_pos - m_buffer;

	size_t bytes_to_read = m_buf_size - remaining_bytes;
	char *write_to = m_buffer + remaining_bytes;
	m_read_pos = m_buffer;
	m_read_end = write_to;

	handle_async_seek();
	int sz = do_read(write_to, bytes_to_read);
	debug_do_read_result(bytes_to_read, sz);
	if (sz <= 0)
		return false;
	m_position_next_read += sz;
	m_read_end = write_to + sz;
	return true;
}

xls::MemBuffer* BaseReader::read_all()
{
	if (!m_content.buffer)
	{
		if (!open())
			return nullptr;
		m_content.buffer = new char[m_content.size];
		if (do_read(m_content.buffer, m_content.size) < 0)
			m_content.clear();
		close();
	}
	return &m_content;
}

size_t BaseReader::tell() const {
	return m_position_buf + (m_buffer ? (m_read_pos - m_buffer) : 0);
}

bool BaseReader::seek(size_t location) {
	if (m_buffer && location >= m_position_buf && location < m_position_buf + current_buffer_size()) {
		// seek in the buffer
		size_t offset = location - m_position_buf;
		m_read_pos = m_buffer + offset;
		debug_file_io("BaseReader::seek(%zu) in_buffer=%zu", location, offset);
		return true;
	}

#ifdef DATADOCS_DEBUG_FILE_IO
	auto _current_location = tell();
	long _offset = location - _current_location;
	debug_file_io("BaseReader::seek(%zu) offset=%ld%s", location, _offset, //
	              m_enabled_async_seek ? " async" : "");
#endif
	if (location < m_position_buf)
		m_optimization_read_backward++;
	else
		m_optimization_read_backward = 0;

	if (m_enabled_async_seek) {
		reset_buffer(location);
		m_pending_async_seek = location;
		return true;
	}

	if (do_seek(location)) {
		reset_buffer(location);
		return true;
	}

	debug_file_io("BaseReader::seek ERROR");
	return false;
}

int BaseReader::pos_percent()
{
	if (m_content.size == 0)
		return 0;
	return (int)((double)tell() * 100 / m_content.size);
}

FileReader::FileReader(const std::string& filename, ClientContext &context) :
	BaseReader(filename),
	fs(FileSystem::GetFileSystem(context))
{}

bool FileReader::do_open()
{
	file_handle = fs.OpenFile(m_filename.data(), FileFlags::FILE_FLAGS_READ);
	idx_t size = file_handle->GetFileSize();
	m_content.size = size < 0 ? 0 : size;
	if (file_handle->CanSeek()) {
		file_handle->Reset();
	}
	return true;
}

void FileReader::do_close()
{
	if (file_handle)
	{
		file_handle->Close();
		file_handle.reset();
	}
}

int FileReader::do_read(char* buffer, size_t size)
{
	return file_handle->Read(buffer, size);
}

bool FileReader::do_seek(size_t location) {
	if (file_handle) {
		file_handle->Seek(location);
		return true;
	}
	return false;
}

} // namespace duckdb
