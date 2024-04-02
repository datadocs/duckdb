#include "file_reader.h"

#include "debug.h"

#include <algorithm>
#include <string.h>

namespace duckdb {

BaseReader::BaseReader(const std::string &filename)
    : m_filename(filename), seek_before_next_read(-1), m_position_buf(0), m_position(0) {
	reset_buffer();
}

BaseReader::~BaseReader() {
}

void BaseReader::reset_buffer() {
	m_read_pos = m_read_end = m_buffer;
}

bool BaseReader::open() {
	reset_buffer();
	m_position = 0;
	return do_open();
}

void BaseReader::close() {
	do_close();
	reset_buffer();
	m_position = 0;
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

size_t BaseReader::read(char *buffer, size_t size) {
	size_t bytes_remaining = size;
	size_t bytes_read = 0;

	while (bytes_remaining > 0) {
		if (!underflow())
			return bytes_read;

		size_t available_bytes_in_buffer = m_read_end - m_read_pos;
		size_t bytes_from_buffer = std::min(bytes_remaining, available_bytes_in_buffer);
		memcpy(buffer, m_read_pos, bytes_from_buffer);
		buffer += bytes_from_buffer;
		bytes_read += bytes_from_buffer;
		bytes_remaining -= bytes_from_buffer;

		m_read_pos += bytes_from_buffer;
	}
	return bytes_read;
}

bool BaseReader::underflow() {
	if (m_read_pos >= m_read_end) {
		int sz = do_read(m_buffer, buf_size);
		debug_do_read_result(buf_size, sz);
		if (sz <= 0)
			return false;
		m_position += sz;
		m_read_end = m_buffer + sz;
		m_read_pos = m_buffer;
	}
	return true;
}
bool BaseReader::underflow(size_t desired_bytes) {
	size_t remaining_bytes = remaining_bytes_in_buffer();
	if (remaining_bytes == 0)
		return underflow();
	if (remaining_bytes >= desired_bytes)
		return true; // The buffer is full, no need to be filled

	debug_file_io("underflow_force(remaining_bytes=%zu)", remaining_bytes);

	// With memcpy, the destination cannot overlap the source at all.
	// With memmove it can. This means that memmove might be very slightly slower than memcpy,
	// as it cannot make the same assumptions.
	// https://stackoverflow.com/questions/1201319
	memmove(m_buffer, m_read_pos, remaining_bytes);

	size_t bytes_to_read = buf_size - remaining_bytes;
	char *write_to = m_buffer + remaining_bytes;
	m_read_pos = m_buffer;
	m_read_end = write_to;

	int sz = do_read(write_to, bytes_to_read);
	debug_do_read_result(bytes_to_read, sz);
	if (sz <= 0)
		return false;
	m_position += sz;
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
	return m_position - (m_read_end - m_read_pos);
}

bool BaseReader::seek(size_t location) {
	debug_file_io("BaseReader::seek(%zu)", location);
	if (do_seek(location)) {
		m_position = location;
		reset_buffer();
		return true;
	}
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
