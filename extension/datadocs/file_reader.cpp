#include <algorithm>
#include <string.h>

#include "file_reader.h"

namespace duckdb {

BaseReader::BaseReader(const std::string& filename) :
	m_filename(filename),
	m_cnt_read(0)
{
	m_read_pos = m_read_end = m_buffer + buf_size;
}

BaseReader::~BaseReader()
{}

bool BaseReader::open()
{
	m_read_pos = m_read_end = m_buffer + buf_size;
	m_cnt_read = 0;
	return do_open();
}

void BaseReader::close()
{
	do_close();
	m_read_pos = m_read_end = m_buffer + buf_size;
	m_cnt_read = 0;
}

void BaseReader::skip_prefix(const std::string_view& prefix)
{
	if (!underflow())
		return;
	for (char c : prefix)
		if (m_read_pos >= m_read_end || *m_read_pos++ != c)
		{
			m_read_pos = m_buffer;
			return;
		}
}

const char* BaseReader::peek_start(size_t length)
{
	if (!underflow())
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

size_t BaseReader::read(char* buffer, size_t size)
{
	size_t from_buffer = 0;
	size_t in_buffer = m_read_end - m_read_pos;
	if (in_buffer > 0)
	{
		from_buffer = std::min(size, in_buffer);
		memcpy(buffer, m_read_pos, from_buffer);
		m_read_pos += from_buffer;
		buffer += from_buffer;
		size -= from_buffer;
	}
	int from_file = do_read(buffer, size);
	if (from_file < 0)
		return 0;
	m_cnt_read += from_file;
	return from_buffer + (size_t)from_file;
}

bool BaseReader::underflow()
{
	if (m_read_pos >= m_read_end)
	{
		int sz = do_read(m_buffer, buf_size);
		if (sz <= 0)
			return false;
		m_cnt_read += sz;
		m_read_end = m_buffer + sz;
		m_read_pos = m_buffer;
	}
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

size_t BaseReader::tell() const
{
	return m_cnt_read - (m_read_end - m_read_pos);
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

}
