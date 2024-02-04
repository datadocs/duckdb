#include <cstring>
#include <utility>
#include <string_view>
#include <regex>

#include <contrib/minizip/unzip.h>

#include "zip_reader.h"
#include "utility.h"
#include "xls/zip_memory.h"

using namespace std::literals;

namespace duckdb {

static const std::regex _re_zip_hidden_files (R"((/|^)((\.DS_Store|desktop\.ini|thumbs\.db)$)|(__MACOSX|\.vscode|\.idea|\.coverage)/|~\$)",
	std::regex::nosubs | std::regex::icase);

class ZIPReader : public BaseReader
{
public:
	ZIPReader(unzFile zip, const std::string& filename) :
		BaseReader(filename),
		m_zip(zip)
	{}

	virtual bool is_file() override { return false; }

protected:
	virtual bool do_open() override
	{
		m_content.size = 0;
		unz_file_info64 file_info;
		if (unzLocateFile(m_zip, m_filename.data(), 0) != UNZ_OK ||
			unzGetCurrentFileInfo64(m_zip, &file_info, nullptr, 0, nullptr, 0, nullptr, 0) != UNZ_OK ||
			unzOpenCurrentFile(m_zip) != UNZ_OK)
			return false;
		m_content.size = file_info.uncompressed_size;
		return true;
	}

	virtual void do_close() override
	{
		unzCloseCurrentFile(m_zip);
	}

	virtual int do_read(char* buffer, size_t size) override
	{
		return unzReadCurrentFile(m_zip, buffer, size);
	}

	unzFile m_zip;
};

ZIPParser::ZIPParser(std::shared_ptr<BaseReader> reader) :
	m_reader(reader),
	m_zip(nullptr)
{}

ZIPParser::~ZIPParser()
{
	close();
}

bool ZIPParser::do_infer_schema()
{
	if (m_parser)
		return m_parser->do_infer_schema();
	return false;
}

Schema* ZIPParser::get_schema()
{
	if (m_parser)
		return m_parser->get_schema();
	return &m_invalid_schema;
}

bool ZIPParser::open()
{
	if (m_parser)
		return m_parser->open();
	return false;
}

void ZIPParser::close()
{
	m_parser.reset();
	unzClose(m_zip);
	m_zip = nullptr;
}

int ZIPParser::get_percent_complete()
{
	if (m_parser)
		return m_parser->get_percent_complete();
	return 0;
}

size_t ZIPParser::get_sheet_count()
{
	if (m_parser)
		return m_parser->get_sheet_count();
	return 0;
}

std::vector<std::string> ZIPParser::get_sheet_names()
{
	if (m_parser)
		return m_parser->get_sheet_names();
	return std::vector<std::string>();
}

bool ZIPParser::select_sheet(const std::string_view &sheet_name)
{
	if (m_parser)
		return m_parser->select_sheet(sheet_name);
	return false;
}

bool ZIPParser::select_sheet(size_t sheet_number)
{
	if (m_parser)
		return m_parser->select_sheet(sheet_number);
	return false;
}

size_t ZIPParser::get_file_count()
{
	if (m_parser)
		return m_parser->get_file_count();
	do_open_zip();
	return m_files.size();
}

std::vector<std::string> ZIPParser::get_file_names()
{
	if (m_parser)
		return m_parser->get_file_names();
	do_open_zip();
	return m_files;
}

bool ZIPParser::select_file(const std::string_view &file_name)
{
	if (m_parser)
		return m_parser->select_file(file_name);
	size_t file_cnt = get_file_count();
	for (size_t i = 0; i < file_cnt; ++i)
		if (file_name == m_files[i])
			return select_file(i);
	return false;
}

bool ZIPParser::select_file(size_t file_number)
{
	if (m_parser)
		return m_parser->select_file(file_number);
	if (file_number >= get_file_count())
		return false;
	m_parser.reset(get_parser_from_reader(std::make_shared<ZIPReader>(m_zip, m_files[file_number])));
	return static_cast<bool>(m_parser);
}

bool ZIPParser::do_open_zip()
{
	if (m_zip)
		return true;
	m_files.clear();
	if (m_reader->is_file())
		m_zip = xls::unzOpenFS(m_reader.get());
	else
		m_zip = xls::unzOpenMemory(m_reader->read_all());
	if (!m_zip)
		return false;
	if (unzGoToFirstFile(m_zip) != UNZ_OK)
		return false;
	unz_file_info64 file_info;
	do {
		if (unzGetCurrentFileInfo64(m_zip, &file_info, nullptr, 0, nullptr, 0, nullptr, 0) != UNZ_OK)
			return false;
		if (file_info.uncompressed_size > 0)
		{
			std::string s;
			s.resize(file_info.size_filename);
			if (unzGetCurrentFileInfo64(m_zip, nullptr, s.data(), s.size(), nullptr, 0, nullptr, 0) != UNZ_OK)
				return false;
			if (!std::regex_search(s, _re_zip_hidden_files))
				m_files.push_back(std::move(s));
		}
	} while (unzGoToNextFile(m_zip) == UNZ_OK);
	return true;
}

void ZIPParser::BuildColumns() {
	if (m_parser)
		m_parser->BuildColumns();
}

void ZIPParser::BindSchema(std::vector<LogicalType> &return_types, std::vector<string> &names) {
	if (m_parser)
		m_parser->BindSchema(return_types, names);
}

idx_t ZIPParser::FillChunk(DataChunk &output)
{
	if (m_parser)
		return m_parser->FillChunk(output);
	return 0;
}

}