#include <cstdlib>
#include <cstring>
#include <utility>
#include <vector>
#include <unordered_set>

#include "xls/read_xlsx.h"
#include "xls/zip_memory.h"

#include "xlsx/xlsxio_read.h"

namespace xls {

template<class Parser>
struct BaseXMLParser
{
	using ParserType = Parser;

	bool parse(ZIPFILETYPE* zip, const XML_Char* filename)
	{
		return expat_process_zip_file(zip, filename, start, nullptr, nullptr, this, &m_xmlparser) == 0;
	}

	bool init()
	{
		m_xmlparser = XML_ParserCreate(nullptr);
		if (m_xmlparser == nullptr)
			return false;
		XML_SetUserData(m_xmlparser, this);
		XML_SetElementHandler(m_xmlparser, start, nullptr);
		return true;
	}

	static void XMLCALL start(void* userdata, const XML_Char* name, const XML_Char** atts)
	{
		Parser::root::check(userdata, name, atts);
	}

	static constexpr XML_EndElementHandler end = nullptr;
	XML_Parser m_xmlparser = nullptr;
};

template<class T, class Parent>
struct Tag
{
	using ParserType = typename Parent::ParserType;
	using ParentTag = Parent;

	static bool check(void* userdata, const XML_Char* name, const XML_Char** atts)
	{
		if (std::strcmp(name, T::tag_name) != 0)
			return false;
		XML_SetElementHandler(static_cast<ParserType*>(userdata)->m_xmlparser, T::start, T::end);
		return true;
	}

	static void XMLCALL end(void* userdata, const XML_Char* name)
	{
		if (std::strcmp(name, T::tag_name) == 0)
			XML_SetElementHandler(static_cast<ParserType*>(userdata)->m_xmlparser, Parent::start, Parent::end);
	}

	static const XML_Char* attr_str(const XML_Char** atts, const XML_Char* name)
	{
		if (atts)
			for (; *atts; atts += 2)
				if (std::strcmp(*atts, name) == 0)
					return atts[1];
		return nullptr;
	}

	static bool attr_int(const XML_Char** atts, const XML_Char* name, int& value)
	{
		const XML_Char* s = attr_str(atts, name);
		if (!s)
			return false;
		value = std::atoi(s);
		return true;
	}

	static bool attr_bool(const XML_Char** atts, const XML_Char* name, bool& value)
	{
		const XML_Char* s = attr_str(atts, name);
		if (!s)
			return false;
		if (std::strcmp(s, "1") == 0 || std::strcmp(s, "true") == 0 || std::strcmp(s, "on") == 0)
		{
			value = true;
			return true;
		}
		if (std::strcmp(s, "0") == 0 || std::strcmp(s, "false") == 0 || std::strcmp(s, "off") == 0)
		{
			value = false;
			return true;
		}
		return false;
	}
};


struct StylesParser : BaseXMLParser<StylesParser>
{
	StylesParser(std::vector<bool>& date_formats) : m_date_formats(date_formats) {}

	struct root : Tag<root, StylesParser>
	{
		static constexpr const char* tag_name = "styleSheet";
		static void XMLCALL start(void* userdata, const XML_Char* name, const XML_Char** atts)
		{
			numFmts::check(userdata, name, atts) || cellXfs::check(userdata, name, atts);
		}
	};

	struct numFmts: Tag<numFmts, root>
	{
		static constexpr const char* tag_name = "numFmts";
		static void XMLCALL start(void* userdata, const XML_Char* name, const XML_Char** atts)
		{
			int num;
			const XML_Char* fmt;
			if (std::strcmp(name, "numFmt") == 0 && attr_int(atts, "numFmtId", num) && (fmt = attr_str(atts, "formatCode")) && is_date_format(fmt))
				static_cast<ParserType*>(userdata)->m_date_ids.insert(num);
		}
	};

	struct cellXfs : Tag<cellXfs, root>
	{
		static constexpr const char* tag_name = "cellXfs";
		static void XMLCALL start(void* userdata, const XML_Char* name, const XML_Char** atts)
		{
			ParserType* th = static_cast<ParserType*>(userdata);
			int num;
			if (std::strcmp(name, "xf") == 0)
				th->m_date_formats.push_back(attr_int(atts, "numFmtId", num) && (
					num >= 14 && num < 23 || num >= 45 && num < 48 || th->m_date_ids.find(num) != th->m_date_ids.end()
					));
		}
	};

	std::vector<bool>& m_date_formats;
	std::unordered_set<int> m_date_ids;
};

struct SSParser : BaseXMLParser<SSParser>
{
	SSParser(std::vector<std::string>& ss) : m_sharedstrings(ss) {}

	struct root : Tag<root, SSParser>
	{
		static constexpr const char* tag_name = "sst";
		static bool check(void* userdata, const XML_Char* name, const XML_Char** atts)
		{
			if (std::strcmp(name, tag_name) == 0)
			{
				ParserType* th = static_cast<ParserType*>(userdata);
				XML_SetElementHandler(th->m_xmlparser, start, end);
				int count;
				if (attr_int(atts, "uniqueCount", count))
					th->m_sharedstrings.reserve(count);
				return true;
			}
			return false;
		}

		static void XMLCALL start(void* userdata, const XML_Char* name, const XML_Char** atts)
		{
			if (si::check(userdata, name, atts))
				static_cast<ParserType*>(userdata)->m_sharedstrings.emplace_back();
		}
	};

	struct si: Tag<si, root>
	{
		static constexpr const char* tag_name = "si";
		static void XMLCALL start(void* userdata, const XML_Char* name, const XML_Char** atts)
		{
			ParserType* th = static_cast<ParserType*>(userdata);
			if (std::strcmp(name, "t") == 0)
			{
				XML_SetElementHandler(th->m_xmlparser, nullptr, string_end);
				XML_SetCharacterDataHandler(th->m_xmlparser, content);
			}
			else if (std::strcmp(name, "rPh") == 0)
			{
				th->skiplevel = 1;
				XML_SetElementHandler(th->m_xmlparser, skip_start, skip_end);
				XML_SetCharacterDataHandler(th->m_xmlparser, nullptr);
			}
		}

		static void XMLCALL content(void* userdata, const XML_Char* buf, int buflen)
		{
			static_cast<ParserType*>(userdata)->m_sharedstrings.back().append(buf, buflen);
		}

		static void XMLCALL string_end(void* userdata, const XML_Char* name)
		{
			ParserType* th = static_cast<ParserType*>(userdata);
			XML_SetElementHandler(th->m_xmlparser, start, end);
			XML_SetCharacterDataHandler(th->m_xmlparser, nullptr);
		}

		static void XMLCALL skip_start(void* userdata, const XML_Char* name, const XML_Char** atts)
		{
			++static_cast<ParserType*>(userdata)->skiplevel;
		}

		static void XMLCALL skip_end(void* userdata, const XML_Char* name)
		{
			ParserType* th = static_cast<ParserType*>(userdata);
			if (--th->skiplevel <= 0)
				XML_SetElementHandler(th->m_xmlparser, start, end);
		}
	};

	std::vector<std::string>& m_sharedstrings;
	int skiplevel = 0;
};

struct WSParser : BaseXMLParser<WSParser>
{
	struct root : Tag<root, WSParser>
	{
		static constexpr const char* tag_name = "worksheet";
		static void XMLCALL start(void* userdata, const XML_Char* name, const XML_Char** atts)
		{
			const XML_Char* ref;
			if (!sheetData::check(userdata, name, atts) && std::strcmp(name, "dimension") == 0 && (ref = attr_str(atts, "ref")))
			{
				while (*ref && *ref != ':') ++ref;
				int nrows = get_row_nr(ref + 1);
				if (nrows > 0)
					static_cast<ParserType*>(userdata)->nrows = nrows;
			}
		}
	};

	struct sheetData: Tag<sheetData, root>
	{
		static constexpr const char* tag_name = "sheetData";
		static void XMLCALL start(void* userdata, const XML_Char* name, const XML_Char** atts)
		{
			if (row::check(userdata, name, atts))
			{
				ParserType* th = static_cast<ParserType*>(userdata);
				++th->cell_row;
				th->cell_col = 0;
				XML_StopParser(th->m_xmlparser, XML_TRUE);
			}
		}
	};

	struct row: Tag<row, sheetData>
	{
		static constexpr const char* tag_name = "row";
		static void XMLCALL start(void* userdata, const XML_Char* name, const XML_Char** atts)
		{
			if (c::check(userdata, name, atts))
			{
				ParserType* th = static_cast<ParserType*>(userdata);
				const XML_Char* t = attr_str(atts, "r");
				int cellcolnr = get_col_nr(t);
				if (th->cell_col == 0)
				{
					int cellrownr = get_row_nr(t);
					if (cellrownr)
						th->cell_row = cellrownr;
				}
				if (cellcolnr)
					th->cell_col = cellcolnr;
				else
					++th->cell_col;
				if ((t = attr_str(atts, "t")) != nullptr)
					th->celldata_type = t[1] ? '\0' : t[0];
				else
					th->celldata_type = 'n';
				if (th->celldata_type == 'n')
				{
					if ((t = attr_str(atts, "s")) != nullptr)
						th->celldata_style = atoi(t);
					else
						th->celldata_style = -1;
				}
				if (th->celldata_type == 's')
				{
					th->ss_index = 0;
					th->content_func = c::content_ss;
				}
				else
				{
					th->content_func = c::content;
					th->celldata.clear();
				}
			}
		}

		static void XMLCALL end(void* userdata, const XML_Char* name)
		{
			if (std::strcmp(name, tag_name) == 0)
			{
				XML_SetElementHandler(static_cast<ParserType*>(userdata)->m_xmlparser, ParentTag::start, ParentTag::end);
				ParserType* th = static_cast<ParserType*>(userdata);
				th->cell_col = -1;
				XML_StopParser(th->m_xmlparser, XML_TRUE);
			}
		}
	};

	struct c: Tag<c, row>
	{
		static constexpr const char* tag_name = "c";
		static void XMLCALL start(void* userdata, const XML_Char* name, const XML_Char** atts)
		{
			ParserType* th = static_cast<ParserType*>(userdata);
			if (std::strcmp(name, "v") == 0 || std::strcmp(name, "t") == 0)
			{
				XML_SetElementHandler(th->m_xmlparser, nullptr, value_end);
				XML_SetCharacterDataHandler(th->m_xmlparser, th->content_func);
			}
			else if (std::strcmp(name, "rPh") == 0 || std::strcmp(name, "extLst") == 0)
			{
				th->skiplevel = 1;
				XML_SetElementHandler(th->m_xmlparser, skip_start, skip_end);
				XML_SetCharacterDataHandler(th->m_xmlparser, nullptr);
			}
		}

		static void XMLCALL value_end(void* userdata, const XML_Char* name)
		{
			ParserType* th = static_cast<ParserType*>(userdata);
			XML_SetElementHandler(th->m_xmlparser, start, end);
			XML_SetCharacterDataHandler(th->m_xmlparser, nullptr);
		}

		static void XMLCALL content(void* userdata, const XML_Char* buf, int buflen)
		{
			static_cast<ParserType*>(userdata)->celldata.append(buf, buflen);
		}

		static void XMLCALL content_ss(void* userdata, const XML_Char* buf, int buflen)
		{
			ParserType* th = static_cast<ParserType*>(userdata);
			size_t n = th->ss_index;
			for (size_t i = 0; i < buflen; ++i)
			{
				size_t d = size_t(buf[i] - '0');
				if (d <= 9)
					n = n * 10 + d;
			}
			th->ss_index = n;
		}

		static void XMLCALL skip_start(void* userdata, const XML_Char* name, const XML_Char** atts)
		{
			++static_cast<ParserType*>(userdata)->skiplevel;
		}

		static void XMLCALL skip_end(void* userdata, const XML_Char* name)
		{
			ParserType* th = static_cast<ParserType*>(userdata);
			if (--th->skiplevel <= 0)
				XML_SetElementHandler(th->m_xmlparser, start, end);
		}

		static void XMLCALL end(void* userdata, const XML_Char* name)
		{
			if (std::strcmp(name, tag_name) == 0)
			{
				XML_SetElementHandler(static_cast<ParserType*>(userdata)->m_xmlparser, ParentTag::start, ParentTag::end);
				ParserType* th = static_cast<ParserType*>(userdata);
				XML_SetCharacterDataHandler(th->m_xmlparser, nullptr);
				XML_StopParser(th->m_xmlparser, XML_TRUE);
			}
		}
	};

	int cell_row = 0;
	int cell_col = 0; // 0 at the beginning of row, -1 at the end of row
	std::string celldata;
	char celldata_type = 0; // 's': shared string, 'n': number, 'b': bool, otherwise string
	int celldata_style = 0;
	void (*content_func)(void* userdata, const XML_Char* buf, int buflen) = nullptr;
	size_t ss_index = 0;
	int nrows = 0;
	int skiplevel = 0;
};


struct WorkSheetX::Impl
{
	Impl(WorkBookX::Impl* wb);
	~Impl();
	bool process(const std::string& name);
	bool next_row();
	bool next_cell(CellValue& value);

	xlsxioreader m_handle = nullptr;
	ZIPFILEENTRYTYPE* m_zipfile = nullptr;
	std::vector<std::string> m_sharedstrings;
	int m_expected_row = 0;
	int m_expected_col = 1;
	int m_total_cols = 0;
	enum class ParserState { EOR, NeedData, HaveData } m_parser_state = ParserState::EOR;

	WorkBookX::Impl* m_wb;
	bool m_reading_row;
	std::vector<bool> m_date_formats;
	WSParser m_parser;
};

struct WorkBookX::Impl
{
public:
	Impl(unzFile zip);
	~Impl();
	void read_workbook();

	xlsxio_read_struct wb;
	std::vector<std::string> m_sheets;
	int m_date_offset;
};


WorkSheetX::Impl::Impl(WorkBookX::Impl* wb) :
	m_wb(wb),
	m_reading_row(false)
{
	m_handle = &wb->wb;
}

WorkSheetX::Impl::~Impl()
{
	if (m_parser.m_xmlparser)
		XML_ParserFree(m_parser.m_xmlparser);

	if (m_zipfile)
		unzCloseCurrentFile(m_zipfile);
}

bool WorkSheetX::Impl::process(const std::string& name)
{
	main_sheet_get_rels_callback_data cb = {};
	cb.sheetname = name.data();
	xlsxioread_preprocess(m_handle, &cb);

	if (cb.sharedstringsfile && cb.sharedstringsfile[0])
		SSParser(m_sharedstrings).parse(m_handle->zip, cb.sharedstringsfile);

	StylesParser(m_date_formats).parse(m_handle->zip, cb.stylesfile);

	int result = 0;
	m_parser.nrows = -1;

	if ((m_zipfile = XML_Char_openzip(m_handle->zip, cb.sheetfile, 0)) == nullptr)
		result = 1;
	else if (!m_parser.init())
		result = 2;

	std::free(cb.basepath);
	std::free(cb.sheetrelid);
	std::free(cb.sheetfile);
	std::free(cb.sharedstringsfile);
	std::free(cb.stylesfile);
	return result == 0;
}

bool WorkSheetX::Impl::next_row()
{
	if (m_parser_state != ParserState::HaveData)
	{
		do {
			if (expat_process_zip_file_resume(m_zipfile, m_parser.m_xmlparser) != XML_STATUS_SUSPENDED)
				return false;
		} while (m_parser.cell_col != 0);
		m_parser_state = ParserState::NeedData;
	}
	++m_expected_row;
	m_expected_col = 1;
	return true;
}

bool WorkSheetX::Impl::next_cell(CellValue& value)
{
	if (m_parser_state == ParserState::NeedData)
	{
		if (expat_process_zip_file_resume(m_zipfile, m_parser.m_xmlparser) != XML_STATUS_SUSPENDED)
			return false;
		m_parser_state = m_parser.cell_col < 0 ? ParserState::EOR : ParserState::HaveData;
	}
	if (m_parser_state == ParserState::EOR || m_parser.cell_row > m_expected_row)
	{
		if (m_expected_col <= m_total_cols)
		{
			++m_expected_col;
			value.type = CellType::Empty;
			return true;
		}
		return false;
	}
	if (m_parser.cell_col > m_expected_col)
	{
		++m_expected_col;
		value.type = CellType::Empty;
		return true;
	}
	if (m_parser.cell_col > m_total_cols)
		m_total_cols = m_parser.cell_col;
	++m_expected_col;
	m_parser_state = ParserState::NeedData;

	if (m_parser.celldata_type == 's')
	{
		if (m_parser.ss_index < m_sharedstrings.size())
			value = m_sharedstrings[m_parser.ss_index];
		else
			value.type = CellType::Empty;
	}
	else if (m_parser.celldata.empty())
		value.type = CellType::Empty;
	else
	{
		switch (m_parser.celldata_type)
		{
		case 'n':
			{
				int style = m_parser.celldata_style;
				bool is_date = style >= 0 && style < m_date_formats.size() && m_date_formats[style];
				parse_number(m_parser.celldata.data(), value, is_date);
				if (is_date)
					value.value_d += m_wb->m_date_offset;
			}
			break;
		case 'b':
			value = m_parser.celldata[0] != '0';
			break;
		default:
			value.type = CellType::String;
			value.value_s = std::move(m_parser.celldata);
			break;
		}
	}
	return true;
}

WorkSheetX::WorkSheetX() = default;
WorkSheetX::WorkSheetX(WorkSheetX&&) = default;
WorkSheetX& WorkSheetX::operator = (WorkSheetX&&) = default;
WorkSheetX::~WorkSheetX() = default;
WorkSheetX::WorkSheetX(WorkSheetX::Impl* ws) : d(ws) {}

void WorkSheetX::close()
{
	d.reset();
}

WorkSheetX::operator bool()
{
	return static_cast<bool>(d);
}

int WorkSheetX::nrows()
{
	return d ? d->m_parser.nrows : -1;
}

bool WorkSheetX::next_row()
{
	if (!d)
		return false;
	if (d->m_reading_row)
	{
		CellValue value;
		while(d->next_cell(value));
	}
	else
		d->m_reading_row = true;
	return d->next_row();
}

bool WorkSheetX::next_cell(CellValue& value)
{
	if (!d)
		return false;
	if (d->next_cell(value))
		return true;
	d->m_reading_row = false;
	return false;
}


struct WBParser : BaseXMLParser<WBParser>
{
	WBParser(WorkBookX::Impl& wb) : m_wb(wb) {}

	struct root : Tag<root, WBParser>
	{
		static bool check(void* userdata, const XML_Char* name, const XML_Char** atts)
		{
			if (std::strcmp(name, "sheet") == 0)
			{
				const XML_Char* sheet_name = attr_str(atts, "name");
				if (sheet_name)
					static_cast<ParserType*>(userdata)->m_wb.m_sheets.emplace_back(sheet_name);
			}
			else if (std::strcmp(name, "workbookPr") == 0)
			{
				bool is1904;
				if (attr_bool(atts, "date1904", is1904) && is1904)
					static_cast<ParserType*>(userdata)->m_wb.m_date_offset = 1462;
			}
			return false;
		}
	};

	WorkBookX::Impl& m_wb;
};

WorkBookX::Impl::Impl(unzFile zip) :
	wb(),
	m_date_offset(0)
{
	wb.zip = (ZIPFILETYPE*)zip;
}

WorkBookX::Impl::~Impl()
{
	unzClose(wb.zip);
}

void WorkBookX::Impl::read_workbook()
{
	XML_Char* wb_file = xlsxioread_workbook_file(&wb);
	WBParser(*this).parse(wb.zip, wb_file);
	std::free(wb_file);
}

WorkBookX::WorkBookX() = default;
WorkBookX::WorkBookX(WorkBookX&&) = default;
WorkBookX& WorkBookX::operator = (WorkBookX&&) = default;
WorkBookX::~WorkBookX() = default;

bool WorkBookX::open(duckdb::BaseReader *reader)
{
	unzFile zip = unzOpenFS(reader);
	if (!zip)
		return false;
	d = std::make_unique<Impl>(zip);
	return true;
}

bool WorkBookX::open(MemBuffer* buffer)
{
	unzFile zip = unzOpenMemory(buffer);
	if (!zip)
		return false;
	d = std::make_unique<Impl>(zip);
	return true;
}

void WorkBookX::close()
{
	d.reset();
}

WorkBookX::operator bool()
{
	return static_cast<bool>(d);
}

size_t WorkBookX::sheet_count()
{
	if (!d)
		return 0;
	if (d->m_sheets.empty())
		d->read_workbook();
	return d->m_sheets.size();
}

std::string WorkBookX::sheet_name(size_t sheet_number)
{
	return sheet_number < sheet_count() ? d->m_sheets[sheet_number] : std::string();
}

WorkSheetX WorkBookX::sheet(size_t sheet_number)
{
	if (sheet_number < sheet_count())
	{
		WorkSheetX::Impl* ws = new WorkSheetX::Impl(d.get());
		if (ws->process(d->m_sheets[sheet_number]))
			return WorkSheetX(ws);
		delete ws;
	}
	return WorkSheetX();
}

}