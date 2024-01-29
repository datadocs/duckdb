#include <cctype>
#include <utility>
#include <array>
#include <unordered_map>
#include <algorithm>
#include <regex>

#include <unicode/ucnv.h>
#include <unicode/ucsdet.h>

#include "duckdb/common/string_util.hpp"

#include "csv_reader.h"
#include "utility.h"

using namespace std::literals;

namespace duckdb {

enum class UnicodeType { INVALID, ASCII, UNICODE2 };
enum class UnicodeInvalidReason { BYTE_MISMATCH, INVALID_UNICODE };

class Utf8Proc {
public:
	static UnicodeType Analyze(const char *s, size_t len, UnicodeInvalidReason *invalid_reason = nullptr, size_t *invalid_pos = nullptr);
};

static constexpr idx_t BUFFER_SIZE = 1024 * 1024;
//static constexpr idx_t BUFFER_SIZE = 32;

namespace {

struct CUCharsetDetector
{
	CUCharsetDetector(UCharsetDetector* pointer) : m_pointer(pointer) {}
	~CUCharsetDetector() { ucsdet_close(m_pointer); }
	CUCharsetDetector(const CUCharsetDetector&) = delete;
	CUCharsetDetector& operator = (const CUCharsetDetector&) = delete;
	operator UCharsetDetector* () { return m_pointer; }
	UCharsetDetector* m_pointer;
};

struct CUConverter
{
	CUConverter(UConverter* pointer) : m_pointer(pointer) {}
	~CUConverter() { ucnv_close(m_pointer); }
	CUConverter(CUConverter&& other) : m_pointer(nullptr)
	{
		*this = std::move(other);
	}
	CUConverter& operator = (CUConverter&& other)
	{
		if (other.m_pointer != m_pointer)
			std::swap(m_pointer, other.m_pointer);
		return *this;
	}
	operator UConverter* () { return m_pointer; }
	UConverter* m_pointer;
};

class IngestColCSVInfer : public IngestColBase {
public:
	IngestColCSVInfer(std::vector<RowRawNumbered> &rows, idx_t &cur_row)
	    : IngestColBase("", cur_row), i_row(cur_row), rows(rows) {
	}

	bool Write(string_t v) override {
		rows[i_row].emplace_back(v.GetString());
		return true;
	}

	bool Write(int64_t v) override {
		rows[i_row].row_no = v;
		if (i_row >= rows.size() - 1) {
			i_row = STANDARD_VECTOR_SIZE;
		}
		return true;
	}

private:
	idx_t &i_row;
	std::vector<RowRawNumbered> &rows;
};

}

class ucvt_streambuf
{
public:
	static constexpr size_t read_buf_size = 4096;
	static constexpr size_t cnv_buf_size = 4096;
	static constexpr size_t pivot_buf_size = 4096;

	ucvt_streambuf(std::shared_ptr<BaseReader> reader, CUConverter&& ucnv_from, CUConverter&& ucnv_to) :
		m_reader(reader),
		m_ucnv_from(std::move(ucnv_from)),
		m_ucnv_to(std::move(ucnv_to)),
		m_data_in_converter(false)
	{
		m_read_pos = m_read_end = m_read_buf + read_buf_size;
		m_pivot_source = m_pivot_target = m_pivot_buf;
		m_cnv_pos = m_cnv_end = m_cnv_buf + cnv_buf_size;
	}

	bool get(char& c)
	{
		if (m_cnv_pos >= m_cnv_end && !underflow())
			return false;
		c = *m_cnv_pos++;
		return true;
	}

	bool check_next_char(char c)
	{
		if (m_cnv_pos >= m_cnv_end && !underflow())
			return false;
		if (*m_cnv_pos != c)
			return false;
		++m_cnv_pos;
		return true;
	}

	bool underflow()
	{
		if (m_read_pos >= m_read_end)
		{
			size_t sz = m_reader->read(m_read_buf, read_buf_size);
			if (sz == 0 && !m_data_in_converter)
				return false;
			m_read_end = m_read_buf + sz;
			m_read_pos = m_read_buf;
		}
		UErrorCode ustatus = U_ZERO_ERROR;
		m_cnv_pos = m_cnv_end = m_cnv_buf;
		ucnv_convertEx(m_ucnv_to, m_ucnv_from, &m_cnv_end, m_cnv_buf + cnv_buf_size, &m_read_pos, m_read_end, m_pivot_buf, &m_pivot_source, &m_pivot_target, m_pivot_buf + pivot_buf_size, false, false, &ustatus);
		m_data_in_converter = ustatus == U_BUFFER_OVERFLOW_ERROR;
		return U_SUCCESS(ustatus) || ustatus == U_TRUNCATED_CHAR_FOUND || m_data_in_converter;
	}

private:
	std::shared_ptr<BaseReader> m_reader;
	const char* m_read_pos;
	const char* m_read_end;
	const char* m_cnv_pos;
	char* m_cnv_end;
	UChar* m_pivot_source;
	UChar* m_pivot_target;
	bool m_data_in_converter;
	CUConverter m_ucnv_from;
	CUConverter m_ucnv_to;
	char m_read_buf[read_buf_size];
	char m_cnv_buf[cnv_buf_size];
	UChar m_pivot_buf[pivot_buf_size];
};

const size_t SAMPLE_SIZE = 1024 * 1024;
static const std::regex _re_line_terminators(R"(\x02\n|\r\n|\n|\r)"); // max 2 chars
static const std::regex _re_universal_newlines(R"(\r\n|\n|\r)"); // these are interchangeable
static const std::array<std::string, 4> _comment_chars { "#", "//", "/*", "*/" }; // max 2 chars
static constexpr std::string_view _delimiters = "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x7f|~^\t,; "sv;

static std::vector<std::string_view> split(const std::string& s, const std::string& delim)
{
	std::vector<std::string_view> result;
	for (size_t pos = 0; pos < s.size();)
	{
		size_t new_pos = s.find(delim, pos);
		if (new_pos == std::string::npos)
			new_pos = s.size();
		if (new_pos > pos)
			result.emplace_back(s.data() + pos, new_pos - pos);
		pos = new_pos + delim.size();
	}
	return result;
}

CSVParser::CSVParser(std::shared_ptr<BaseReader> reader) :
	m_reader(reader),
	m_row_number(0),
	is_inferring(false)
{
	m_schema.delimiter = ',';
	m_schema.quote_char = '"';
	m_schema.escape_char = '\0';
	m_schema.header_row = -1;
	m_schema.first_data_row = 1;
}

CSVParser::~CSVParser()
{
	close();
}

bool CSVParser::do_infer_schema()
{
	m_schema.columns.clear();
	if (!m_reader->open())
		return false;
	std::string sample(SAMPLE_SIZE, '\0');
	sample.resize(m_reader->read(&sample[0], SAMPLE_SIZE));
	bool complete_file = sample.size() >= m_reader->filesize();
	close();

	UErrorCode ustatus = U_ZERO_ERROR;
	CUCharsetDetector ucsd(ucsdet_open(&ustatus)); if (!U_SUCCESS(ustatus)) return false;
	ucsdet_setText(ucsd, sample.data(), (int)sample.size(), &ustatus); if (!U_SUCCESS(ustatus)) return false;
	const UCharsetMatch* ucm = ucsdet_detect(ucsd, &ustatus); if (!U_SUCCESS(ustatus)) return false;
	int32_t uconfidence = ucsdet_getConfidence(ucm, &ustatus); if (!U_SUCCESS(ustatus)) return false;
	if (uconfidence < 10)
		return false;
	m_schema.charset = ucsdet_getName(ucm, &ustatus);
	if (startswith("ISO-")(m_schema.charset) && std::all_of(sample.begin(), sample.end(), [](int c) { return c < 128; } ))
		m_schema.charset = "ASCII";
	else if (m_schema.charset != "UTF-8")
	{
		CUConverter ucnv(ucnv_open(m_schema.charset.data(), &ustatus)); if (!U_SUCCESS(ustatus)) return false;
		std::string sample_cnv(SAMPLE_SIZE, '\0');
		int new_size = ucnv_toAlgorithmic(UCNV_UTF8, ucnv, sample_cnv.data(), (int)sample_cnv.size(), sample.data(), (int)sample.size(), &ustatus);
		if (!(U_SUCCESS(ustatus) || ustatus == U_TRUNCATED_CHAR_FOUND || ustatus == U_BUFFER_OVERFLOW_ERROR)) return false;
		if (new_size < (int)sample_cnv.size())
			sample_cnv.resize(new_size);
		sample = std::move(sample_cnv);
	}
	if (sample.size() >= 2 && sample[0] == '\xEF' && sample[1] == '\xBB' && sample[2] == '\xBF')
		sample.erase(sample.begin(), sample.begin() + 3);

	bool quoting = false;
	int escape = -1;
	bool in_quotes = false;
	size_t iw = 0;
	for (size_t ir = 0; ir < sample.size(); ++ir) // replace all quoted strings with single \" character
	{
		char c = sample[ir];
		if (in_quotes)
		{
			if (c == '"')
				if (sample[ir + 1] == '"') // skip double quotes (in C++11 sample[sample.size()] == '\0')
					++ir, escape = 0; // no escape_char if double quote found
				else
					in_quotes = false;
			else if (c == '\\') // skip escaped characters
				if (sample[++ir] == '"' && escape != 0)
					escape = 1;
		}
		else
		{
			if (c == '"')
				in_quotes = quoting = true;
			sample[iw++] = c;
		}
	}
	sample.resize(iw);

	std::smatch m;
	if (!std::regex_search(sample, m, _re_line_terminators))
		return false;
	m_schema.newline = m.str();
	std::vector<std::string_view> lines = split(sample, m_schema.newline);
	if (!complete_file)
		lines.pop_back();

	// comment_char is one that is first in the largest number of lines
	m_schema.comment.clear();
	const std::string* comment = nullptr;
	size_t cmt_cnt = 0;
	for (const std::string& c : _comment_chars)
	{
		size_t this_cnt = std::count_if(lines.begin(), lines.end(), startswith(c));
		if (this_cnt > cmt_cnt && this_cnt < lines.size()) // if all lines start with comment, ignore it
			cmt_cnt = this_cnt, comment = &c;
	}
	if (comment != nullptr)
		lines.erase(std::remove_if(lines.begin(), lines.end(), startswith(*comment)), lines.end());

	// find most consistent delimiter (with largest number of rows where it has the same count)
	std::array<std::unordered_map<size_t, size_t>, _delimiters.size()> cnt_freq; // cnt_freq[delimiter_index][number of delimiters per line] = number of lines with this count
	for (const std::string_view& line : lines)
		for (size_t i = 0; i < _delimiters.size(); ++i)
		{
			size_t cnt = std::count(line.begin(), line.end(), _delimiters[i]);
			if (cnt > 0)
				++cnt_freq[i][cnt];
		}
	size_t lines_cnt = 0;
	for (size_t i = 0; i < _delimiters.size(); ++i)
		if (!cnt_freq[i].empty())
		{
			size_t this_cnt = std::max_element(cnt_freq[i].begin(), cnt_freq[i].end(), [](auto p1, auto p2) { return p1.second < p2.second; })->second;
			if (this_cnt > lines_cnt)
				lines_cnt = this_cnt, m_schema.delimiter = _delimiters[i];
		}
//	if (lines_cnt == 0)
//		return false;

	if (quoting)
		for (const std::string_view& line : lines)
			for (size_t i = 0; i < line.size(); ++i)
				if (line[i] == '"') // found quoted value, if it's not surrounded by delimiters then quotes are messed up, ignore them
				{
					for (size_t j = i; j > 0 && line[j - 1] != m_schema.delimiter; --j)
						if (!(quoting = StringUtil::CharacterIsSpace(line[j - 1])))
							goto found_bad_quote;
					for (size_t j = i; j < line.size() - 1 && line[j + 1] != m_schema.delimiter; ++j)
						if (!(quoting = StringUtil::CharacterIsSpace(line[j + 1])))
							goto found_bad_quote;
				}
found_bad_quote:
	if (quoting)
		m_schema.quote_char = '"', m_schema.escape_char = escape == 1 ? '\\' : '\0';
	else
		m_schema.quote_char = m_schema.escape_char = '\0';
	//m_schema.quote_char = '"';  m_schema.escape_char = '\\';
	m_schema.header_row = -1;
	m_schema.first_data_row = 1;
	if (!open())
		return false;

	std::vector<RowRawNumbered> rows(INFER_MAX_ROWS);
	DataChunk output;
	output.data.emplace_back(LogicalType::SQLNULL, nullptr);
	m_columns.push_back(std::make_unique<IngestColCSVInfer>(rows, cur_row));
	is_inferring = true;
	idx_t n_rows = FillChunk(output);
	if (n_rows <= STANDARD_VECTOR_SIZE) {
		rows.resize(n_rows);
	}
	is_inferring = false;
	m_columns.clear();
	do_infer_table(comment, rows);
	close();
	
//	if (std::regex_match(m_schema.newline, _re_universal_newlines))
//		m_schema.newline.clear();
	if (comment != nullptr)
		m_schema.comment = *comment;

	return true;
}

bool CSVParser::open()
{
	m_cvt_buf.reset();
	if (!m_reader->open())
		return false;
	if (m_schema.charset == "UTF-8")
		m_reader->skip_prefix("\xEF\xBB\xBF"sv);
	else if (m_schema.charset != "ASCII")
	{
		UErrorCode ustatus = U_ZERO_ERROR;
		CUConverter ucnv_from = ucnv_open(m_schema.charset.data(), &ustatus); if (!U_SUCCESS(ustatus)) return false;
		CUConverter ucnv_to = ucnv_open("UTF-8", &ustatus); if (!U_SUCCESS(ustatus)) return false;
		if (m_schema.charset == "UTF-16LE")
			m_reader->skip_prefix("\xFF\xFE"sv);
		else if (m_schema.charset == "UTF-16BE")
			m_reader->skip_prefix("\xFE\xFF"sv);
		m_cvt_buf.reset(new ucvt_streambuf(m_reader, std::move(ucnv_from), std::move(ucnv_to)));
	}
	m_schema.comment_lines_skipped_in_parsing = 0;

	cur = end = nullptr;
	is_finished = read_finished = false;
	buffer = make_unsafe_uniq_array<char>(BUFFER_SIZE);
	for (m_row_number = 0; m_row_number < m_schema.first_data_row - 1; ++m_row_number) // skip first rows
	{
		char c;
		if (!next_char(c)) return false;
		while (!is_newline(c)) if (!next_char(c)) return false;
	}
	return true;
}

void CSVParser::close()
{
	m_reader->close();
	m_cvt_buf.reset();
}

int CSVParser::get_percent_complete()
{
	return m_reader->pos_percent();
}

bool CSVParser::next_char(char& c)
{
	if (cur < end || underflow()) {
		c = *cur++;
		return true;
	}
	return false;
}

bool CSVParser::check_next_char(char c)
{
	if (cur < end || underflow()) {
		bool ret = *cur == c;
		cur += ret;
		return ret;
	}
	return false;
}

bool CSVParser::underflow()
{
	// check total buffer length or tmp_value length
	if (read_finished) {
		is_finished = true;
		return false;
	}
	char* start = buffer.get();
	size_t n = 0;
	if (m_cvt_buf) {
		while(n < BUFFER_SIZE && m_cvt_buf->get(start[n]))
			++n;
	} else {
		n = m_reader->read(start, BUFFER_SIZE);
	}
	read_finished = n < BUFFER_SIZE;
	if (n == 0) {
		return false;
	}
	cur = start;
	end = cur + n;
	return true;
}

bool CSVParser::is_newline(char c)
{
	return /*m_schema.newline.empty() ?
		(c == '\r' || c == '\n') :*/
		(c == m_schema.newline[0] && (m_schema.newline.size() == 1 || check_next_char(m_schema.newline[1])));
}

void CSVParser::write_value(size_t i_col) {
	bool remove_null_strings = m_schema.remove_null_strings;
	if (i_col >= m_columns.size() - 1) {
		if (is_inferring) {
			i_col = 0;
			remove_null_strings = false;
		} else {
			tmp_string.clear();
			return;
		}
	}
	string_t s = tmp_string.empty() ? string_t(value_start, value_end - value_start) : tmp_string.AppendChunk(value_start, value_end);
	auto utf_type = Utf8Proc::Analyze(s.GetData(), s.GetSize());
	if (utf_type == UnicodeType::INVALID) {
		throw InvalidInputException("Invalid unicode");
	}
	if (remove_null_strings && (s.GetSize() == 0 || s == "NULL" || s == "null") || !m_columns[i_col]->Write(s)) {
		m_columns[i_col]->WriteNull();
	}
	tmp_string.clear();
}

idx_t CSVParser::FillChunk(DataChunk &output) {
	size_t n_columns = m_columns.size();
	D_ASSERT(output.data.size() == n_columns);
	for (size_t i = 0; i < n_columns; ++i) {
		m_columns[i]->SetVector(&output.data[i]);
	}
	--n_columns; // last column is row number

	cur_row = 0;
	size_t i_col;

	while (cur < end || underflow()) {
		i_col = 0;
		if (!m_schema.comment.empty() && *cur == m_schema.comment[0]) {
			++cur;
			if (m_schema.comment.size() == 1) {
				goto state_comment;
			}
			if (cur >= end) {
				if (underflow()) {
					if (*cur == m_schema.comment[1]) {
						++cur;
						goto state_comment;
					}
					tmp_string.AppendChar(m_schema.comment[0]);
					goto state_unquoted;
				}
			} else if (*cur == m_schema.comment[1]) {
				++cur;
				goto state_comment;
			}
			--cur;
		}
		goto state_value_start;

	state_comment:
		while (true) {
			if (cur >= end && !underflow()) goto state_eof;
			if (*cur == m_schema.newline[0]) {
				++cur;
				if (m_schema.newline.size() == 1) {
					break;
				}
				if (cur >= end && !underflow()) goto state_eof;
				if (*cur == m_schema.newline[1]) {
					++cur;
					break;
				}
				continue;
			}
			++cur;
		}
		++m_row_number;
		continue;

	state_value_start:
		if (*cur == m_schema.quote_char) {
			++cur;
			value_start = cur;

			while (true) {
				while (true) {
					if (cur >= end) {
						if (read_finished) {
							value_end = cur;
							goto state_row_end_quoted;
						}
						tmp_string.AppendChunk(value_start, cur);
						value_start = buffer.get();
						if (!underflow()) {
							value_end = value_start;
							goto state_row_end_quoted;
						}
					}
					if (*cur == m_schema.quote_char) {
						value_end = cur;
						++cur;
						break;
					}
					if (*cur == m_schema.escape_char) {
						tmp_string.AppendChunk(value_start, cur);
						++cur;
						if (cur >= end && !underflow()) {
							value_start = value_end = nullptr;
							goto state_row_end_quoted;
						}
						value_start = cur;
					}
					++cur;
				}

				if (cur >= end) {
					if (read_finished) {
						goto state_row_end_quoted;
					}
					tmp_string.AppendChunk(value_start, value_end);
					value_start = value_end = nullptr;
					if (!underflow()) {
						goto state_row_end_quoted;
					}
				}
				if (*cur == m_schema.delimiter) {
					++cur;
					goto state_value_end;
				}
				if (*cur == m_schema.newline[0]) {
					if (m_schema.newline.size() == 1) {
						++cur;
						goto state_row_end_quoted;
					}
					++cur;
					if (cur >= end) {
						if (read_finished) {
							goto state_row_end_quoted;
						}
						tmp_string.AppendChunk(value_start, value_end);
						value_start = value_end = nullptr;
						if (!underflow()) {
							goto state_row_end_quoted;
						}
						if (*cur == m_schema.newline[1]) {
							++cur;
							goto state_row_end_quoted;
						}
						tmp_string.AppendChar(m_schema.quote_char);
						tmp_string.AppendChar(m_schema.newline[0]);
						value_start = cur;
						continue;
					} else if (*cur == m_schema.newline[1]) {
						++cur;
						goto state_row_end_quoted;
					}
					--cur;
				} else if (*cur == m_schema.quote_char) {
					tmp_string.AppendChunk(value_start, value_end);
					value_start = cur;
					++cur;
					continue;
				}

				if (!value_start) {
					tmp_string.AppendChar(m_schema.quote_char);
					value_start = cur;
				}
			}
		} else {

		state_unquoted:
			value_start = cur;
			while (true) {
				if (*cur == m_schema.delimiter) {
					value_end = cur;
					++cur;
					goto state_value_end;
				}
				if (*cur == m_schema.newline[0]) {
					if (m_schema.newline.size() == 1) {
						value_end = cur;
						++cur;
						goto state_row_end;
					}
					++cur;
					if (cur >= end) {
						if (read_finished) {
							value_end = cur;
							goto state_row_end;
						}
						tmp_string.AppendChunk(value_start, cur-1);
						if (!underflow()) {
							value_start = cur - 1;
							value_end = cur;
							goto state_row_end;
						}
						if (*cur == m_schema.newline[1]) {
							++cur;
							value_start = value_end = nullptr;
							goto state_row_end;
						}
						tmp_string.AppendChar(m_schema.newline[0]);
						value_start = cur;
					} else if (*cur == m_schema.newline[1]) {
						value_end = cur - 1;
						++cur;
						goto state_row_end;
					}
					continue;
				}
				++cur;
				if (cur >= end) {
					if (read_finished) {
						value_end = cur;
						goto state_row_end;
					}
					tmp_string.AppendChunk(value_start, cur);
					value_start = buffer.get();
					if (!underflow()) {
						value_end = value_start;
						goto state_row_end;
					}
				}
			}
		}

	state_value_end:
		write_value(i_col++);
		if (cur >= end) {
			if (!underflow()) {
				value_start = value_end = nullptr;
				goto state_row_end;
			}
		}
		goto state_value_start;

	state_row_end:
		if (i_col == 0 && tmp_string.empty() && value_start == value_end) {
			++m_row_number;
			continue;
		}

	state_row_end_quoted:
		write_value(i_col++);
		while (i_col < n_columns)
			m_columns[i_col++]->WriteNull();
		m_columns.back()->Write((int64_t)++m_row_number);
		if (++cur_row >= STANDARD_VECTOR_SIZE) {
			return cur_row;
		}
	}

state_eof:
	close();
	return cur_row;
}

WKTParser::WKTParser(std::shared_ptr<BaseReader> reader) :
	CSVParser(reader)
{
	m_schema.delimiter = '\0';
	m_schema.quote_char = '\0';
	m_schema.newline = "\n";
	m_schema.columns.clear();
	m_schema.charset = "ASCII";
	m_schema.columns.push_back({ "geometry", ColumnType::Geography, 0, false });
}

}
