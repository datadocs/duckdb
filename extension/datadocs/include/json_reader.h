#ifndef JSON_READER_H
#define JSON_READER_H

#include "rapidjson/reader.h"
namespace rj = rapidjson;

#include "json.h"
#include "inferrer_impl.h"
#include "file_reader.h"

namespace duckdb {

class BaseReaderStream
{
public:
	typedef char Ch;
	BaseReaderStream(BaseReader& reader) : m_reader(reader) {}
	Ch Peek() { Ch c; if (!m_reader.peek(c)) return '\0'; return c; }
	Ch Take() { Ch c; if (!m_reader.next_char(c)) return '\0'; return c; }
	size_t Tell() const { return m_reader.tell(); }

	// Not implemented
	void Put(Ch) { RAPIDJSON_ASSERT(false); }
	void Flush() { RAPIDJSON_ASSERT(false); } 
	Ch* PutBegin() { RAPIDJSON_ASSERT(false); return 0; }
	size_t PutEnd(Ch*) { RAPIDJSON_ASSERT(false); return 0; }

	// For encoding detection only.
	const Ch* Peek4() { return m_reader.peek_start(4); }

private:
	BaseReader& m_reader;
};

class JSONValue : public JSONHandler
{
public:
	//static JSONValue* create(IngestColBase &column);
	JSONValue(IngestColBase *column) : column(*column) {}
	virtual ~JSONValue() = default;

	virtual bool Null() override { column.WriteNull(); return true; }
	virtual bool Bool(bool b) override { return new_value(b); }
	virtual bool Int(int i) override { return new_value((int64_t)i); }
	virtual bool Uint(unsigned i) override { return new_value((int64_t)i); }
	virtual bool Int64(int64_t i) override { return new_value(i); }
	virtual bool Uint64(uint64_t i) override { return new_value(int64_t(i < INT64_MAX ? i : INT64_MAX)); }
	virtual bool Double(double d) override { return new_value(d); }
	virtual bool String(const char* s, int length, bool copy) override { return new_value(string_t(s, length)); }
	IngestColBase &column;
protected:
	template <typename T>
	bool new_value(T value) {
		if (!column.Write(value))
			column.WriteNull();
		return true;
	}
};

JSONValue *JSONBuildColumn(const IngestColumnDefinition &col, idx_t &cur_row, int list_level=0);

class JSONTopListStruct : public JSONHandler {
public:
	JSONTopListStruct() : col_row_number("__rownum__", cur_row) {}
	virtual ~JSONTopListStruct() = default;

	void BuildColumns(JSONSchema &schema);
	void BindSchema(std::vector<LogicalType> &return_types, std::vector<string> &names);
	void NewChunk(DataChunk &output);

	virtual bool Null() override { ++m_row_number; return true; }
	virtual bool Bool(bool b) override { ++m_row_number; return true; }
	virtual bool Int(int i) override { ++m_row_number; return true; }
	virtual bool Uint(unsigned i) override { ++m_row_number; return true; }
	virtual bool Int64(int64_t i) override { ++m_row_number; return true; }
	virtual bool Uint64(uint64_t i) override { ++m_row_number; return true; }
	virtual bool Double(double d) override { ++m_row_number; return true; }
	virtual bool String(const char* s, int length, bool copy) override { ++m_row_number; return true; }
	virtual bool StartObject(JSONDispatcher* dispatcher) override
	{
		m_children.Clear();
		//dispatcher->push(this);
		return true;
	}
	virtual bool Key(const char* s, int length, bool copy, JSONDispatcher* dispatcher) override
	{
		size_t i = m_children.GetIndex(std::string(s, length));
		if (i == -1)
			return false;
		dispatcher->m_value = m_columns[i].get();
		return true;
	}
	virtual bool EndObject(JSONDispatcher* dispatcher) override {
		dispatcher->m_value = this;
		if (m_children.cnt_valid == 0) {
			++m_row_number;
			return true;
		}
		size_t n_columns = m_columns.size();
		if (m_children.cnt_valid < n_columns) {
			for (size_t i_col = 0; i_col < n_columns; ++i_col)
			{
				if (!m_children.valid[i_col]) {
					m_columns[i_col]->column.WriteNull();
				}
			}
		}
		col_row_number.Write(m_row_number++);
		if (++cur_row >= STANDARD_VECTOR_SIZE) {
			dispatcher->m_suspended = true;
		}
		return true;
	}
	virtual bool EndArray(JSONDispatcher* dispatcher) override { return false; }

	idx_t cur_row;

private:
	int64_t m_row_number;
	std::vector<std::unique_ptr<JSONValue>> m_columns;
	IngestColChildrenMap m_children;
	IngestColBIGINT col_row_number;
};

class JSONParser : public ParserImpl
{
public:
	JSONParser(std::shared_ptr<BaseReader> reader);
	virtual ~JSONParser() override;
	virtual bool do_infer_schema() override;
	virtual Schema* get_schema() override { return &m_schema; }
	virtual bool open() override;
	virtual void close() override;
	virtual void BuildColumns() override;
	virtual void BindSchema(std::vector<LogicalType> &return_types, std::vector<string> &names) override;
	virtual idx_t FillChunk(DataChunk &output) override;
	virtual int get_percent_complete() override;

protected:
	bool do_parse();

	JSONSchema m_schema;
	std::shared_ptr<BaseReader> m_reader;
	BaseReaderStream m_stream;
	rj::Reader rj_reader;
	JSONDispatcher m_dispatcher;
	JSONTopListStruct m_top;
};

}

#endif
