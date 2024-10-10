#ifndef XML_READER_H
#define XML_READER_H

#include <string>

#include "inferrer_impl.h"
#include "file_reader.h"
#include "xml.h"

struct XML_ParserStruct;

namespace duckdb {

class XMLValueBase : public XMLBase
{
public:
	XMLValueBase(IngestColBase *column) : column(*column) {}
	IngestColBase &column;
};

class XMLValue : public XMLValueBase
{
public:
	using XMLValueBase::XMLValueBase;
	bool new_text(std::string&& s) override
	{
		if (s.empty() || !column.Write(s)) {
			column.WriteNull();
		}
		return true;
	}
};

struct XMLColumnBuilder {
	template <class Col>
	class XMLCol : public XMLValue, public Col {
	public:
		template<typename... Args>
		XMLCol(Args&&... args) : XMLValue(this), Col(std::forward<Args>(args)...) {
		}
	};

	using ReturnType = XMLValueBase;
	template <typename T> using Type = XMLCol<T>;

	static ReturnType *Build(const IngestColumnDefinition &col, idx_t &cur_row);
};

class XMLParser : public ParserImpl
{
public:
	XMLParser(std::shared_ptr<BaseReader> reader);
	virtual ~XMLParser() override;
	virtual bool do_infer_schema() override;
	virtual Schema* get_schema() override { return &m_schema; }
	virtual bool open() override;
	virtual void close() override;
	virtual void BuildColumns() override;
	virtual void BindSchema(std::vector<LogicalType> &return_types, std::vector<string> &names) override;
	virtual idx_t FillChunk(DataChunk &output) override;
	virtual int get_percent_complete() override;

protected:
	Schema m_schema;
	std::shared_ptr<BaseReader> m_reader;
	XMLRoot root;
};

}

#endif
