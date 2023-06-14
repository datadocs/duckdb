#ifndef XML_H
#define XML_H

#include <string>
#include <duckdb.hpp>

#include "inferrer_impl.h"

struct XML_ParserStruct;

namespace duckdb {

class XMLHandlerBase
{
public:
	~XMLHandlerBase();
	bool parse_string(string_t input);
	XML_ParserStruct* create_parser();
protected:
	static void do_text(void* userdata, const char* buf, int buflen);
	bool check_new_text();
	void abort();
	XML_ParserStruct* m_parser = nullptr;
	std::string m_cur_text;
	void (*p_start)(void*, const char*, const char**) = nullptr;
	void (*p_end)(void*, const char*) = nullptr;
};

template <class Handler>
class XMLHandler : public XMLHandlerBase
{
protected:
	XMLHandler()
	{
		p_start = do_start;
		p_end = do_end;
	}
private:
	static void do_start(void* userdata, const char* name, const char** atts)
	{
		Handler* handler = static_cast<Handler*>(userdata);
		if (handler->check_new_text() && !handler->new_text())
			handler->abort();
		else if (!handler->start_tag(name, atts))
			handler->abort();
	}
	static void do_end(void* userdata, const char*)
	{
		Handler* handler = static_cast<Handler*>(userdata);
		if (handler->check_new_text() && !handler->new_text())
			handler->abort();
		else
			handler->end_tag();
	}
};

class XMLBase
{
public:
	//static XMLBase* create(const IngestColumnDefinition& col, Row::ColumnAdapter* adapter);
	virtual ~XMLBase() = default;
	virtual XMLBase* new_tag(const char* name, const char** atts) { return nullptr; }
	virtual void first_tag() {}
	virtual bool second_tag() { return false; }
	virtual void end_tag() {}
	virtual bool new_text(std::string&& s) { return true; }
	bool m_saw_tag;
};

class XMLRoot : public XMLBase
{
public:
	void assign(XMLBase* xml) { m_root.reset(xml); }
	virtual ~XMLRoot() = default;
	virtual XMLBase* new_tag(const char* name, const char** atts) override
	{
		m_root->first_tag();
		return m_root.get();
	}
//protected:
	std::unique_ptr<XMLBase> m_root;
};

class XMLParseHandler : public XMLHandler<XMLParseHandler>
{
public:
	XMLParseHandler(XMLBase* top = nullptr) : m_top(top) {}

	bool start_tag(const char* name, const char** atts)
	{
		XMLBase* new_top = m_top->new_tag(name, atts);
		if (!new_top)
			return false;
		m_stack.push_back(m_top);
		m_top = new_top;
		return true;
	}

	void end_tag()
	{
		m_top->end_tag();
		m_top = m_stack.back();
		m_stack.pop_back();
	}

	bool new_text()
	{
		bool res = m_top->new_text(std::move(m_cur_text));
		m_cur_text.clear();
		return res;
	};

	bool convert_cell(string_t src, XMLBase *root)
	{
		m_top = root;
		m_stack.clear();
		m_cur_text.clear();
		return parse_string(src);
	}
private:
	XMLBase* m_top;
	std::vector<XMLBase*> m_stack;
};

}
#endif
