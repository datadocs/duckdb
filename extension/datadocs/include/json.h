#ifndef JSON_H
#define JSON_H

#include <vector>
#include <string>

#include "file_reader.h"

namespace duckdb {

class JSONDispatcher;

class JSONHandler
{
public:
	virtual ~JSONHandler() = default;
	virtual bool Null() { return false; }
	virtual bool Bool(bool b) { return false; }
	virtual bool Int(int i) { return false; }
	virtual bool Uint(unsigned i) { return false; }
	virtual bool Int64(int64_t i) { return false; }
	virtual bool Uint64(uint64_t i) { return false; }
	virtual bool Double(double d) { return false; }
	virtual bool String(const char* s, int length, bool copy) { return false; }
	virtual bool StartObject(JSONDispatcher* dispatcher) { return false; }
	virtual bool EndObject(JSONDispatcher* dispatcher);
	virtual bool StartArray(JSONDispatcher* dispatcher) { return false; }
	virtual bool Key(const char* s, int length, bool copy, JSONDispatcher* dispatcher) { return false; }
	virtual bool EndArray(JSONDispatcher* dispatcher);
};

class JSONDispatcher
{
public:
	bool Null() { return m_value->Null(); }
	bool Bool(bool b) { return m_value->Bool(b); }
	bool RawNumber(const char* str, int length, bool copy) { return true; }
	bool Int(int i) { return m_value->Int(i); }
	bool Uint(unsigned i) { return m_value->Uint(i); }
	bool Int64(int64_t i) { return m_value->Int64(i); }
	bool Uint64(uint64_t i) { return m_value->Uint64(i); }
	bool Double(double d) { return m_value->Double(d); }
	bool String(const char* s, int length, bool copy) { return m_value->String(s, length, copy); }
	bool StartObject() { return m_value->StartObject(this); }
	bool Key(const char* s, int length, bool copy) { return m_top->Key(s, length, copy, this); }
	bool EndObject(int memberCount) { return m_top->EndObject(this); }
	bool StartArray() { return m_value->StartArray(this); }
	bool EndArray(int memberCount) { return m_top->EndArray(this); }

	void push(JSONHandler* handler) { m_stack.push_back(m_top); m_top = handler; }
	void pop() { m_top = m_value = m_stack.back(); m_stack.pop_back(); }
	void skip() { m_value = &m_skip; }

	bool parse_string(const std::string& input, JSONHandler* handler);
	void init(JSONHandler* root);

	JSONHandler* m_value = nullptr;
	JSONHandler* m_top = nullptr;
	bool m_suspended;

protected:
	class JSONSkip : public JSONHandler
	{
	public:
		virtual bool Null() { return true; }
		virtual bool Bool(bool b) override { return true; }
		virtual bool Int(int i) override { return true; }
		virtual bool Uint(unsigned i) override { return true; }
		virtual bool Int64(int64_t i) override { return true; }
		virtual bool Uint64(uint64_t i) override { return true; }
		virtual bool Double(double d) override { return true; }
		virtual bool String(const char* s, int length, bool copy) override { return true; }
		virtual bool StartObject(JSONDispatcher* dispatcher) override
		{
			if (m_level == 0)
			{
				dispatcher->push(this);
				dispatcher->m_value = this;
			}
			++m_level;
			return true; 
		}
		virtual bool Key(const char* s, int length, bool copy, JSONDispatcher* dispatcher) override { return true; }
		virtual bool EndObject(JSONDispatcher* dispatcher) override
		{
			if (--m_level == 0)
				dispatcher->pop();
			return true;
		}
		virtual bool StartArray(JSONDispatcher* dispatcher) override { return StartObject(dispatcher); }
		virtual bool EndArray(JSONDispatcher* dispatcher) override { return EndObject(dispatcher); }
		size_t m_level = 0;
	};

	std::vector<JSONHandler*> m_stack;
	JSONSkip m_skip;
};

}
#endif
