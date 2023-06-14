#ifndef UTILITY_H
#define UTILITY_H

#include <string>
#include <string_view>
#include <algorithm>

namespace duckdb {

template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

struct startswith
{
	startswith(const std::string& prefix) : m_prefix(prefix) {}
	bool operator () (const std::string_view& s)
	{ return s.size() >= m_prefix.size() && std::equal(m_prefix.begin(), m_prefix.end(), s.begin()); }
private:
	const std::string& m_prefix;
};

struct endswith
{
	endswith(const std::string& suffix) : m_suffix(suffix) {}
	bool operator () (const std::string_view& s)
	{ return s.size() >= m_suffix.size() && std::equal(m_suffix.rbegin(), m_suffix.rend(), s.rbegin()); }
private:
	const std::string& m_suffix;
};

void rtrim(std::string& s);
void trim(std::string& s);

bool is_integer(double v);
bool strptime(const std::string& src, const std::string& fmt, double& dt);

}

#endif
