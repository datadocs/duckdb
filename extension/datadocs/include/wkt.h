#ifndef WKT_H
#define WKT_H

#include <string>

namespace duckdb {

bool wkt_to_bytes(const char*& begin, const char* end, std::string& data);

}

#endif
