#ifndef TYPE_CONV_H
#define TYPE_CONV_H

#include <string>

#include "inferrer.h"

namespace duckdb {

bool string0x_to_bytes(const char* begin, const char* end, char* dst);
bool string_to_decimal(const char* begin, const char* end, std::string& data);
void string_to_variant(const char* src, int src_length, VariantCell& cell);

extern const std::unordered_map<string, bool> bool_dict;

}

#endif
