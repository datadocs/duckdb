#include <cstdlib>
#include <charconv>

#include "duckdb/common/types/cast_helpers.hpp"

#include "converters.hpp"

namespace duckdb {

static void FormatTwoDigits(int value, char buffer[], idx_t &length) {
	D_ASSERT(value >= -99 && value <= 99);
	if (value < 0) {
		buffer[length++] = '-';
		value = -value;
	}
	if (value < 10) {
		buffer[length++] = '0' + value;
	} else {
		size_t index = (size_t)value * 2;
		buffer[length++] = duckdb_fmt::internal::data::digits[index];
		buffer[length++] = duckdb_fmt::internal::data::digits[index + 1];
	}
}

string IntervalToISOString(const interval_t &interval) {
	char buffer[50] = {'P'};
	idx_t length = 1;
	if (interval.months != 0) {
		auto [years, months] = std::div(interval.months, 12);
		if (years != 0) {
			IntervalToStringCast::FormatSignedNumber(years, buffer, length);
			buffer[length++] = 'Y';
		}
		if (months != 0) {
			FormatTwoDigits(months, buffer, length);
			buffer[length++] = 'M';
		}
	}
	if (interval.days != 0) {
		IntervalToStringCast::FormatSignedNumber(interval.days, buffer, length);
		buffer[length++] = 'D';
	}
	if (interval.micros != 0) {
		buffer[length++] = 'T';
		auto dv = std::div(interval.micros, Interval::MICROS_PER_HOUR);
		if (dv.quot != 0) {
			IntervalToStringCast::FormatSignedNumber(dv.quot, buffer, length);
			buffer[length++] = 'H';
		}
		if (dv.rem != 0) { // minutes, seconds, micros
			dv = std::div(dv.rem, Interval::MICROS_PER_MINUTE);
			if (dv.quot != 0) {
				FormatTwoDigits((int)dv.quot, buffer, length);
				buffer[length++] = 'M';
			}
			if (dv.rem != 0) { // seconds, micros
				if (dv.rem < 0) {
					buffer[length++] = '-';
					dv.rem = -dv.rem;
				}
				dv = std::div(dv.rem, Interval::MICROS_PER_SEC);
				FormatTwoDigits((int)dv.quot, buffer, length);
				if (dv.rem != 0) { // micros
					buffer[length++] = '.';
					auto trailing_zeros = TimeToStringCast::FormatMicros((uint32_t)dv.rem, buffer + length);
					length += 6 - trailing_zeros;
				}
				buffer[length++] = 'S';
			}
		}
	} else if (length == 1) { // empty interval
		buffer[length++] = '0';
		buffer[length++] = 'Y';
	}
	D_ASSERT(length <= std::size(buffer));
	return string(buffer, length);
}

#define CHECK_PART(PART, RANGE)                                                                                        \
	if (cur_part >= PART || num < -RANGE || num > RANGE) {                                                             \
		return false;                                                                                                  \
	}                                                                                                                  \
	cur_part = PART

bool IntervalFromISOString(const char *str, size_t len, interval_t &result) {
	const char *end = str + len;
	if (len < 3 || *str++ != 'P') {
		return false;
	}

	result = {};
	enum IntervalPart { START, YEAR, MONTH, DAY, TIME, HOUR, MINUTE, SECOND };
	IntervalPart cur_part = START;
	for (; str < end; ++str) {
		if (*str == 'T') {
			if (cur_part >= TIME) {
				return false;
			}
			cur_part = TIME;
			continue;
		}
		int32_t num;
		auto [suffix, ec] = std::from_chars(str, end, num);
		if (ec != std::errc() || suffix >= end) {
			return false;
		}
		switch (*suffix) {
		case 'Y':
			CHECK_PART(YEAR, 10000);
			result.months = num * 12;
			break;
		case 'M':
			if (cur_part >= TIME) {
				CHECK_PART(MINUTE, 59);
				result.micros += num * Interval::MICROS_PER_MINUTE;
			} else {
				CHECK_PART(MONTH, 11);
				result.months += num;
			}
			break;
		case 'D':
			CHECK_PART(DAY, 3660000);
			result.days = num;
			break;
		case 'H':
			CHECK_PART(HOUR, 87840000);
			result.micros = num * Interval::MICROS_PER_HOUR;
			break;
		case 'S':
		case '.':
			CHECK_PART(SECOND, 59);
			result.micros += num * Interval::MICROS_PER_SEC;
			if (*suffix == '.') {
				int64_t micros = 0;
				ptrdiff_t exp = 5;
				for (++suffix; suffix < end; --exp) {
					char c = *suffix++;
					if (c == 'S') {
						if (*str == '-') {
							micros = -micros;
						}
						result.micros += micros;
						return suffix == end;
					}
					if (c < '0' || c > '9' || exp < 0) {
						return false;
					}
					micros += (c - '0') * NumericHelper::POWERS_OF_TEN[exp];
				}
				return false;
			}
			break;
		default:
			return false;
		}
		str = suffix;
	}
	return true;
}

} // namespace duckdb
