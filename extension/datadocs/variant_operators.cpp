#include "datadocs.hpp"
#include "datadocs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

/**
 * @brief Define function type for bitwise operators for bytes type like & (and), | (or) and ^ (xor)
 * second paramater is byte type
 */
typedef void (*bitwise_operator_t)(const string_t &, const string_t &, string_t &);
/**
 * @brief Define function type for bitwise shift operators for bytes type like shift left (<<) and shift right (>>)
 * second parameter is number of bit need to shift
 */
typedef void (*bitwise_shift_opeartor_t)(const string_t &, const idx_t &, string_t &);

/**
 * @brief Define types of operators for any type
 * + (add), - (subtract) (binary) - (date/time, number|interval), (interval, interval), (number, number)
 * || (concat) - (bytes,bytes), (array, array), (anything-else, anything-else)
 * ~ (bitwise negative) - (integer, integer), (bytes, bytes)
 * | (bitwise or), ^ (bitwise xor), & (bitwise and) - (integer, integer), (bytes, bytes)
 * >> (bitwise shift right), << (bitwise shift left) - (integer), (bytes) // left-hand operator must be integer
 * = (assign) - only assign current variant column to result
 */
enum class OperatorType : uint8_t {
	INVALID,
	ADD_OPERATOR,
	SUB_OPERATOR,
	CONCAT_OPERATOR,
	BITWISE_OR,
	BITWISE_AND,
	BITWISE_XOR,
	BITWISE_SHIFT_RIGHT,
	BITWISE_SHIFT_LEFT,
	BITWISE_NEGATION,
	ASSIGN
};

/**
 * @brief Parse Operator type from string
 *
 * @param str
 * @return OperatorType
 */
OperatorType StringToOperatorType(string str) {
	if (str == "+") {
		return OperatorType::ADD_OPERATOR;
	} else if (str == "-") {
		return OperatorType::SUB_OPERATOR;
	} else if (str == "||") {
		return OperatorType::CONCAT_OPERATOR;
	} else if (str == "|") {
		return OperatorType::BITWISE_OR;
	} else if (str == "&") {
		return OperatorType::BITWISE_AND;
	} else if (str == "xor" || str == "^") {
		return OperatorType::BITWISE_XOR;
	} else if (str == ">>") {
		return OperatorType::BITWISE_SHIFT_RIGHT;
	} else if (str == "<<") {
		return OperatorType::BITWISE_SHIFT_LEFT;
	} else if (str == "~") {
		return OperatorType::BITWISE_NEGATION;
	} else if (str == "=") {
		return OperatorType::ASSIGN;
	}

	throw NotImplementedException("String %s is invalid operator!", str);
	return OperatorType::INVALID;
}

/**
 * @brief Function to check logical type is integer and can add to date
 *
 * @param type
 * @return true
 * @return false
 */
bool IntegerCanAddSubDate(LogicalType type) {
	if (type.IsIntegral()) {
		if (type != LogicalType::BIGINT && type != LogicalType::UBIGINT && type != LogicalType::HUGEINT) {
			return true;
		}
	}
	return false;
}

/**
 * @brief Check logical type is array of decayable type
 *
 * @param type
 * @return true
 * @return false
 */
bool IsDecayableArrayType(LogicalType type) {
	if (type.id() == LogicalTypeId::LIST) {
		return IsDecayableType(ListType::GetChildType(type));
	}
	return false;
}

/**
 * @brief Define all operators for any type
 *
 */
//===--------------------------------------------------------------------===//
// - [subtract]
//===--------------------------------------------------------------------===//
struct NegateAnyOperator {
	template <class T>
	static bool CanNegate(T input) {
		using Limits = std::numeric_limits<T>;
		return !(Limits::is_integer && Limits::is_signed && Limits::lowest() == input);
	}

	template <class TA, class TR>
	static inline TR Operation(TA input) {
		auto cast = (TR)input;
		if (!CanNegate<TR>(cast)) {
			throw OutOfRangeException("Overflow in negation of integer!");
		}
		return -cast;
	}
};

template <>
bool NegateAnyOperator::CanNegate(float input) {
	return true;
}

template <>
bool NegateAnyOperator::CanNegate(double input) {
	return true;
}

template <>
interval_t NegateAnyOperator::Operation(interval_t input) {
	interval_t result;
	result.months = NegateAnyOperator::Operation<int32_t, int32_t>(input.months);
	result.days = NegateAnyOperator::Operation<int32_t, int32_t>(input.days);
	result.micros = NegateAnyOperator::Operation<int64_t, int64_t>(input.micros);
	return result;
}

//===--------------------------------------------------------------------===//
// | [bitwise_or]
//===--------------------------------------------------------------------===//
struct BitwiseOROperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return left | right;
	}
};

//===--------------------------------------------------------------------===//
// & [bitwise_and]
//===--------------------------------------------------------------------===//
struct BitwiseANDOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return left & right;
	}
};

//===--------------------------------------------------------------------===//
// # [bitwise_xor]
//===--------------------------------------------------------------------===//
struct BitwiseXOROperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return left ^ right;
	}
};

//===--------------------------------------------------------------------===//
// >> [bitwise_right_shift]
//===--------------------------------------------------------------------===//
template <class T>
bool RightShiftInRange(T shift) {
	return shift >= 0 && shift < T(sizeof(T) * 8);
}

struct BitwiseShiftRightOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA input, TB shift) {
		return RightShiftInRange(shift) ? input >> shift : 0;
	}
};

//===--------------------------------------------------------------------===//
// << [bitwise_left_shift]
//===--------------------------------------------------------------------===//

struct BitwiseShiftLeftOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA input, TB shift) {
		TA max_shift = TA(sizeof(TA) * 8);
		if (input < 0) {
			throw OutOfRangeException("Cannot left-shift negative number %s", NumericHelper::ToString(input));
		}
		if (shift < 0) {
			throw OutOfRangeException("Cannot left-shift by negative number %s", NumericHelper::ToString(shift));
		}
		if (shift >= max_shift) {
			if (input == 0) {
				return 0;
			}
			throw OutOfRangeException("Left-shift value %s is out of range", NumericHelper::ToString(shift));
		}
		if (shift == 0) {
			return input;
		}
		TA max_value = (TA(1) << (max_shift - shift - 1));
		if (input >= max_value) {
			throw OutOfRangeException("Overflow in left shift (%s << %s)", NumericHelper::ToString(input),
			                          NumericHelper::ToString(shift));
		}
		return input << shift;
	}
};

//===--------------------------------------------------------------------===//
// ~ [bitwise_not]
//===--------------------------------------------------------------------===//
struct BitwiseNotOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return ~input;
	}
};

/**
 * @brief Function execute any binary operator between two Value for integer type only
 *
 * @tparam OP
 * @param type
 * @param left_val
 * @param right_val
 * @return Value
 */
template <class OP>
static Value ExecuteBinaryAnyOperatorInteger(PhysicalType type, Value left_val, Value right_val) {
	Value res;
	switch (type) {
	case PhysicalType::INT8:
		res = Value::CreateValue<int8_t>(
		    OP::template Operation<int8_t, int8_t, int8_t>(left_val.GetValue<int8_t>(), right_val.GetValue<int8_t>()));
		break;
	case PhysicalType::INT16:
		res = Value::CreateValue<int16_t>(OP::template Operation<int16_t, int16_t, int16_t>(
		    left_val.GetValue<int16_t>(), right_val.GetValue<int16_t>()));
		break;
	case PhysicalType::INT32:
		res = Value::CreateValue<int32_t>(OP::template Operation<int32_t, int32_t, int32_t>(
		    left_val.GetValue<int32_t>(), right_val.GetValue<int32_t>()));
		break;
	case PhysicalType::INT64:
		res = Value::CreateValue<int64_t>(OP::template Operation<int64_t, int64_t, int64_t>(
		    left_val.GetValue<int64_t>(), right_val.GetValue<int64_t>()));
		break;
	case PhysicalType::UINT8:
		res = Value::CreateValue<uint8_t>(OP::template Operation<uint8_t, uint8_t, uint8_t>(
		    left_val.GetValue<uint8_t>(), right_val.GetValue<uint8_t>()));
		break;
	case PhysicalType::UINT16:
		res = Value::CreateValue<uint16_t>(OP::template Operation<uint16_t, uint16_t, uint16_t>(
		    left_val.GetValue<uint16_t>(), right_val.GetValue<uint16_t>()));
		break;
	case PhysicalType::UINT32:
		res = Value::CreateValue<uint32_t>(OP::template Operation<uint32_t, uint32_t, uint32_t>(
		    left_val.GetValue<uint32_t>(), right_val.GetValue<uint32_t>()));
		break;
	case PhysicalType::UINT64:
		res = Value::CreateValue<uint64_t>(OP::template Operation<uint64_t, uint64_t, uint64_t>(
		    left_val.GetValue<uint64_t>(), right_val.GetValue<uint64_t>()));
		break;
	case PhysicalType::INT128:
		res = Value::CreateValue<hugeint_t>(OP::template Operation<hugeint_t, hugeint_t, hugeint_t>(
		    left_val.GetValueUnsafe<hugeint_t>(), right_val.GetValueUnsafe<hugeint_t>()));
		break;
	default:
		throw NotImplementedException("Unimplemented type for ExecuteBinaryAnyOperatorIngeter");
	}
	return res;
}

/**
 * @brief Function execute any binary operator for numeric between two Values type only
 *
 * @tparam OP
 * @param type
 * @param left_val
 * @param right_val
 * @return Value
 */
template <class OP>
static Value ExecuteBinaryAnyOperator(PhysicalType type, Value left_val, Value right_val) {
	Value res;
	switch (type) {
	case PhysicalType::INT128: {
		auto right_int = right_val.GetValueUnsafe<hugeint_t>();
		auto left_int = left_val.GetValueUnsafe<hugeint_t>();
		res = Value::CreateValue<hugeint_t>(OP::template Operation<hugeint_t, hugeint_t, hugeint_t>(
		    left_val.GetValueUnsafe<hugeint_t>(), right_val.GetValueUnsafe<hugeint_t>()));
	} break;
	case PhysicalType::FLOAT:
		res = Value::CreateValue<float>(
		    OP::template Operation<float, float, float>(left_val.GetValue<float>(), right_val.GetValue<float>()));
		break;
	case PhysicalType::DOUBLE:
		res = Value::CreateValue<double>(
		    OP::template Operation<double, double, double>(left_val.GetValue<double>(), right_val.GetValue<double>()));
		break;
	default:
		res = ExecuteBinaryAnyOperatorInteger<OP>(type, left_val, right_val);
		break;
	}
	return res;
}

/**
 * @brief Execute any unary operator for integer only base on Value
 *
 * @tparam OP
 * @param type
 * @param val
 * @return Value
 */
template <class OP>
static Value ExecuteUnaryAnyOperatorInteger(PhysicalType type, Value val) {
	Value res;
	switch (type) {
	case PhysicalType::INT8:
		res = Value::CreateValue<int8_t>(OP::template Operation<int8_t, int8_t>(val.GetValue<int8_t>()));
		break;
	case PhysicalType::INT16:
		res = Value::CreateValue<int16_t>(OP::template Operation<int16_t, int16_t>(val.GetValue<int16_t>()));
		break;
	case PhysicalType::INT32:
		res = Value::CreateValue<int32_t>(OP::template Operation<int32_t, int32_t>(val.GetValue<int32_t>()));
		break;
	case PhysicalType::INT64:
		res = Value::CreateValue<int64_t>(OP::template Operation<int64_t, int64_t>(val.GetValue<int64_t>()));
		break;
	case PhysicalType::UINT8:
		res = Value::CreateValue<uint8_t>(OP::template Operation<uint8_t, uint8_t>(val.GetValue<uint8_t>()));
		break;
	case PhysicalType::UINT16:
		res = Value::CreateValue<uint16_t>(OP::template Operation<uint16_t, uint16_t>(val.GetValue<uint16_t>()));
		break;
	case PhysicalType::UINT32:
		res = Value::CreateValue<uint32_t>(OP::template Operation<uint32_t, uint32_t>(val.GetValue<uint32_t>()));
		break;
	case PhysicalType::UINT64:
		res = Value::CreateValue<uint64_t>(OP::template Operation<uint64_t, uint64_t>(val.GetValue<uint64_t>()));
		break;
	case PhysicalType::INT128:
		res = Value::CreateValue<hugeint_t>(
		    OP::template Operation<hugeint_t, hugeint_t>(val.GetValueUnsafe<hugeint_t>()));
		break;
	default:
		throw NotImplementedException("Unimplemented type for ExecuteUnaryAnyOperatorInteger");
	}
	return res;
}

/**
 * @brief Execute any unary operators for numeric base on Value
 *
 * @tparam OP
 * @param type
 * @param val
 * @return Value
 */
template <class OP>
static Value ExecuteUnaryAnyOperatorNumeric(PhysicalType type, Value val) {
	Value res;
	switch (type) {
	case PhysicalType::DOUBLE:
		res = Value::CreateValue<double>(OP::template Operation<double, double>(val.GetValue<double>()));
		break;
	case PhysicalType::FLOAT:
		res = Value::CreateValue<float>(OP::template Operation<float, float>(val.GetValue<float>()));
		break;
	default:
		res = ExecuteUnaryAnyOperatorInteger<OP>(type, val);
	}
	return res;
}

/**
 * @brief Add/Subtract Operator between two Values and return Value
 * 		- Cast to same numeric type if not same types
 * 		- Execute add/subtract between two numeric using Add/Subtract Operators for each types (general numeric types,
 * Decimal type and hugeint type)
 *
 * @param left_val
 * @param right_val
 * @param is_add_op
 * @return Value
 */
static Value AddSubtractNumericOperator(Value left_val, Value right_val, bool is_add_op) {
	LogicalType left_type = left_val.type();
	LogicalType right_type = right_val.type();
	Value res;

	Value new_left_val = left_val;
	Value new_right_val = right_val;
	if (left_type.IsNumeric() && right_type.IsNumeric() && left_type.id() != right_type.id()) {
		LogicalType input_type = LogicalType::ForceMaxLogicalType(left_type, right_type);
		left_type = input_type;
		right_type = input_type;
		new_left_val = left_val.DefaultCastAs(input_type);
		new_right_val = right_val.DefaultCastAs(input_type);
	}

	if (left_type.IsNumeric() && left_type.id() == right_type.id()) {
		if (left_type.id() == LogicalTypeId::DECIMAL) {
			res = is_add_op ? ExecuteBinaryAnyOperator<DecimalAddOverflowCheck>(left_type.InternalType(), new_left_val,
			                                                                    new_right_val)
			                : ExecuteBinaryAnyOperator<DecimalSubtractOverflowCheck>(left_type.InternalType(),
			                                                                         new_left_val, new_right_val);
			DecimalTypeInfo info = left_type.AuxInfo()->Cast<DecimalTypeInfo>();
			res = Value::DECIMAL(res.GetValue<hugeint_t>(), info.width, info.scale);
		} else if (left_type.IsIntegral() && left_type.id() != LogicalTypeId::HUGEINT) {
			res = is_add_op ? ExecuteBinaryAnyOperatorInteger<AddOperatorOverflowCheck>(left_type.InternalType(),
			                                                                            new_left_val, new_right_val)
			                : ExecuteBinaryAnyOperatorInteger<SubtractOperatorOverflowCheck>(
			                      left_type.InternalType(), new_left_val, new_right_val);
		} else {
			res =
			    is_add_op
			        ? ExecuteBinaryAnyOperator<AddOperator>(left_type.InternalType(), new_left_val, new_right_val)
			        : ExecuteBinaryAnyOperator<SubtractOperator>(left_type.InternalType(), new_left_val, new_right_val);
		}
	}

	return res;
}

/**
 * @brief Add any operator between two Values
 *   - (numeric + numeric) => call numeric operator
 *   - (date + numeric) => date
 *   - (date + interval) => date
 *   - (date + time) => datetime (timestamp)
 *   - (numeric + date) => date
 *   - (interval + interval) => interval
 *   - (interval + date) => date
 *   - (interval + timestamp) => timestamp
 *   - (time + interval) => time
 * 	 - (time + date) => datetime (timestamp)
 * 	 - (timestamp + interval) => timestamp
 * @param left_val
 * @param right_val
 * @return Value
 */
static Value AddAnyOperator(Value left_val, Value right_val) {
	LogicalType left_type = left_val.type();
	LogicalType right_type = right_val.type();
	Value res;

	if (left_type.IsNumeric() && right_type.IsNumeric()) {
		res = AddSubtractNumericOperator(left_val, right_val, true);
	}

	switch (left_type.id()) {
	case LogicalTypeId::DATE:
		if (right_type.IsNumeric()) {
			res = Value::DATE(AddOperator::Operation<date_t, int32_t, date_t>(left_val.GetValue<date_t>(),
			                                                                  right_val.GetValue<int32_t>()));
		} else if (right_type.id() == LogicalTypeId::INTERVAL) {
			res = Value::DATE(Timestamp::GetDate(AddOperator::Operation<date_t, interval_t, timestamp_t>(
			    left_val.GetValue<date_t>(), right_val.GetValue<interval_t>())));
		} else if (right_type.id() == LogicalTypeId::TIME) {
			res = Value::TIMESTAMP(AddOperator::Operation<date_t, dtime_t, timestamp_t>(left_val.GetValue<date_t>(),
			                                                                            right_val.GetValue<dtime_t>()));
		}
		break;
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::BIGINT:
		if (right_type.id() == LogicalTypeId::DATE) {
			res = Value::DATE(AddOperator::Operation<int32_t, date_t, date_t>(left_val.GetValue<int32_t>(),
			                                                                  right_val.GetValue<date_t>()));
		}
		break;
	case LogicalTypeId::INTERVAL:
		if (right_type.id() == LogicalTypeId::INTERVAL) {
			res = Value::INTERVAL(AddOperator::Operation<interval_t, interval_t, interval_t>(
			    left_val.GetValue<interval_t>(), right_val.GetValue<interval_t>()));
		} else if (right_type.id() == LogicalTypeId::DATE) {
			res = Value::DATE(Timestamp::GetDate(AddOperator::Operation<interval_t, date_t, timestamp_t>(
			    left_val.GetValue<interval_t>(), right_val.GetValue<date_t>())));
		} else if (right_type.id() == LogicalTypeId::TIME) {
			return Value::TIME(AddTimeOperator::Operation<interval_t, dtime_t, dtime_t>(left_val.GetValue<interval_t>(),
			                                                                            right_val.GetValue<dtime_t>()));
		} else if (right_type.id() == LogicalTypeId::TIMESTAMP) {
			return Value::TIMESTAMP(AddOperator::Operation<interval_t, timestamp_t, timestamp_t>(
			    left_val.GetValue<interval_t>(), right_val.GetValue<timestamp_t>()));
		}
		break;
	case LogicalTypeId::TIME:
		if (right_type.id() == LogicalTypeId::INTERVAL) {
			return Value::TIME(AddTimeOperator::Operation<dtime_t, interval_t, dtime_t>(
			    left_val.GetValue<dtime_t>(), right_val.GetValue<interval_t>()));
		} else if (right_type.id() == LogicalTypeId::DATE) {
			return Value::TIMESTAMP(AddOperator::Operation<dtime_t, date_t, timestamp_t>(left_val.GetValue<dtime_t>(),
			                                                                             right_val.GetValue<date_t>()));
		}
		break;
	case LogicalTypeId::TIMESTAMP:
		if (right_type.id() == LogicalTypeId::INTERVAL) {
			return Value::TIMESTAMP(AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(
			    left_val.GetValue<timestamp_t>(), right_val.GetValue<interval_t>()));
		}
		break;
	default:
		break;
	}

	return res;
}

/**
 * @brief Subtract any operator between two Values
 * 	 - (numeric - numeric) => call numeric binary operator
 * 	 - (date - date) => integer
 * 	 - (date - numeric) => date
 * 	 - (date - interval) => date
 * 	 - (timestamp - timestamp) => interval
 *   - (timestamp - interval) => timestamp
 *   - (interval - interval) => interval
 *   - (time - interval) => time
 *
 * @param left_val
 * @param right_val
 * @return Value
 */
static Value SubtractAnyOperator(Value left_val, Value right_val) {
	LogicalType left_type = left_val.type();
	LogicalType right_type = right_val.type();
	Value res;

	if (left_type.IsNumeric() && right_type.IsNumeric()) {
		res = AddSubtractNumericOperator(left_val, right_val, false);
	}

	switch (left_type.id()) {
	case LogicalTypeId::DATE:
		if (right_type.id() == LogicalTypeId::DATE) {
			res = Value::BIGINT(SubtractOperator::Operation<date_t, date_t, int64_t>(left_val.GetValue<date_t>(),
			                                                                         right_val.GetValue<date_t>()));
		} else if (right_type.IsNumeric()) {
			res = Value::DATE(SubtractOperator::Operation<date_t, int32_t, date_t>(left_val.GetValue<date_t>(),
			                                                                       right_val.GetValue<int32_t>()));
		} else if (right_type.id() == LogicalTypeId::INTERVAL) {
			res = Value::DATE(Timestamp::GetDate(SubtractOperator::Operation<date_t, interval_t, timestamp_t>(
			    left_val.GetValue<date_t>(), right_val.GetValue<interval_t>())));
		}
		break;
	case LogicalTypeId::TIMESTAMP:
		if (right_type.id() == LogicalTypeId::TIMESTAMP) {
			res = Value::INTERVAL(SubtractOperator::Operation<timestamp_t, timestamp_t, interval_t>(
			    left_val.GetValue<timestamp_t>(), right_val.GetValue<timestamp_t>()));
		} else if (right_type.id() == LogicalTypeId::INTERVAL) {
			res = Value::TIMESTAMP(SubtractOperator::Operation<timestamp_t, interval_t, timestamp_t>(
			    left_val.GetValue<timestamp_t>(), right_val.GetValue<interval_t>()));
		}
		break;
	case LogicalTypeId::INTERVAL:
		if (right_type.id() == LogicalTypeId::INTERVAL) {
			res = Value::INTERVAL(SubtractOperator::Operation<interval_t, interval_t, interval_t>(
			    left_val.GetValue<interval_t>(), right_val.GetValue<interval_t>()));
		}
		break;
	case LogicalTypeId::TIME:
		if (right_type.id() == LogicalTypeId::INTERVAL) {
			res = Value::TIME(SubtractTimeOperator::Operation<dtime_t, interval_t, dtime_t>(
			    left_val.GetValue<dtime_t>(), right_val.GetValue<interval_t>()));
		}
		break;
	default:
		break;
	}

	return res;
}

/**
 * @brief Concat any operators between two Values
 *  - (bytes || bytes) => bytes
 * 	- (list || list) => list
 *  - (string || string) => string
 * 	- (remaining type || remaining type) => cast to string and concat
 *
 * @param left_val
 * @param right_val
 * @param result
 * @return Value
 */
static Value ConcatAnyOperator(Value left_val, Value right_val, Vector &result) {
	Value res;
	LogicalType left_type = left_val.type();
	LogicalType right_type = right_val.type();

	if (left_type == LogicalType::BLOB && right_type == LogicalType::BLOB) {
		// Handle concat for two byte type values
		string_t a = left_val.GetValueUnsafe<string_t>();
		string_t b = right_val.GetValueUnsafe<string_t>();
		auto a_data = a.GetData();
		auto b_data = b.GetData();
		auto a_length = a.GetSize();
		auto b_length = b.GetSize();

		auto target_length = a_length + b_length;
		// auto target = string_t(target_length);
		auto target = StringVector::EmptyString(result, target_length);
		auto target_data = target.GetDataWriteable();

		memcpy(target_data, a_data, a_length);
		memcpy(target_data + a_length, b_data, b_length);
		target.Finalize();
		res = Value::BLOB((const_data_ptr_t)target_data, target_length);
	} else if (left_type != LogicalType::BLOB && right_type != LogicalType::BLOB) {
		if (left_type.id() != LogicalTypeId::LIST || right_type.id() != LogicalTypeId::LIST) {
			// Handle concat for none-bytes, none-list, Cast to string and concat between two string
			auto new_left_val = left_val;
			auto new_right_val = right_val;
			if (left_type != LogicalType::VARCHAR) {
				new_left_val = left_val.DefaultCastAs(LogicalType::VARCHAR);
			}
			if (right_type != LogicalType::VARCHAR) {
				new_right_val = right_val.DefaultCastAs(LogicalType::VARCHAR);
			}
			string_t a = new_left_val.GetValueUnsafe<string_t>();
			string_t b = new_right_val.GetValueUnsafe<string_t>();
			auto a_data = a.GetData();
			auto b_data = b.GetData();
			auto a_length = a.GetSize();
			auto b_length = b.GetSize();

			auto target_length = a_length + b_length;
			auto target = StringVector::EmptyString(result, target_length);
			auto target_data = target.GetDataWriteable();

			memcpy(target_data, a_data, a_length);
			memcpy(target_data + a_length, b_data, b_length);
			target.Finalize();
			res = Value::CreateValue<string_t>(target);
		} else {
			// Handle for cast list type
			auto left_child_type = ListType::GetChildType(left_type);
			auto right_child_type = ListType::GetChildType(right_type);
			vector<Value> left_values = ListValue::GetChildren(left_val);
			vector<Value> right_values = ListValue::GetChildren(right_val);
			if (left_child_type == right_child_type) {
				// Same list type => list of child type
				std::vector<Value> values;
				values.insert(values.end(), left_values.begin(), left_values.end());
				values.insert(values.end(), right_values.begin(), right_values.end());

				res = Value::LIST(left_child_type, std::move(values));
			} else {
				// not same type => List of variant
				auto input_type = DDVariantType;
				if (left_type != DDVariantArrayType) {
					Vector left_vec(left_val);
					Vector left_result(DDVariantArrayType);
					TransformVariantArrayFunc(left_vec, left_result, 1);
					left_values = ListValue::GetChildren(left_result.GetValue(0));
				}
				if (right_type != DDVariantArrayType) {
					Vector right_vec(right_val);
					Vector right_result(DDVariantArrayType);
					TransformVariantArrayFunc(right_vec, right_result, 1);
					right_values = ListValue::GetChildren(right_result.GetValue(0));
				}
				std::vector<Value> values;
				values.insert(values.end(), left_values.begin(), left_values.end());
				values.insert(values.end(), right_values.begin(), right_values.end());

				res = Value::LIST(input_type, std::move(values));
			}
		}
	}

	return res;
}

/**
 * @brief Bitwise any operator between two Values
 * 		- (& - and, | - or, ~ - not, ^ - xor): operator between two numerics or bytes
 * 		- (>> - shift right, << - shift left): operator between numeric or byte with integer
 *
 * @tparam OP
 * @tparam FUNC_TYPE
 * @tparam RIGHT_TYPE
 * @param op_type
 * @param left_val
 * @param right_val
 * @param FUNC
 * @return Value
 */
template <class OP, typename FUNC_TYPE, typename RIGHT_TYPE>
static Value BitwiseAnyOperator(OperatorType op_type, Value left_val, Value right_val, FUNC_TYPE FUNC) {
	Value res;
	LogicalType left_type = left_val.type();
	LogicalType right_type = right_val.type();
	if (left_type.IsIntegral() && right_type.IsIntegral()) {
		// Handle bitwise operator for numeric types
		LogicalType input_type = left_type;
		Value new_left_val = left_val;
		Value new_right_val = right_val;
		if (left_type != right_type) {
			input_type = LogicalType::ForceMaxLogicalType(left_type, right_type);
			new_left_val = left_val.DefaultCastAs(input_type);
			new_right_val = right_val.DefaultCastAs(input_type);
		}
		res = ExecuteBinaryAnyOperatorInteger<OP>(input_type.InternalType(), new_left_val, new_right_val);
	} else if (op_type == OperatorType::BITWISE_SHIFT_LEFT || op_type == OperatorType::BITWISE_SHIFT_RIGHT) {
		// Handle bitwise shift left/right for byte type (right type must be integral)
		if (right_type.IsIntegral() && left_type == LogicalType::BLOB) {
			auto new_left_val = left_val.DefaultCastAs(LogicalType::BIT);
			auto new_right_val = right_val.DefaultCastAs(LogicalType::INTEGER);
			string_t lhs = new_left_val.GetValueUnsafe<string_t>();
			RIGHT_TYPE rhs = new_right_val.GetValueUnsafe<RIGHT_TYPE>();
			string_t target(lhs.GetSize());
			FUNC(lhs, rhs, target);
			res = Value::BIT((const_data_ptr_t)target.GetDataUnsafe(), target.GetSize());
			res = res.DefaultCastAs(LogicalType::BLOB);
		}
	} else if (left_type == LogicalType::BLOB && right_type == LogicalType::BLOB) {
		// Handle remaining bitwise operators for two byte values
		auto new_left_val = left_val.DefaultCastAs(LogicalType::BIT);
		auto new_right_val = right_val.DefaultCastAs(LogicalType::BIT);
		string_t lhs = new_left_val.GetValueUnsafe<string_t>();
		RIGHT_TYPE rhs = new_right_val.GetValueUnsafe<RIGHT_TYPE>();
		string_t target(lhs.GetSize());
		FUNC(lhs, rhs, target);
		res = Value::BIT((const_data_ptr_t)target.GetDataUnsafe(), target.GetSize());
		res = res.DefaultCastAs(LogicalType::BLOB);
	}
	return res;
}

/**
 * @brief Handle Binary Operators for any type between two values
 * - One of two types is decayable => get original value and handle binary with new values
 * - Execute between two values base on Operator type
 * 		+ add operator -> AddAnyOperator
 * 		+ subtract operator -> SubtractAnyOperator
 * 		+ concat operator -> ConcatAnyOperator
 * 		+ bitwise operators -> BitwiseAnyOperator
 *
 * @param result
 * @param index
 * @param op
 * @param left_val
 * @param right_val
 */
static void HandleBinaryOpAny(Vector &result, idx_t index, OperatorType op, Value left_val, Value right_val) {
	auto left_type = left_val.type();
	auto right_type = right_val.type();
	if (IsDecayableType(left_type) || IsDecayableType(right_type)) {
		JSONAllocator alc {Allocator::DefaultAllocator()};
		// Handle for one of two value is decay able type
		Value new_left_val = left_val;
		Value new_right_val = right_val;
		if (left_type == DDVariantType) {
			new_left_val = GetDecayVariantValue(left_val, alc.GetYYAlc());
		} else if (left_type == DDJsonType) {
			new_left_val = GetDecayJsonValue(left_val, alc.GetYYAlc());
		}
		if (right_type == DDVariantType) {
			new_right_val = GetDecayVariantValue(right_val, alc.GetYYAlc());
		} else if (right_type == DDJsonType) {
			new_right_val = GetDecayJsonValue(right_val, alc.GetYYAlc());
		}
		HandleBinaryOpAny(result, index, op, new_left_val, new_right_val);
		return;
	}
	Value res_val;
	switch (op) {
	case OperatorType::ADD_OPERATOR: {
		res_val = AddAnyOperator(left_val, right_val);
	} break;

	case OperatorType::SUB_OPERATOR: {
		res_val = SubtractAnyOperator(left_val, right_val);
	} break;

	case OperatorType::CONCAT_OPERATOR: {
		Vector res_vec(LogicalType::VARCHAR);
		res_val = ConcatAnyOperator(left_val, right_val, res_vec);
	} break;

	case OperatorType::BITWISE_OR: {
		res_val = BitwiseAnyOperator<BitwiseOROperator, bitwise_operator_t, string_t>(op, left_val, right_val,
		                                                                              &Bit::BitwiseOr);
	} break;

	case OperatorType::BITWISE_AND: {
		res_val = BitwiseAnyOperator<BitwiseANDOperator, bitwise_operator_t, string_t>(op, left_val, right_val,
		                                                                               &Bit::BitwiseAnd);
	} break;

	case OperatorType::BITWISE_XOR: {
		res_val = BitwiseAnyOperator<BitwiseXOROperator, bitwise_operator_t, string_t>(op, left_val, right_val,
		                                                                               &Bit::BitwiseXor);
	} break;

	case OperatorType::BITWISE_SHIFT_RIGHT: {
		res_val = BitwiseAnyOperator<BitwiseShiftRightOperator, bitwise_shift_opeartor_t, int32_t>(
		    op, left_val, right_val, &Bit::RightShift);
	} break;

	case OperatorType::BITWISE_SHIFT_LEFT: {
		res_val = BitwiseAnyOperator<BitwiseShiftLeftOperator, bitwise_shift_opeartor_t, int32_t>(
		    op, left_val, right_val, &Bit::LeftShift);
	} break;

	default:
		break;
	}

	VectorWriter writer(result, index);
	if (res_val.IsNull()) {
		writer.SetNull();
	} else {
		VariantWriteValue(writer, res_val);
	}
}

/**
 * @brief Handle Unary Operators for any type
 * type of value is decayable => get original value and handle unary with new value
 * Execute unary operator base on Operator type
 * 		- add operator: (numeric) assign value
 * 		- subtract operator: call negative operator
 * 		- bitwise negation: call bitwise not operator for numeric or byte
 *
 * @param result
 * @param index
 * @param op
 * @param val
 */
static void HandleUnaryOpAny(Vector &result, idx_t index, OperatorType op, Value val) {
	auto value_type = val.type();
	if (IsDecayableType(value_type)) {
		JSONAllocator alc {Allocator::DefaultAllocator()};
		// Handle for one of two value is decay able type
		Value new_val = val;
		if (value_type == DDVariantType) {
			new_val = GetDecayVariantValue(val, alc.GetYYAlc());
		} else if (value_type == DDJsonType) {
			new_val = GetDecayJsonValue(val, alc.GetYYAlc());
		}
		HandleUnaryOpAny(result, index, op, new_val);
		return;
	}

	Value res_val;
	switch (op) {
	case OperatorType::ADD_OPERATOR: {
		if (value_type.IsNumeric()) {
			res_val = val;
		}
	} break;

	case OperatorType::SUB_OPERATOR: {
		if (value_type.id() == LogicalTypeId::INTERVAL) {
			res_val = Value::CreateValue<interval_t>(
			    NegateAnyOperator::Operation<interval_t, interval_t>(val.GetValue<interval_t>()));
		} else if (value_type.id() == LogicalTypeId::DECIMAL) {
			res_val = ExecuteUnaryAnyOperatorNumeric<NegateAnyOperator>(value_type.InternalType(), val);
			DecimalTypeInfo info = value_type.AuxInfo()->Cast<DecimalTypeInfo>();
			res_val = Value::DECIMAL(res_val.GetValue<hugeint_t>(), info.width, info.scale);
		} else if (value_type.IsNumeric()) {
			res_val = ExecuteUnaryAnyOperatorNumeric<NegateAnyOperator>(value_type.InternalType(), val);
		}
	} break;

	case OperatorType::BITWISE_NEGATION: {
		if (value_type.IsIntegral()) {
			res_val = ExecuteUnaryAnyOperatorInteger<BitwiseNotOperator>(value_type.InternalType(), val);
		} else if (value_type == LogicalType::BLOB) {
			auto new_val = val.DefaultCastAs(LogicalType::BIT);
			string_t lhs = new_val.GetValueUnsafe<string_t>();
			string_t target(lhs.GetSize());
			Bit::BitwiseNot(lhs, target);
			res_val = Value::BIT((const_data_ptr_t)target.GetDataUnsafe(), target.GetSize());
			res_val = res_val.DefaultCastAs(LogicalType::BLOB);
		}
	} break;

	default:
		break;
	}

	VectorWriter writer(result, index);
	if (res_val.IsNull()) {
		writer.SetNull();
	} else {
		VariantWriteValue(writer, res_val);
	}
}

/**
 * @brief Main body of binary Operator Any function
 * - Assign operator: assign result vector from lhs argument vector
 * - Other operators: loop each rows and execute operator between two values
 *
 * @param args
 * @param state
 * @param result
 */
static void BinaryOpAnyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType() == DDVariantType);
	auto operator_vec = args.data[0];
	D_ASSERT(operator_vec.GetType() == LogicalType::VARCHAR);
	auto left_vec = args.data[1];

	auto &result_validity = FlatVector::Validity(result);
	idx_t count = args.size();

	UnifiedVectorFormat operator_data, left_data;
	operator_vec.ToUnifiedFormat(count, operator_data);
	left_vec.ToUnifiedFormat(count, left_data);

	auto operators = UnifiedVectorFormat::GetData<string_t>(operator_data);

	if (operator_vec.GetVectorType() != VectorType::CONSTANT_VECTOR) {
		throw Exception(ExceptionType::INVALID, "Operator must be constant!");
	}

	OperatorType op_type = OperatorType::INVALID;
	if (operator_data.validity.RowIsValid(0)) {
		op_type = (OperatorType)StringToOperatorType(operators[0].GetString());
	}

	// Assign operator
	if (op_type == OperatorType::ASSIGN) {
		result.Reinterpret(left_vec);
		return;
	}

	UnifiedVectorFormat right_data;
	auto right_vec = args.data[2];
	right_vec.ToUnifiedFormat(count, right_data);

	// Loop value in vector
	for (idx_t i = 0; i < count; i++) {
		auto op_index = operator_data.sel->get_index(i);
		auto left_index = left_data.sel->get_index(i);
		auto right_index = right_data.sel->get_index(i);
		auto op_valid = operator_data.validity.RowIsValid(op_index);
		auto left_valid = left_data.validity.RowIsValid(left_index);
		auto right_valid = right_data.validity.RowIsValid(right_index);
		if (op_valid && left_valid && right_valid) {
			auto left_value = left_vec.GetValue(left_index);
			auto right_value = right_vec.GetValue(right_index);
			HandleBinaryOpAny(result, i, op_type, left_value, right_value);
		} else {
			result_validity.SetInvalid(i);
		}
	}
}

/**
 * @brief Main body of unary Operator Any function
 * - Assign operator: assign result vector from value vector
 * - Other operators: loop each rows and execute operator for value vector
 *
 * @param args
 * @param state
 * @param result
 */
static void UnaryOpAnyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType() == DDVariantType);
	auto operator_vec = args.data[0];
	D_ASSERT(operator_vec.GetType() == LogicalType::VARCHAR);
	auto value_vec = args.data[1];

	auto &result_validity = FlatVector::Validity(result);
	idx_t count = args.size();

	UnifiedVectorFormat operator_data, value_data;
	operator_vec.ToUnifiedFormat(count, operator_data);
	value_vec.ToUnifiedFormat(count, value_data);

	auto operators = UnifiedVectorFormat::GetData<string_t>(operator_data);

	if (operator_vec.GetVectorType() != VectorType::CONSTANT_VECTOR) {
		throw Exception(ExceptionType::INVALID, "Operator must be constant!");
	}

	OperatorType op_type = OperatorType::INVALID;
	if (operator_data.validity.RowIsValid(0)) {
		op_type = (OperatorType)StringToOperatorType(operators[0].GetString());
	}

	// Assign operator
	if (op_type == OperatorType::ASSIGN) {
		result.Reinterpret(value_vec);
		return;
	}

	// Loop and execute for each row
	for (idx_t i = 0; i < count; i++) {
		auto op_index = operator_data.sel->get_index(i);
		auto value_index = value_data.sel->get_index(i);
		auto op_valid = operator_data.validity.RowIsValid(op_index);
		auto value_valid = value_data.validity.RowIsValid(value_index);
		if (op_valid && value_valid) {
			auto value = value_vec.GetValue(value_index);
			HandleUnaryOpAny(result, i, op_type, value);
		} else {
			result_validity.SetInvalid(i);
		}
	}
}

/**
 * @brief Get the Scalar Decimal Binary Function object base on internal type
 *
 * @tparam OP
 * @param type
 * @return scalar_function_t
 */
template <class OP>
static scalar_function_t GetScalarDecimalBinaryFunction(PhysicalType type) {
	scalar_function_t function;
	switch (type) {
	case PhysicalType::INT128:
		function = &ScalarFunction::BinaryFunction<hugeint_t, hugeint_t, hugeint_t, OP>;
		break;
	case PhysicalType::INT16:
		function = &ScalarFunction::BinaryFunction<int16_t, int16_t, int16_t, OP>;
		break;
	case PhysicalType::INT32:
		function = &ScalarFunction::BinaryFunction<int32_t, int32_t, int32_t, OP>;
		break;
	case PhysicalType::INT64:
		function = &ScalarFunction::BinaryFunction<int64_t, int64_t, int64_t, OP>;
		break;
	default:
		throw Exception(ExceptionType::INVALID, "Invalid Physical type for GetScalarDecimalBinaryFunction");
		break;
	}
	return function;
}

/**
 * @brief Create a Bound Add/Subtract Function Expression object
 *
 * @param context
 * @param left_type
 * @param right_type
 * @param arguments
 * @param op_type
 * @return unique_ptr<Expression>
 */
unique_ptr<Expression> CreateBoundAddSubtractFuncExpression(ClientContext &context, LogicalType left_type,
                                                            LogicalType right_type,
                                                            vector<unique_ptr<Expression>> &arguments,
                                                            OperatorType op_type) {
	vector<unique_ptr<Expression>> func_arguments;
	func_arguments.push_back(std::move(arguments[1]));
	func_arguments.push_back(std::move(arguments[2]));
	auto is_add_func = op_type == OperatorType::ADD_OPERATOR;
	auto add_subtract_function =
	    is_add_func ? AddFun::GetFunction(left_type, right_type) : SubtractFun::GetFunction(left_type, right_type);
	if ((left_type.id() == LogicalTypeId::DECIMAL || right_type.id() == LogicalTypeId::DECIMAL) &&
	    add_subtract_function.function == nullptr) {
		if (left_type.InternalType() == PhysicalType::INT128) {
			add_subtract_function.function =
			    is_add_func ? GetScalarDecimalBinaryFunction<DecimalAddOverflowCheck>(left_type.InternalType())
			                : GetScalarDecimalBinaryFunction<DecimalSubtractOverflowCheck>(left_type.InternalType());
		} else {
			add_subtract_function.function =
			    is_add_func ? GetScalarDecimalBinaryFunction<AddOperator>(left_type.InternalType())
			                : GetScalarDecimalBinaryFunction<SubtractOperator>(left_type.InternalType());
		}
	}
	auto return_type = add_subtract_function.return_type;
	auto result = make_uniq<BoundFunctionExpression>(return_type, std::move(add_subtract_function),
	                                                 std::move(func_arguments), nullptr);
	return std::move(result);
}

/**
 * @brief Create a Bound Operator Function Expression object
 * get best fit function base on logical type of arguments and function name
 * Create bound function expression base on above best function
 *
 * @param context
 * @param types
 * @param func_arguments
 * @param function_name
 * @return unique_ptr<Expression>
 */
unique_ptr<Expression> CreateBoundOperatorFuncExpression(ClientContext &context, vector<LogicalType> types,
                                                         vector<unique_ptr<Expression>> &func_arguments,
                                                         string function_name) {
	QueryErrorContext error_context();
	auto &func =
	    Catalog::GetSystemCatalog(context).GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, function_name);
	D_ASSERT(func.type == CatalogType::SCALAR_FUNCTION_ENTRY);

	ErrorData error;
	FunctionBinder function_binder(context);
	const idx_t best_function = function_binder.BindFunction(func.name, func.functions, types, error);
	if (best_function == DConstants::INVALID_INDEX) {
		string call_str = Function::CallToString(function_name, types);
		string candidate_str = "";
		for (auto &f : func.functions.functions) {
			candidate_str += "\t" + f.ToString() + "\n";
		}
		string str_error =
		    StringUtil::Format("No function matches the given name and argument types '%s'. You might need to add "
		                       "explicit type casts.\n\tCandidate functions:\n%s",
		                       call_str, candidate_str);
		throw BinderException(str_error);
		return nullptr;
	}
	auto bound_function = func.functions.GetFunctionByOffset(best_function);
	unique_ptr<Expression> result = function_binder.BindScalarFunction(bound_function, std::move(func_arguments), true);
	return result;
}

/**
 * @brief Create a Bound Binary Operator Function Expression object
 * Handle left type and right type and calling create bound function
 *
 * @param context
 * @param left_type
 * @param right_type
 * @param arguments
 * @param function_name
 * @return unique_ptr<Expression>
 */
unique_ptr<Expression> CreateBoundBinaryOperatorFuncExpression(ClientContext &context, LogicalType left_type,
                                                               LogicalType right_type,
                                                               vector<unique_ptr<Expression>> &arguments,
                                                               string function_name) {
	vector<unique_ptr<Expression>> func_arguments;
	func_arguments.push_back(std::move(arguments[1]));
	func_arguments.push_back(std::move(arguments[2]));
	return CreateBoundOperatorFuncExpression(context, {left_type, right_type}, func_arguments, function_name);
}

/**
 * @brief Main body of bind function for unary Operator
 *
 * @param context
 * @param value_type
 * @param arguments
 * @param function_name
 * @return unique_ptr<Expression>
 */
unique_ptr<Expression> CreateBoundUnaryOperatorFuncExpression(ClientContext &context, LogicalType value_type,
                                                              vector<unique_ptr<Expression>> &arguments,
                                                              string function_name) {
	vector<unique_ptr<Expression>> func_arguments;
	func_arguments.push_back(std::move(arguments[1]));
	return CreateBoundOperatorFuncExpression(context, {value_type}, func_arguments, function_name);
}

/**
 * @brief Main body of bind function for Binary Operator
 *
 * @param context
 * @param bound_function
 * @param arguments
 * @return unique_ptr<FunctionData>
 */
static unique_ptr<FunctionData> BinaryOpAnyBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 3) {
		throw Exception(ExceptionType::INVALID, "Arguments size must be 3!");
		return nullptr;
	}
	if (arguments[0]->type != ExpressionType::VALUE_CONSTANT) {
		throw Exception(ExceptionType::INVALID, "Operator must be constant!");
		return nullptr;
	}
	auto &bound_const_expr = arguments[0]->Cast<BoundConstantExpression>();
	if (bound_const_expr.return_type != LogicalType::VARCHAR) {
		throw Exception(ExceptionType::INVALID, "Operator must be string!");
		return nullptr;
	}
	string_t str_val = bound_const_expr.value.GetValueUnsafe<string_t>();
	string function_name = str_val.GetString();
	OperatorType op_type = StringToOperatorType(function_name);

	LogicalType left_type = arguments[1]->return_type;
	LogicalType right_type = arguments[2]->return_type;
	// Cast numeric function to same types
	if (left_type.IsNumeric() && right_type.IsNumeric() && left_type.id() != right_type.id()) {
		auto input_type = LogicalType::ForceMaxLogicalType(left_type, right_type);
		arguments[1] = BoundCastExpression::AddCastToType(context, std::move(arguments[1]), input_type,
		                                                  input_type.id() == LogicalTypeId::ENUM);
		arguments[2] = BoundCastExpression::AddCastToType(context, std::move(arguments[2]), input_type,
		                                                  input_type.id() == LogicalTypeId::ENUM);
		left_type = input_type;
		right_type = input_type;
	}
	if (!IsDecayableType(left_type) && !IsDecayableType(right_type)) {
		// Add/subtract: call function from core duckdb code
		if (op_type == OperatorType::ADD_OPERATOR || op_type == OperatorType::SUB_OPERATOR) {
			if (left_type == LogicalType::DATE) {
				if (IntegerCanAddSubDate(right_type)) {
					arguments[2] =
					    BoundCastExpression::AddCastToType(context, std::move(arguments[2]), LogicalType::INTEGER);
					right_type = LogicalType::INTEGER;
				}
			} else if (right_type == LogicalType::DATE && op_type != OperatorType::SUB_OPERATOR) {
				if (IntegerCanAddSubDate(left_type)) {
					arguments[1] =
					    BoundCastExpression::AddCastToType(context, std::move(arguments[1]), LogicalType::INTEGER);
					left_type = LogicalType::INTEGER;
				}
			}
			auto bound_add_expr =
			    CreateBoundAddSubtractFuncExpression(context, left_type, right_type, arguments, op_type);
			arguments[1] = BoundCastExpression::AddCastToType(context, std::move(bound_add_expr), DDVariantType);
			arguments[2] = make_uniq<BoundConstantExpression>(Value::TINYINT((int8_t)OperatorType::ASSIGN));
			arguments[0] = make_uniq<BoundConstantExpression>(Value("="));
			return nullptr;
		} else if ((op_type == OperatorType::CONCAT_OPERATOR || op_type == OperatorType::BITWISE_OR ||
		            op_type == OperatorType::BITWISE_AND || op_type == OperatorType::BITWISE_XOR ||
		            op_type == OperatorType::BITWISE_SHIFT_RIGHT || op_type == OperatorType::BITWISE_SHIFT_LEFT) &&
		           !IsDecayableArrayType(left_type) && !IsDecayableArrayType(right_type)) {
			if (op_type == OperatorType::BITWISE_OR || op_type == OperatorType::BITWISE_AND ||
			    op_type == OperatorType::BITWISE_XOR || op_type == OperatorType::BITWISE_SHIFT_RIGHT ||
			    op_type == OperatorType::BITWISE_SHIFT_LEFT) {
				if (left_type == LogicalType::BLOB) {
					arguments[1] =
					    BoundCastExpression::AddCastToType(context, std::move(arguments[1]), LogicalType::BIT);
					left_type = LogicalType::BIT;
				}
				if (op_type == OperatorType::BITWISE_SHIFT_RIGHT || op_type == OperatorType::BITWISE_SHIFT_LEFT) {
					if (right_type.IsIntegral()) {
						arguments[2] =
						    BoundCastExpression::AddCastToType(context, std::move(arguments[2]), LogicalType::INTEGER);
						right_type = LogicalType::INTEGER;
					}
				} else {
					if (right_type == LogicalType::BLOB) {
						arguments[2] =
						    BoundCastExpression::AddCastToType(context, std::move(arguments[2]), LogicalType::BIT);
						right_type = LogicalType::BIT;
					}
				}
			}
			if (function_name == "^") {
				function_name = "xor";
			}
			unique_ptr<Expression> bound_add_expr =
			    CreateBoundBinaryOperatorFuncExpression(context, left_type, right_type, arguments, function_name);
			if (bound_add_expr->return_type == LogicalType::BIT) {
				bound_add_expr =
				    BoundCastExpression::AddCastToType(context, std::move(bound_add_expr), LogicalType::BLOB);
			}
			arguments[1] = BoundCastExpression::AddCastToType(context, std::move(bound_add_expr), DDVariantType);
			arguments[2] = make_uniq<BoundConstantExpression>(Value::TINYINT((int8_t)OperatorType::ASSIGN));
			arguments[0] = make_uniq<BoundConstantExpression>(Value("="));
			return nullptr;
		}
	}
	return nullptr;
}

/**
 * @brief Main body of bind function for Unary Operator
 *
 * @param context
 * @param bound_function
 * @param arguments
 * @return unique_ptr<FunctionData>
 */
static unique_ptr<FunctionData> UnaryOpAnyBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw Exception(ExceptionType::INVALID, "Arguments size must be 2!");
		return nullptr;
	}
	if (arguments[0]->type != ExpressionType::VALUE_CONSTANT) {
		throw Exception(ExceptionType::INVALID, "Operator must be constant!");
		return nullptr;
	}
	auto &bound_const_expr = arguments[0]->Cast<BoundConstantExpression>();
	if (bound_const_expr.return_type != LogicalType::VARCHAR) {
		throw Exception(ExceptionType::INVALID, "Operator must be string!");
		return nullptr;
	}
	string_t str_val = bound_const_expr.value.GetValueUnsafe<string_t>();
	string function_name = str_val.GetString();
	OperatorType op_type = StringToOperatorType(function_name);

	LogicalType value_type = arguments[1]->return_type;
	if (!IsDecayableType(value_type)) {
		// Call unary function in duckdb core
		unique_ptr<Expression> bound_add_expr =
		    CreateBoundUnaryOperatorFuncExpression(context, value_type, arguments, function_name);
		if (bound_add_expr->return_type == LogicalType::BIT) {
			bound_add_expr = BoundCastExpression::AddCastToType(context, std::move(bound_add_expr), LogicalType::BLOB);
		}
		arguments[1] = BoundCastExpression::AddCastToType(context, std::move(bound_add_expr), DDVariantType);
		arguments[0] = make_uniq<BoundConstantExpression>(Value("="));
		return nullptr;
	}

	return nullptr;
}

void DatadocsExtension::LoadVariantOperators(DatabaseInstance &inst) {
	// Adding `binary_op_any` to catalog
	ScalarFunction binary_op_any_func("binary_op_any", {LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY},
	                                  DDVariantType, BinaryOpAnyFunction, BinaryOpAnyBind);
	ExtensionUtil::RegisterFunction(inst, binary_op_any_func);

	// Adding `unary_op_any` to catalog
	ScalarFunction unary_op_any_func("unary_op_any", {LogicalType::VARCHAR, LogicalType::ANY}, DDVariantType,
	                                 UnaryOpAnyFunction, UnaryOpAnyBind);
	ExtensionUtil::RegisterFunction(inst, unary_op_any_func);
}

} // namespace duckdb
