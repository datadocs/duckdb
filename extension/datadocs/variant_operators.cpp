#include "datadocs.hpp"
#include "datadocs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/extra_type_info.hpp";
#include "duckdb/common/operator/add.hpp"
#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

#include <iostream>

namespace duckdb {

enum class OperatorType : uint8_t { INVALID, ADD_OPERATOR, ASSIGN };

OperatorType StringToOperatorType(string str) {
	if (str == "+") {
		return OperatorType::ADD_OPERATOR;
	} else if (str == "=") {
		return OperatorType::ASSIGN;
	}

	throw NotImplementedException("String %s is invalid operator!", str);
	return OperatorType::INVALID;
}

bool IntegerCanAddDate(LogicalType type) {
	if (type.IsIntegral()) {
		if (type != LogicalType::BIGINT && type != LogicalType::UBIGINT && type != LogicalType::HUGEINT) {
			return true;
		}
	}
	return false;
}

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
	default:
		throw NotImplementedException("Unimplemented type for ExecuteBinaryAnyOperator");
	}
	return res;
}

template <class OP>
static Value ExecuteBinaryAnyOperator(PhysicalType type, Value left_val, Value right_val) {
	Value res;
	switch (type) {
	case PhysicalType::INT128:
		res = Value::CreateValue<hugeint_t>(OP::template Operation<hugeint_t, hugeint_t, hugeint_t>(
		    left_val.GetValue<hugeint_t>(), right_val.GetValue<hugeint_t>()));
		break;
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

static Value AddAnyOperator(Value left_val, Value right_val) {
	LogicalType left_type = left_val.type();
	LogicalType right_type = right_val.type();
	Value res;

	Value new_left_val = left_val;
	Value new_right_val = right_val;
	if (left_type.IsNumeric() && right_type.IsNumeric() && left_type.id() != right_type.id()) {
		LogicalType input_type = BoundComparisonExpression::BindComparison(left_type, right_type);
		left_type = input_type;
		right_type = input_type;
		new_left_val = left_val.DefaultCastAs(input_type);
		new_right_val = right_val.DefaultCastAs(input_type);
	}

	if (left_type.IsNumeric() && left_type.id() == right_type.id()) {
		if (left_type.id() == LogicalTypeId::DECIMAL) {
			res = ExecuteBinaryAnyOperator<DecimalAddOverflowCheck>(left_type.InternalType(), left_val, right_val);
			hugeint_t result;
			string *error_message;
			DecimalTypeInfo info = left_type.AuxInfo()->Cast<DecimalTypeInfo>();
			if (!TryCastToDecimal::Operation(res.GetValue<hugeint_t>(), result, error_message, info.width,
			                                 info.scale)) {
				throw Exception("Cast to Decimal fail");
				return nullptr;
			}
			res = Value::DECIMAL(result, info.width, info.scale);
		} else if (left_type.IsIntegral() && left_type.id() != LogicalTypeId::HUGEINT) {
			res = ExecuteBinaryAnyOperatorInteger<AddOperatorOverflowCheck>(left_type.InternalType(), left_val,
			                                                                right_val);
		} else {
			res = ExecuteBinaryAnyOperator<AddOperator>(left_type.InternalType(), left_val, right_val);
		}
	}

	switch (left_type.id()) {
	case LogicalTypeId::DATE:
		if (right_type.id() == LogicalTypeId::INTEGER) {
			res = Value::DATE(AddOperator::Operation<date_t, int32_t, date_t>(left_val.GetValue<date_t>(),
			                                                                  right_val.GetValue<int32_t>()));
		} else if (right_type.id() == LogicalTypeId::INTERVAL) {
			res = Value::DATE(AddOperator::Operation<date_t, interval_t, date_t>(left_val.GetValue<date_t>(),
			                                                                     right_val.GetValue<interval_t>()));
		} else if (right_type.id() == LogicalTypeId::TIME) {
			res = Value::TIMESTAMP(AddOperator::Operation<date_t, dtime_t, timestamp_t>(left_val.GetValue<date_t>(),
			                                                                       right_val.GetValue<dtime_t>()));
		}
		break;
	// case LogicalTypeId::INTEGER:
	// 	if (right_type.id() == LogicalTypeId::DATE) {
	// 		return ScalarFunction("+", {left_type, right_type}, right_type,
	// 		                      ScalarFunction::BinaryFunction<int32_t, date_t, date_t, AddOperator>);
	// 	}
	// 	break;
	// case LogicalTypeId::INTERVAL:
	// 	if (right_type.id() == LogicalTypeId::INTERVAL) {
	// 		return ScalarFunction("+", {left_type, right_type}, LogicalType::INTERVAL,
	// 		                      ScalarFunction::BinaryFunction<interval_t, interval_t, interval_t, AddOperator>);
	// 	} else if (right_type.id() == LogicalTypeId::DATE) {
	// 		return ScalarFunction("+", {left_type, right_type}, LogicalType::DATE,
	// 		                      ScalarFunction::BinaryFunction<interval_t, date_t, date_t, AddOperator>);
	// 	} else if (right_type.id() == LogicalTypeId::TIME) {
	// 		return ScalarFunction("+", {left_type, right_type}, LogicalType::TIME,
	// 		                      ScalarFunction::BinaryFunction<interval_t, dtime_t, dtime_t, AddTimeOperator>);
	// 	} else if (right_type.id() == LogicalTypeId::TIMESTAMP) {
	// 		return ScalarFunction("+", {left_type, right_type}, LogicalType::TIMESTAMP,
	// 		                      ScalarFunction::BinaryFunction<interval_t, timestamp_t, timestamp_t, AddOperator>);
	// 	}
	// 	break;
	// case LogicalTypeId::TIME:
	// 	if (right_type.id() == LogicalTypeId::INTERVAL) {
	// 		return ScalarFunction("+", {left_type, right_type}, LogicalType::TIME,
	// 		                      ScalarFunction::BinaryFunction<dtime_t, interval_t, dtime_t, AddTimeOperator>);
	// 	} else if (right_type.id() == LogicalTypeId::DATE) {
	// 		return ScalarFunction("+", {left_type, right_type}, LogicalType::TIMESTAMP,
	// 		                      ScalarFunction::BinaryFunction<dtime_t, date_t, timestamp_t, AddOperator>);
	// 	}
	// 	break;
	// case LogicalTypeId::TIMESTAMP:
	// 	if (right_type.id() == LogicalTypeId::INTERVAL) {
	// 		return ScalarFunction("+", {left_type, right_type}, LogicalType::TIMESTAMP,
	// 		                      ScalarFunction::BinaryFunction<timestamp_t, interval_t, timestamp_t, AddOperator>);
	// 	}
	// 	break;
	default:
		break;
	}

	return res;
}

static void HandleBinaryOpAny(Vector &result, idx_t index, OperatorType op, Value left_val, Value right_val) {
	auto left_type = left_val.type();
	auto right_type = right_val.type();
	std::cout << "Handle Binary Op Any =========================== " << left_type.ToString() << " and "
	          << right_type.ToString() << std::endl;
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
		std::cout << "new_left_val ==== " << new_left_val.ToString() << " and new_right_val "
		          << new_right_val.ToString() << std::endl;
		HandleBinaryOpAny(result, index, op, new_left_val, new_right_val);
		return;
	}
	switch (op) {
	case OperatorType::ADD_OPERATOR: {
		Value res_val = AddAnyOperator(left_val, right_val);
		std::cout << "Add operator ========================= res is " << res_val.ToString() << std::endl;
		VectorWriter writer(result, index);
		if (res_val.IsNull()) {
			writer.SetNull();
		} else {
			VariantWriteValue(writer, res_val);
		}
	} break;

	default:
		break;
	}
}

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
		throw Exception("Operator must be constant!");
	}

	OperatorType op_type = OperatorType::INVALID;
	if (operator_data.validity.RowIsValid(0)) {
		op_type = (OperatorType)StringToOperatorType(operators[0].GetString());
	}
	std::cout << "Op type ================= " << (int)op_type << std::endl;

	if (op_type == OperatorType::ASSIGN) {
		std::cout << "Binary Op Any Function =================== " << (int)op_type << " and type "
		          << left_vec.GetType().ToString() << std::endl;
		result.Reinterpret(left_vec);
		return;
	}

	UnifiedVectorFormat right_data;
	auto right_vec = args.data[2];
	right_vec.ToUnifiedFormat(count, right_data);

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
		throw Exception("Invalid Physical type for GetScalarDecimalBinaryFunction");
		break;
	}
	return function;
}

unique_ptr<Expression> CreateBoundAddFuncExpression(ClientContext &context, LogicalType left_type,
                                                    LogicalType right_type, vector<unique_ptr<Expression>> &arguments) {
	vector<unique_ptr<Expression>> func_arguments;
	func_arguments.push_back(std::move(arguments[1]));
	func_arguments.push_back(std::move(arguments[2]));
	auto add_function = AddFun::GetFunction(left_type, right_type);
	if ((left_type.id() == LogicalTypeId::DECIMAL || right_type.id() == LogicalTypeId::DECIMAL) &&
	    add_function.function == nullptr) {
		if (left_type.InternalType() == PhysicalType::INT128) {
			add_function.function = GetScalarDecimalBinaryFunction<DecimalAddOverflowCheck>(left_type.InternalType());
		} else {
			add_function.function = GetScalarDecimalBinaryFunction<AddOperator>(left_type.InternalType());
		}
	}
	auto return_type = add_function.return_type;
	auto result =
	    make_uniq<BoundFunctionExpression>(return_type, std::move(add_function), std::move(func_arguments), nullptr);
	return std::move(result);
}

static unique_ptr<FunctionData> BinaryOpAnyBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 3) {
		throw Exception("Arguments size must be 3!");
		return nullptr;
	}
	if (arguments[0]->type != ExpressionType::VALUE_CONSTANT) {
		throw Exception("Operator must be constant!");
		return nullptr;
	}
	auto &bound_const_expr = arguments[0]->Cast<BoundConstantExpression>();
	if (bound_const_expr.return_type != LogicalType::VARCHAR) {
		throw Exception("Operator must be string!");
		return nullptr;
	}
	string_t str_val = bound_const_expr.value.GetValueUnsafe<string_t>();
	OperatorType op_type = StringToOperatorType(str_val.GetString());

	LogicalType left_type = arguments[1]->return_type;
	LogicalType right_type = arguments[2]->return_type;
	if (left_type.IsNumeric() && right_type.IsNumeric() && left_type.id() != right_type.id()) {
		auto input_type = BoundComparisonExpression::BindComparison(left_type, right_type);
		arguments[1] = BoundCastExpression::AddCastToType(context, std::move(arguments[1]), input_type,
		                                                  input_type.id() == LogicalTypeId::ENUM);
		arguments[2] = BoundCastExpression::AddCastToType(context, std::move(arguments[2]), input_type,
		                                                  input_type.id() == LogicalTypeId::ENUM);
		left_type = input_type;
		right_type = input_type;
	}
	if (op_type == OperatorType::ADD_OPERATOR && !IsDecayableType(left_type) && !IsDecayableType(right_type)) {
		if (left_type == LogicalType::DATE) {
			if (IntegerCanAddDate(right_type)) {
				arguments[2] =
				    BoundCastExpression::AddCastToType(context, std::move(arguments[2]), LogicalType::INTEGER);
				right_type = LogicalType::INTEGER;
			}
		} else if (right_type == LogicalType::DATE) {
			if (IntegerCanAddDate(left_type)) {
				arguments[1] =
				    BoundCastExpression::AddCastToType(context, std::move(arguments[1]), LogicalType::INTEGER);
				left_type = LogicalType::INTEGER;
			}
		}
		auto bound_add_expr = CreateBoundAddFuncExpression(context, left_type, right_type, arguments);
		arguments[1] = BoundCastExpression::AddCastToType(context, std::move(bound_add_expr), DDVariantType);
		arguments[2] = make_uniq<BoundConstantExpression>(Value::TINYINT((int8_t)OperatorType::ASSIGN));
		arguments[0] = make_uniq<BoundConstantExpression>(Value("="));
		return nullptr;
	}
	return nullptr;
}

void DatadocsExtension::LoadVariantOperators(DatabaseInstance &inst) {
	std::cout << "Load Variant Operators =================== " << std::endl;
	// Adding `binary_op_any` to catalog
	ScalarFunction binary_op_any_fun("binary_op_any", {LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY},
	                                 DDVariantType, BinaryOpAnyFunction, BinaryOpAnyBind);
	ExtensionUtil::RegisterFunction(inst, binary_op_any_fun);
}

} // namespace duckdb
