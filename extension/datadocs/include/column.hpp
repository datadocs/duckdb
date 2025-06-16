#pragma once
#include <utility>
#include <memory>

#include "duckdb.hpp"
#include "json_common.hpp"

#include "datadocs.hpp"
#include "vector_proxy.hpp"
#include "type_conv.h"
#include "inferrer.h"

namespace duckdb {

class IngestColBase {
public:
	IngestColBase(string name, idx_t &cur_row) noexcept : vec(nullptr), cur_row(cur_row), name(std::move(name)) {
	}
	virtual ~IngestColBase() = default;

	void WriteNull() {
		Writer().SetNull();
	}
	virtual bool Write(string_t v) {
		return false;
	}
	virtual bool Write(int64_t v) {
		return false;
	}
	virtual bool Write(bool v) {
		return false;
	}
	virtual bool Write(double v) {
		return false;
	}
	virtual bool WriteExcelDate(double v) {
		return false;
	}
	virtual void SetVector(Vector *new_vec) noexcept {
		D_ASSERT(new_vec->GetType() == GetType());
		vec = new_vec;
	}

	virtual LogicalType GetType() const {
		return LogicalType::SQLNULL;
	}
	const string &GetName() const noexcept {
		return name;
	}

protected:
	Vector &GetVector() noexcept {
		D_ASSERT(vec);
		return *vec;
	}
	VectorWriter Writer() noexcept {
		return {GetVector(), cur_row};
	}

private:
	Vector *vec;
	idx_t &cur_row;
	string name;
};

class IngestColVARCHAR : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return LogicalType::VARCHAR;
	};
	bool Write(string_t v) override;
	bool Write(int64_t v) override;
	bool Write(bool v) override;
	bool Write(double v) override;
	bool WriteExcelDate(double v) override;
};

class IngestColBOOLEAN : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return LogicalType::BOOLEAN;
	}
	bool Write(string_t v) override;
	bool Write(int64_t v) override;
	bool Write(bool v) override;
};

template <typename T, LogicalTypeId TYPE_ID>
class IngestColInteger : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return TYPE_ID;
	}

	bool Write(string_t v) override {
		T result;
		if (!TryCast::Operation(v, result, true)) {
			std::string buffer;
			if (!parse_money(v.GetData(), v.GetSize(), buffer) || !TryCast::Operation(string_t(buffer), result, true)) {
				return false;
			}
		}
		Writer().Set(result);
		return true;
	}

	bool Write(int64_t v) override {
		if constexpr (std::is_same_v<T, uint64_t>) {
			if (v < 0) {
				return false;
			}
		} else if constexpr(!std::is_same_v<T, int64_t>) {
			if (v < std::numeric_limits<T>::min() || v > std::numeric_limits<T>::max()) {
				return false;
			}
		}
		Writer().Set(static_cast<T>(v));
		return true;
	}

	bool Write(bool v) override {
		Writer().Set(static_cast<T>(v));
		return true;
	}

	bool Write(double v) override {
		if (v < std::numeric_limits<T>::min() || v > std::numeric_limits<T>::max() || v != std::trunc(v)) {
			return false;
		}
		Writer().Set(static_cast<T>(v));
		return true;
	}
};

typedef IngestColInteger<int64_t, LogicalTypeId::BIGINT> IngestColBIGINT;

class IngestColDOUBLE : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return LogicalType::DOUBLE;
	}
	bool Write(string_t v) override;
	bool Write(int64_t v) override;
	bool Write(bool v) override;
	bool Write(double v) override;
};

class IngestColDateBase : public IngestColBase {
public:
	using IngestColBase::Write;

	IngestColDateBase(string name, idx_t &cur_row, string format) noexcept
	    : IngestColBase(std::move(name), cur_row), format(std::move(format)) {
	}

protected:
	string format;
};

class IngestColDATE : public IngestColDateBase {
public:
	using IngestColDateBase::IngestColDateBase, IngestColDateBase::Write;

	LogicalType GetType() const override {
		return LogicalType::DATE;
	}
	bool Write(string_t v) override;
	bool WriteExcelDate(double v) override;
};

class IngestColTIME : public IngestColDateBase {
public:
	using IngestColDateBase::IngestColDateBase, IngestColDateBase::Write;

	LogicalType GetType() const override {
		return LogicalType::TIME;
	}
	bool Write(string_t v) override;
	bool WriteExcelDate(double v) override;
};

class IngestColTIMESTAMP : public IngestColDateBase {
public:
	using IngestColDateBase::Write;

	IngestColTIMESTAMP(string name, idx_t &cur_row, string format, LogicalTypeId type_id) noexcept
	    : IngestColDateBase(std::move(name), cur_row, std::move(format)), type(type_id) {
	}

	LogicalType GetType() const override {
		return type;
	}
	bool Write(string_t v) override;
	bool WriteExcelDate(double v) override;

protected:
	const LogicalType type;
};

class IngestColINTERVAL : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return LogicalType::INTERVAL;
	}
	bool Write(string_t v) override;
	bool WriteExcelDate(double v) override;
};

class IngestColINTERVALFormat : public IngestColDateBase {
public:
	using IngestColDateBase::IngestColDateBase, IngestColDateBase::Write;

	LogicalType GetType() const override {
		return LogicalType::INTERVAL;
	}
	bool Write(string_t v) override;
	bool WriteExcelDate(double v) override;
};

class IngestColINTERVALISO : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return LogicalType::INTERVAL;
	}
	bool Write(string_t v) override;
};

class IngestColBLOBBase64 : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return LogicalType::BLOB;
	}
	bool Write(string_t v) override;
};

class IngestColBLOBHex : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return LogicalType::BLOB;
	}
	bool Write(string_t v) override;
};

class IngestColNUMERICBase : public IngestColBase {
public:
	using IngestColBase::Write;

	IngestColNUMERICBase(string name, idx_t &cur_row, uint8_t width, uint8_t scale) noexcept
	    : IngestColBase(std::move(name), cur_row), width(width), scale(scale) {
	}

	LogicalType GetType() const override {
		return LogicalType::DECIMAL(width, scale);
	}

	bool Write(bool v) override {
		return Write((int64_t)v);
	}

protected:
	uint8_t width;
	uint8_t scale;
};

template <typename T>
class IngestColNUMERIC : public IngestColNUMERICBase {
public:
	using IngestColNUMERICBase::IngestColNUMERICBase, IngestColNUMERICBase::Write;

	bool Write(string_t v) override {
		string message;
		CastParameters parameters(false, &message);
		if (!TryCastToDecimal::Operation(v, Writer().template Get<T>(), parameters, width, scale)) {
			string buffer;
			return parse_money(v.GetData(), v.GetSize(), buffer) &&
			    TryCastToDecimal::Operation(string_t(buffer), Writer().template Get<T>(), parameters, width, scale);
		}
		return true;
	}

	bool Write(int64_t v) override {
		string message;
		CastParameters parameters(false, &message);
		return TryCastToDecimal::Operation(v, Writer().template Get<T>(), parameters, width, scale);
	}

	bool Write(double v) override {
		string message;
		CastParameters parameters(false, &message);
		return TryCastToDecimal::Operation(v, Writer().template Get<T>(), parameters, width, scale);
	}
};

class IngestColGEO : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return DDGeoType;
	}
	bool Write(string_t v) override;
};

class IngestColJSON : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return DDJsonType;
	}
	bool Write(string_t v) override;
	bool Write(int64_t v) override;
	bool Write(bool v) override;
	bool Write(double v) override;
	bool WriteExcelDate(double v) override;

private:
	JSONAllocator alc {Allocator::DefaultAllocator()};
};

class IngestColVariant : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return DDVariantType;
	}
	bool Write(string_t v) override;
	bool Write(int64_t v) override;
	bool Write(bool v) override;
	bool Write(double v) override;
	bool WriteExcelDate(double v) override;
};

struct IngestColChildrenMap {
	void Clear() {
		valid.assign(keys.size(), false);
		cnt_valid = 0;
	}
	size_t GetIndex(const string &s) {
		auto it = keys.find(s);
		if (it == keys.end())
			return -1;
		if (!valid[it->second]) {
			valid[it->second] = true;
			++cnt_valid;
		}
		return it->second;
	}

	std::unordered_map<string, size_t> keys;
	size_t cnt_valid;
	std::vector<bool> valid;
};

template <class T>
typename T::ReturnType *BuildColumn(const IngestColumnDefinition &col, idx_t &cur_row) {
	switch(col.column_type) {
	case ColumnType::String : return new typename T::template Type<IngestColVARCHAR>(col.name, cur_row);
	case ColumnType::Boolean: return new typename T::template Type<IngestColBOOLEAN>(col.name, cur_row);
	case ColumnType:: Int8: return new typename T::template Type<IngestColInteger< int8_t, LogicalTypeId:: TINYINT>> (col.name, cur_row);
	case ColumnType::UInt8: return new typename T::template Type<IngestColInteger<uint8_t, LogicalTypeId::UTINYINT>> (col.name, cur_row);
	case ColumnType:: Int16: return new typename T::template Type<IngestColInteger< int16_t, LogicalTypeId:: SMALLINT>> (col.name, cur_row);
	case ColumnType::UInt16: return new typename T::template Type<IngestColInteger<uint16_t, LogicalTypeId::USMALLINT>> (col.name, cur_row);
	case ColumnType:: Int32: return new typename T::template Type<IngestColInteger< int32_t, LogicalTypeId:: INTEGER>> (col.name, cur_row);
	case ColumnType::UInt32: return new typename T::template Type<IngestColInteger<uint32_t, LogicalTypeId::UINTEGER>> (col.name, cur_row);
	case ColumnType:: Int64: return new typename T::template Type<IngestColInteger< int64_t, LogicalTypeId:: BIGINT>> (col.name, cur_row);
	case ColumnType::UInt64: return new typename T::template Type<IngestColInteger<uint64_t, LogicalTypeId::UBIGINT>> (col.name, cur_row);
	case ColumnType::Decimal: return new typename T::template Type<IngestColDOUBLE> (col.name, cur_row);
	case ColumnType::Date   : return new typename T::template Type<IngestColDATE>   (col.name, cur_row, col.format);
	case ColumnType::Time   : return new typename T::template Type<IngestColTIME>   (col.name, cur_row, col.format);
	case ColumnType::Datetime:
		return new typename T::template Type<IngestColTIMESTAMP>(col.name, cur_row, col.format, LogicalType::TIMESTAMP);
	case ColumnType::Datetime_tz:
		return new typename T::template Type<IngestColTIMESTAMP>(col.name, cur_row, col.format, LogicalType::TIMESTAMP_TZ);
	case ColumnType::Interval:
		if (col.format.empty()) {
			return new typename T::template Type<IngestColINTERVAL>(col.name, cur_row);
		} else if (col.format == "ISO") {
			return new typename T::template Type<IngestColINTERVALISO>(col.name, cur_row);
		} else {
			return new typename T::template Type<IngestColINTERVALFormat>(col.name, cur_row, col.format);
		}
	case ColumnType::Bytes:
		if (col.format == "base64") {
			return new typename T::template Type<IngestColBLOBBase64>(col.name, cur_row);
		}
		return new typename T::template Type<IngestColBLOBHex>(col.name, cur_row);
	case ColumnType::Numeric: {
		uint8_t f_digits = col.f_digits;
		int need_width = col.i_digits + f_digits;
		if (need_width <= Decimal::MAX_WIDTH_INT16) {
			return new typename T::template Type<IngestColNUMERIC<int16_t>>(
			    col.name, cur_row, Decimal::MAX_WIDTH_INT16, f_digits);
		} else if (need_width <= Decimal::MAX_WIDTH_INT32) {
			return new typename T::template Type<IngestColNUMERIC<int32_t>>(
			    col.name, cur_row, Decimal::MAX_WIDTH_INT32, f_digits);
		} else if (need_width <= Decimal::MAX_WIDTH_INT64) {
			return new typename T::template Type<IngestColNUMERIC<int64_t>>(
			    col.name, cur_row, Decimal::MAX_WIDTH_INT64, f_digits);
		} else {
			if (need_width > Decimal::MAX_WIDTH_DECIMAL) {
				f_digits = MaxValue(0, Decimal::MAX_WIDTH_DECIMAL - col.i_digits);
			}
			return new typename T::template Type<IngestColNUMERIC<hugeint_t>>(
			    col.name, cur_row, Decimal::MAX_WIDTH_DECIMAL, f_digits);
		}
	}
	case ColumnType::Variant: return new typename T::template Type<IngestColVariant>(col.name, cur_row);
	default:
		D_ASSERT(false);
		return new typename T::template Type<IngestColBase>(col.name, cur_row);
	}
}

struct ColumnBuilder {
	using ReturnType = IngestColBase;
	template <typename T> using Type = T;

	static ReturnType *Build(const IngestColumnDefinition &col, idx_t &cur_row);
};

class IngestColErrors : public IngestColBase {
public:
	IngestColErrors(idx_t &cur_row) noexcept
	    : IngestColBase("__errors__", cur_row), child_key("", list_row), child_value("", list_row) {
	}

	virtual void SetVector(Vector *new_vec) noexcept override {
		IngestColBase::SetVector(new_vec);
		buffer = (VectorListBuffer *)(new_vec->GetAuxiliary().get());
		Vector &child_vector = buffer->GetChild();
		const auto &entries = StructVector::GetEntries(child_vector);
		child_key.SetVector(entries[0].get());
		child_value.SetVector(entries[1].get());
	}

	virtual LogicalType GetType() const override;

	void WriteColumnName(string_t column) {
		list_row = buffer->GetSize();
		buffer->Reserve(list_row + 1);
		buffer->SetSize(list_row + 1);
		auto &entry = Writer().GetList();
		if (!m_have_error) {
			m_have_error = true;
			entry.offset = list_row;
			entry.length = 1;
		} else {
			++entry.length;
		}
		child_key.Write(column);
	}

	void WriteError(string_t column, string_t value) {
		WriteColumnName(column);
		child_value.Write(value);
	}

	virtual bool Write(string_t v) { return child_value.Write(v); }
	virtual bool Write(int64_t v) { return child_value.Write(v); }
	virtual bool Write(bool v) { return child_value.Write(v); }
	virtual bool Write(double v) { return child_value.Write(v); }
	virtual bool WriteExcelDate(double v) { return child_value.Write(v); }

	void Reset() {
		if (m_have_error) {
			m_have_error = false;
		} else {
			WriteNull();
		}
	}

private:
	bool m_have_error = false;
	VectorListBuffer *buffer = nullptr;
	IngestColVARCHAR child_key;
	IngestColVARCHAR child_value;
	idx_t list_row;
};

} // namespace duckdb
