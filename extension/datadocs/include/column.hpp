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

class IngestColBIGINT : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return LogicalType::BIGINT;
	}
	bool Write(string_t v) override;
	bool Write(int64_t v) override;
	bool Write(bool v) override;
	bool Write(double v) override;
};

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
	using IngestColDateBase::IngestColDateBase, IngestColDateBase::Write;

	LogicalType GetType() const override {
		return LogicalType::TIMESTAMP;
	}
	bool Write(string_t v) override;
	bool WriteExcelDate(double v) override;
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
	case ColumnType::String  : return new typename T::template Type<IngestColVARCHAR>  (col.name, cur_row);
	case ColumnType::Boolean : return new typename T::template Type<IngestColBOOLEAN>  (col.name, cur_row);
	case ColumnType::Integer : return new typename T::template Type<IngestColBIGINT>   (col.name, cur_row);
	case ColumnType::Decimal : return new typename T::template Type<IngestColDOUBLE>   (col.name, cur_row);
	case ColumnType::Date    : return new typename T::template Type<IngestColDATE>     (col.name, cur_row, col.format);
	case ColumnType::Time    : return new typename T::template Type<IngestColTIME>     (col.name, cur_row, col.format);
	case ColumnType::Datetime: return new typename T::template Type<IngestColTIMESTAMP>(col.name, cur_row, col.format);
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

} // namespace duckdb
