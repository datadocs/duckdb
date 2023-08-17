#pragma once
#include "duckdb.hpp"

#include <iterator>
#include <stddef.h>
#include <string_view>
#include <type_traits>
#include <utility>

namespace duckdb {

class VectorReader;

class VectorHolder {
	friend class VectorReader;

public:
	VectorHolder(Vector &vec, idx_t count);
	explicit VectorHolder(void *ptr) noexcept;
	explicit VectorHolder(const std::string_view &s) noexcept : VectorHolder(string_t(s.data(), s.size())) {
	}

	template <typename T>
	explicit VectorHolder(const T &value) noexcept : VectorHolder((void *)&value) {
	}

	VectorHolder(VectorHolder &&) noexcept = default;
	VectorHolder &operator=(VectorHolder &&) noexcept = default;

	VectorReader operator[](idx_t index) const noexcept;

private:
	UnifiedVectorFormat data;
	std::vector<VectorHolder> child_data;
};

class VectorReader {
	class ListIterator;

public:
	VectorReader(const VectorHolder &holder, idx_t i_row) noexcept;
	VectorReader(const VectorHolder &holder) noexcept : holder(holder) {
	}

	template <typename T>
	const T &Get() const noexcept {
		return reinterpret_cast<const T *>(holder.data.data)[i_row];
	}

	bool IsNull() const noexcept;
	std::string_view GetString() const noexcept;
	VectorReader operator[](size_t index) const noexcept;
	uint64_t ListSize() const noexcept;
	ListIterator begin() const noexcept;
	idx_t end() const noexcept;
	VectorReader &operator++() noexcept;
	void SetRow(idx_t index) noexcept;

private:
	const VectorHolder &holder;
	idx_t i_row;
	idx_t i_sel_row;
};

class VectorReader::ListIterator {
public:
	using iterator = ListIterator;
	using iterator_category = std::input_iterator_tag;
	using difference_type = ptrdiff_t;
	using value_type = VectorReader;
	using reference = const value_type &;
	using pointer = const value_type *;
	// clang-format off
	ListIterator(const VectorHolder &holder, idx_t i_row) noexcept : reader(holder, i_row) {}
	iterator &operator++() noexcept { ++reader; return *this; }
	iterator operator++(int) noexcept { iterator retval = *this; ++(*this); return retval; }
	bool operator==(idx_t end) const noexcept { return reader.i_sel_row == end; }
	bool operator!=(idx_t end) const noexcept { return reader.i_sel_row != end; }
	reference operator*() const noexcept { return reader; }
	// clang-format on
private:
	VectorReader reader;
};

class VectorListWriter;
class VectorStructWriter;

class VectorWriter {
public:
	VectorWriter(Vector &vec, idx_t i_row) noexcept : vec(vec), i_row(i_row) {
		D_ASSERT(vec.GetVectorType() == VectorType::FLAT_VECTOR);
	}

	void SetNull();
	void SetString(string_t data);
	void SetVectorString(string_t data) noexcept;
	string_t &ReserveString(idx_t size);
	list_entry_t &GetList() noexcept;
	VectorListWriter SetList() noexcept;
	VectorStructWriter SetStruct() noexcept;

	template <typename T>
	void Set(const T &v) noexcept {
		D_ASSERT(vec.GetType().InternalType() == GetTypeId<T>());
		FlatVector::GetData<T>(vec)[i_row] = v;
	}

	template <typename T>
	T &Get() noexcept {
		D_ASSERT(vec.GetType().InternalType() == GetTypeId<T>());
		return FlatVector::GetData<T>(vec)[i_row];
	}

protected:
	Vector &vec;
	idx_t i_row;
};

class VectorListWriter {
public:
	VectorListWriter(VectorListBuffer &buffer, list_entry_t &entry) noexcept;
	~VectorListWriter();

	VectorWriter Append();

private:
	VectorListBuffer &buffer;
	list_entry_t &entry;
};

class VectorStructWriter {
public:
	VectorStructWriter(std::vector<unique_ptr<Vector>> &children, idx_t i_row) noexcept : children(children), i_row(i_row) {
	}

	VectorWriter operator[](size_t index) noexcept;

private:
	std::vector<unique_ptr<Vector>> &children;
	idx_t i_row;
};

template <bool CHECK_NULL, typename FUNC, size_t... IDXS>
void VectorExecuteImpl(DataChunk &args, Vector &result, FUNC &&func, std::index_sequence<IDXS...>) {
	constexpr size_t N_ARG = sizeof...(IDXS);
	D_ASSERT(args.ColumnCount() == N_ARG);
	bool constant = (... && (args.data[IDXS].GetVectorType() == VectorType::CONSTANT_VECTOR));
	idx_t size = constant ? 1 : args.size();
	VectorHolder holders[N_ARG] = {{args.data[IDXS], size}...};
	VectorReader readers[N_ARG] = {{holders[IDXS]}...};
	for (idx_t i_row = 0; i_row < size; ++i_row) {
		(..., readers[IDXS].SetRow(i_row));
		VectorWriter writer(result, i_row);
		if (CHECK_NULL && (... || readers[IDXS].IsNull()) || !func(writer, readers[IDXS]...)) {
			writer.SetNull();
		}
	}
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

template <bool CHECK_NULL = true, typename... ARGS>
void VectorExecute(DataChunk &args, Vector &result, bool (*func)(VectorWriter &, ARGS...)) {
	VectorExecuteImpl<CHECK_NULL>(args, result, func, std::index_sequence_for<ARGS...>());
}

template <bool CHECK_NULL = true, class T, typename... ARGS, typename E = typename std::remove_reference<T>::type>
void VectorExecute(DataChunk &args, Vector &result, T &&instance, bool (E::*method)(VectorWriter &, ARGS...)) {
	VectorExecuteImpl<CHECK_NULL>(
	    args, result, [&](auto &&...args) { return (instance.*method)(std::forward<decltype(args)>(args)...); },
	    std::index_sequence_for<ARGS...>());
}

template <bool CHECK_NULL = true, class T, typename... ARGS, typename E = typename std::remove_reference<T>::type>
void VectorCastExecute(VectorReader &reader, Vector &result, T &&instance, idx_t i_row,
                       bool (E::*method)(VectorWriter &, ARGS...)) {
	VectorWriter writer(result, i_row);
	if (CHECK_NULL && reader.IsNull() || !(instance.*method)(writer, reader)) {
		writer.SetNull();
	}
}

} // namespace duckdb
