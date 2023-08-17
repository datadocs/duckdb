#include "vector_proxy.hpp"

namespace duckdb {

VectorHolder::VectorHolder(Vector &vec, idx_t count) {
	vec.ToUnifiedFormat(count, data);
	if (vec.GetType().InternalType() == PhysicalType::LIST) {
		child_data.emplace_back(ListVector::GetEntry(vec), ListVector::GetListSize(vec));
	} else if (vec.GetType().InternalType() == PhysicalType::STRUCT) {
		const auto &entries = StructVector::GetEntries(vec);
		child_data.reserve(entries.size());
		for (auto &entry : entries) {
			child_data.emplace_back(*entry, count);
		}
	}
}

VectorHolder::VectorHolder(void *ptr) noexcept : data {ConstantVector::ZeroSelectionVector(), (data_ptr_t)ptr} {
}

VectorReader VectorHolder::operator[](idx_t index) const noexcept {
	return {*this, index};
}

VectorReader::VectorReader(const VectorHolder &holder, idx_t i_row) noexcept
    : holder(holder), i_row(holder.data.sel->get_index(i_row)), i_sel_row(i_row) {
}

bool VectorReader::IsNull() const noexcept {
	return !holder.data.validity.RowIsValid(i_row);
}

std::string_view VectorReader::GetString() const noexcept {
	const string_t &s = Get<string_t>();
	return {s.GetData(), (size_t)s.GetSize()};
}

VectorReader VectorReader::operator[](size_t index) const noexcept {
	D_ASSERT(index < holder.child_data.size());
	return {holder.child_data[index], i_sel_row};
}

uint64_t VectorReader::ListSize() const noexcept {
	D_ASSERT(holder.child_data.size() == 1);
	return Get<list_entry_t>().length;
}

VectorReader::ListIterator VectorReader::begin() const noexcept {
	D_ASSERT(holder.child_data.size() == 1);
	return {holder.child_data[0], Get<list_entry_t>().offset};
}

idx_t VectorReader::end() const noexcept {
	D_ASSERT(holder.child_data.size() == 1);
	const list_entry_t &entry = Get<list_entry_t>();
	return entry.offset + entry.length;
}

VectorReader &VectorReader::operator++() noexcept {
	i_row = holder.data.sel->get_index(++i_sel_row);
	return *this;
}

void VectorReader::SetRow(idx_t index) noexcept {
	i_sel_row = index;
	i_row = holder.data.sel->get_index(i_sel_row);
}

void VectorWriter::SetNull() {
	FlatVector::SetNull(vec, i_row, true);
}

void VectorWriter::SetString(string_t data) {
	FlatVector::GetData<string_t>(vec)[i_row] = StringVector::AddStringOrBlob(vec, data);
}

void VectorWriter::SetVectorString(string_t data) noexcept {
	FlatVector::GetData<string_t>(vec)[i_row] = data;
}

string_t &VectorWriter::ReserveString(idx_t size) {
	return FlatVector::GetData<string_t>(vec)[i_row] = StringVector::EmptyString(vec, size);
}

list_entry_t &VectorWriter::GetList() noexcept {
	D_ASSERT(vec.GetType().id() == LogicalTypeId::LIST);
	return FlatVector::GetData<list_entry_t>(vec)[i_row];
}

VectorListWriter VectorWriter::SetList() noexcept {
	return {(VectorListBuffer &)*vec.GetAuxiliary(), GetList()};
}

VectorStructWriter VectorWriter::SetStruct() noexcept {
	return {StructVector::GetEntries(vec), i_row};
}

VectorListWriter::VectorListWriter(VectorListBuffer &buffer, list_entry_t &entry) noexcept
    : buffer(buffer), entry(entry) {
	entry.offset = buffer.GetSize();
}

VectorListWriter::~VectorListWriter() {
	entry.length = buffer.GetSize() - entry.offset;
}

VectorWriter VectorListWriter::Append() {
	idx_t size = buffer.GetSize();
	buffer.Reserve(size + 1);
	buffer.SetSize(size + 1);
	return {buffer.GetChild(), size};
}

VectorWriter VectorStructWriter::operator[](size_t index) noexcept {
	D_ASSERT(index < children.size());
	return {*children[index], i_row};
}

} // namespace duckdb
