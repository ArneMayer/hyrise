#include <algorithm>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_column.hpp"
#include "chunk.hpp"
#include "index/base_index.hpp"
#include "reference_column.hpp"
#include "utils/assert.hpp"

namespace opossum {

const CommitID Chunk::MAX_COMMIT_ID = std::numeric_limits<CommitID>::max();

uint64_t Chunk::AccessCounter::history_sample(size_t lookback) const {
  if (_history.size() < 2 || lookback == 0) return 0;
  const auto last = _history.back();
  const auto prelast_index = std::max(0l, static_cast<int64_t>(_history.size()) - static_cast<int64_t>(lookback));
  const auto prelast = _history.at(prelast_index);
  return last - prelast;
}

Chunk::Chunk() : Chunk(PolymorphicAllocator<Chunk>(), false) {}

Chunk::Chunk(const bool has_mvcc_columns) : Chunk(PolymorphicAllocator<Chunk>(), has_mvcc_columns) {}

Chunk::Chunk(const PolymorphicAllocator<Chunk>& alloc, const std::shared_ptr<AccessCounter> access_counter,
             const bool has_mvcc_columns)
    : _alloc(alloc), _access_counter(access_counter) {
  if (has_mvcc_columns) _mvcc_columns = std::make_shared<MvccColumns>();
}

Chunk::Chunk(const PolymorphicAllocator<Chunk>& alloc, const bool has_mvcc_columns, const bool has_access_counter)
    : _alloc(alloc), _columns(alloc), _indices(alloc) {
  if (has_mvcc_columns) _mvcc_columns = std::make_shared<MvccColumns>();
  if (has_access_counter) _access_counter = std::make_shared<AccessCounter>(alloc);
}

void Chunk::add_column(std::shared_ptr<BaseColumn> column) {
  // The added column must have the same size as the chunk.
  DebugAssert((_columns.size() <= 0 || size() == column->size()),
              "Trying to add column with mismatching size to chunk");

  if (_columns.size() == 0 && has_mvcc_columns()) grow_mvcc_column_size_by(column->size(), 0);

  _columns.push_back(column);
}

void Chunk::replace_column(size_t column_id, std::shared_ptr<BaseColumn> column) {
  std::atomic_store(&_columns.at(column_id), column);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  // Do this first to ensure that the first thing to exist in a row are the MVCC columns.
  if (has_mvcc_columns()) grow_mvcc_column_size_by(1u, Chunk::MAX_COMMIT_ID);

  // The added values, i.e., a new row, must have the same number of attributes as the table.
  DebugAssert((_columns.size() == values.size()),
              ("append: number of columns (" + std::to_string(_columns.size()) + ") does not match value list (" +
               std::to_string(values.size()) + ")"));

  auto column_it = _columns.cbegin();
  auto value_it = values.begin();
  for (; column_it != _columns.end(); column_it++, value_it++) {
    (*column_it)->append(*value_it);
  }
}

std::shared_ptr<BaseColumn> Chunk::get_mutable_column(ColumnID column_id) const {
  return std::atomic_load(&_columns.at(column_id));
}

std::shared_ptr<const BaseColumn> Chunk::get_column(ColumnID column_id) const {
  return std::atomic_load(&_columns.at(column_id));
}

uint16_t Chunk::column_count() const { return _columns.size(); }

uint32_t Chunk::size() const {
  if (_columns.empty()) return 0;
  auto first_column = get_column(ColumnID{0});
  return first_column->size();
}

void Chunk::grow_mvcc_column_size_by(size_t delta, CommitID begin_cid) {
  auto mvcc_columns = this->mvcc_columns();

  mvcc_columns->tids.grow_to_at_least(size() + delta);
  mvcc_columns->begin_cids.grow_to_at_least(size() + delta, begin_cid);
  mvcc_columns->end_cids.grow_to_at_least(size() + delta, MAX_COMMIT_ID);
}

void Chunk::use_mvcc_columns_from(const Chunk& chunk) {
  Assert(chunk.has_mvcc_columns(), "Passed chunk needs to have mvcc columns.");
  _mvcc_columns = chunk._mvcc_columns;
}

bool Chunk::has_mvcc_columns() const { return _mvcc_columns != nullptr; }
bool Chunk::has_access_counter() const { return _access_counter != nullptr; }

SharedScopedLockingPtr<Chunk::MvccColumns> Chunk::mvcc_columns() {
  DebugAssert((has_mvcc_columns()), "Chunk does not have mvcc columns");

  return {*_mvcc_columns, _mvcc_columns->_mutex};
}

SharedScopedLockingPtr<const Chunk::MvccColumns> Chunk::mvcc_columns() const {
  DebugAssert((has_mvcc_columns()), "Chunk does not have mvcc columns");

  return {*_mvcc_columns, _mvcc_columns->_mutex};
}

void Chunk::shrink_mvcc_columns() {
  DebugAssert((has_mvcc_columns()), "Chunk does not have mvcc columns");

  std::unique_lock<std::shared_mutex> lock{_mvcc_columns->_mutex};

  _mvcc_columns->tids.shrink_to_fit();
  _mvcc_columns->begin_cids.shrink_to_fit();
  _mvcc_columns->end_cids.shrink_to_fit();
}

std::vector<std::shared_ptr<BaseIndex>> Chunk::get_indices_for(
    const std::vector<std::shared_ptr<const BaseColumn>>& columns) const {
  auto result = std::vector<std::shared_ptr<BaseIndex>>();
  std::copy_if(_indices.cbegin(), _indices.cend(), std::back_inserter(result),
               [&columns](const std::shared_ptr<BaseIndex>& index) { return index->is_index_for(columns); });
  return result;
}

bool Chunk::references_exactly_one_table() const {
  if (column_count() == 0) return false;

  auto first_column = std::dynamic_pointer_cast<const ReferenceColumn>(get_column(ColumnID{0}));
  if (first_column == nullptr) return false;
  auto first_referenced_table = first_column->referenced_table();
  auto first_pos_list = first_column->pos_list();

  for (ColumnID i{1}; i < column_count(); ++i) {
    const auto column = std::dynamic_pointer_cast<const ReferenceColumn>(get_column(i));
    if (column == nullptr) return false;

    if (first_referenced_table != column->referenced_table()) return false;

    if (first_pos_list != column->pos_list()) return false;
  }

  return true;
}

void Chunk::migrate(boost::container::pmr::memory_resource* memory_source) {
  // Migrating chunks with indices is not implemented yet.
  if (_indices.size() > 0) {
    Fail("Cannot migrate Chunk with Indices.");
  }

  _alloc = PolymorphicAllocator<size_t>(memory_source);
  pmr_concurrent_vector<std::shared_ptr<BaseColumn>> new_columns(_alloc);
  for (size_t i = 0; i < _columns.size(); i++) {
    new_columns.push_back(_columns.at(i)->copy_using_allocator(_alloc));
  }
  _columns = std::move(new_columns);
}

const PolymorphicAllocator<Chunk>& Chunk::get_allocator() const { return _alloc; }

}  // namespace opossum
