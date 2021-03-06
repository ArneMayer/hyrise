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
#include "resolve_type.hpp"
#include "index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "index/counting_quotient_filter/counting_quotient_filter.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/chunk_statistics/chunk_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

// The last chunk offset is reserved for NULL as used in ReferenceColumns.
const ChunkOffset Chunk::MAX_SIZE = std::numeric_limits<ChunkOffset>::max() - 1;

Chunk::Chunk(const ChunkColumns& columns, std::shared_ptr<MvccColumns> mvcc_columns,
             const std::optional<PolymorphicAllocator<Chunk>>& alloc,
             const std::shared_ptr<ChunkAccessCounter> access_counter)
    : _columns(columns), _mvcc_columns(mvcc_columns), _access_counter(access_counter) {
#if IS_DEBUG
  const auto chunk_size = columns.empty() ? 0u : columns[0]->size();
  Assert(!_mvcc_columns || _mvcc_columns->size() == chunk_size, "Invalid MvccColumns size");
  for (const auto& column : columns) {
    Assert(column->size() == chunk_size, "Columns don't have the same length");
  }
#endif

  if (alloc) _alloc = *alloc;
}

bool Chunk::is_mutable() const {
  return std::all_of(_columns.begin(), _columns.end(),
                     [](const auto& column) { return std::dynamic_pointer_cast<BaseValueColumn>(column) != nullptr; });
}

void Chunk::replace_column(size_t column_id, std::shared_ptr<BaseColumn> column) {
  std::atomic_store(&_columns.at(column_id), column);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(is_mutable(), "Can't append to immutable Chunk");

  // Do this first to ensure that the first thing to exist in a row are the MVCC columns.
  if (has_mvcc_columns()) mvcc_columns()->grow_by(1u, MvccColumns::MAX_COMMIT_ID);

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

const ChunkColumns& Chunk::columns() const { return _columns; }

uint16_t Chunk::column_count() const { return _columns.size(); }

uint32_t Chunk::size() const {
  if (_columns.empty()) return 0;
  auto first_column = get_column(ColumnID{0});
  return first_column->size();
}

bool Chunk::has_mvcc_columns() const { return _mvcc_columns != nullptr; }
bool Chunk::has_access_counter() const { return _access_counter != nullptr; }

SharedScopedLockingPtr<MvccColumns> Chunk::mvcc_columns() {
  DebugAssert((has_mvcc_columns()), "Chunk does not have mvcc columns");

  return {*_mvcc_columns, _mvcc_columns->_mutex};
}

SharedScopedLockingPtr<const MvccColumns> Chunk::mvcc_columns() const {
  DebugAssert((has_mvcc_columns()), "Chunk does not have mvcc columns");

  return {*_mvcc_columns, _mvcc_columns->_mutex};
}

std::vector<std::shared_ptr<BaseIndex>> Chunk::get_indices(
    const std::vector<std::shared_ptr<const BaseColumn>>& columns) const {
  auto result = std::vector<std::shared_ptr<BaseIndex>>();
  std::copy_if(_indices.cbegin(), _indices.cend(), std::back_inserter(result),
               [&](const auto& index) { return index->is_index_for(columns); });
  return result;
}

std::vector<std::shared_ptr<BaseIndex>> Chunk::get_indices(const std::vector<ColumnID> column_ids) const {
  auto columns = get_columns_for_ids(column_ids);
  return get_indices(columns);
}

std::shared_ptr<BaseIndex> Chunk::get_index(const ColumnIndexType index_type,
                                            const std::vector<std::shared_ptr<const BaseColumn>>& columns) const {
  auto index_it = std::find_if(_indices.cbegin(), _indices.cend(), [&](const auto& index) {
    return index->is_index_for(columns) && index->type() == index_type;
  });

  return (index_it == _indices.cend()) ? nullptr : *index_it;
}

std::shared_ptr<BaseIndex> Chunk::get_index(const ColumnIndexType index_type,
                                            const std::vector<ColumnID> column_ids) const {
  auto columns = get_columns_for_ids(column_ids);
  return get_index(index_type, columns);
}

void Chunk::remove_index(std::shared_ptr<BaseIndex> index) {
  auto it = std::find(_indices.cbegin(), _indices.cend(), index);
  DebugAssert(it != _indices.cend(), "Trying to remove a non-existing index");
  _indices.erase(it);
}

bool Chunk::references_exactly_one_table() const {
  if (column_count() == 0) return false;

  auto first_column = std::dynamic_pointer_cast<const ReferenceColumn>(get_column(ColumnID{0}));
  if (first_column == nullptr) return false;
  auto first_referenced_table = first_column->referenced_table();
  auto first_pos_list = first_column->pos_list();

  for (ColumnID column_id{1}; column_id < column_count(); ++column_id) {
    const auto column = std::dynamic_pointer_cast<const ReferenceColumn>(get_column(column_id));
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
  ChunkColumns new_columns(_alloc);
  for (size_t i = 0; i < _columns.size(); i++) {
    new_columns.push_back(_columns.at(i)->copy_using_allocator(_alloc));
  }
  _columns = std::move(new_columns);
}

const PolymorphicAllocator<Chunk>& Chunk::get_allocator() const { return _alloc; }

size_t Chunk::estimate_memory_usage() const {
  auto bytes = size_t{sizeof(*this)};

  for (const auto& column : _columns) {
    bytes += column->estimate_memory_usage();
  }

  // TODO(anybody) Index memory usage missing
  // TODO(anybody) ChunkAccessCounter memory usage missing

  if (_mvcc_columns) {
    bytes += sizeof(_mvcc_columns->tids) + sizeof(_mvcc_columns->begin_cids) + sizeof(_mvcc_columns->end_cids);
    bytes += _mvcc_columns->tids.size() * sizeof(decltype(_mvcc_columns->tids)::value_type);
    bytes += _mvcc_columns->begin_cids.size() * sizeof(decltype(_mvcc_columns->begin_cids)::value_type);
    bytes += _mvcc_columns->end_cids.size() * sizeof(decltype(_mvcc_columns->end_cids)::value_type);
  }

  return bytes;
}

std::vector<std::shared_ptr<const BaseColumn>> Chunk::get_columns_for_ids(
    const std::vector<ColumnID>& column_ids) const {
  DebugAssert(([&]() {
                for (auto column_id : column_ids)
                  if (column_id >= column_count()) return false;
                return true;
              }()),
              "Column IDs not within range [0, column_count()).");

  auto columns = std::vector<std::shared_ptr<const BaseColumn>>{};
  columns.reserve(column_ids.size());
  std::transform(column_ids.cbegin(), column_ids.cend(), std::back_inserter(columns),
                 [&](const auto& column_id) { return get_column(column_id); });
  return columns;
}

std::shared_ptr<ChunkStatistics> Chunk::statistics() const { return _statistics; }

void Chunk::set_statistics(std::shared_ptr<ChunkStatistics> chunk_statistics) {
  DebugAssert(chunk_statistics->statistics().size() == column_count(),
              "ChunkStatistics must have same column amount as Chunk");
  _statistics = chunk_statistics;
}

std::shared_ptr<AbstractTask> Chunk::populate_quotient_filter(ColumnID column_id, DataType column_type,
                                                              uint8_t quotient_bits, uint8_t remainder_bits) {
  return std::make_shared<JobTask>([this, column_id, column_type, quotient_bits, remainder_bits]() {
    _quotient_filters[column_id] = make_shared_by_data_type<BaseFilter, CountingQuotientFilter>(column_type,
                                                                                        quotient_bits, remainder_bits);
    //std::cout << "creating filter" << std::endl;
    _quotient_filters[column_id]->populate(get_column(column_id));
  });
}

void Chunk::delete_quotient_filter(ColumnID column_id) {
  _quotient_filters[column_id] = nullptr;
}

std::shared_ptr<const BaseFilter> Chunk::get_filter(ColumnID column_id) const {
  auto result = _quotient_filters.find(column_id);
  if (result == _quotient_filters.end()) {
    return nullptr;
  } else {
    return result->second;
  }
}

std::shared_ptr<BaseIndex> Chunk::get_art_index(ColumnID column_id) const {
  auto column = get_column(column_id);
  for (size_t i = 0; i < _art_indices.size(); i++) {
    if (_art_indices[i]->is_index_for(std::vector({column}))) {
      return _art_indices[i];
    }
  }
  return nullptr;
}

void Chunk::populate_art_index(ColumnID column_id) {
  auto column = get_column(column_id);
  // Dont create a new one if it is already there
  for (size_t i = 0; i < _art_indices.size(); i++) {
    if (_art_indices[i]->is_index_for(std::vector({column}))) {
      return;
    }
  }
  //std::cout << "creating art" << std::endl;
  auto index = std::make_shared<AdaptiveRadixTreeIndex>(std::vector({column}));
  _art_indices.emplace_back(index);
}

void Chunk::delete_art_index(ColumnID column_id) {
  auto column = get_column(column_id);
  for (size_t i = 0; i < _art_indices.size(); i++) {
    if (_art_indices[i]->is_index_for(std::vector({column}))) {
      auto iterator = _art_indices.begin() + i;
      _art_indices.erase(iterator);
    }
  }
}

}  // namespace opossum
