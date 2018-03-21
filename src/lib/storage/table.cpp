#include "table.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_column.hpp"
#include "index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "index/interval_map/base_interval_map.hpp"
#include "index/interval_map/interval_map.hpp"

namespace opossum {

std::shared_ptr<Table> Table::create_dummy_table(const TableColumnDefinitions& column_definitions) {
  return std::make_shared<Table>(column_definitions, TableType::Data);
}

Table::Table(const TableColumnDefinitions& column_definitions, const TableType type, const uint32_t max_chunk_size,
             const UseMvcc use_mvcc)
    : _column_definitions(column_definitions),
      _type(type),
      _use_mvcc(use_mvcc),
      _max_chunk_size(max_chunk_size),
      _append_mutex(std::make_unique<std::mutex>()) {
  Assert(max_chunk_size > 0, "Table must have a chunk size greater than 0.");
}

const TableColumnDefinitions& Table::column_definitions() const { return _column_definitions; }

TableType Table::type() const { return _type; }

UseMvcc Table::has_mvcc() const { return _use_mvcc; }

size_t Table::column_count() const { return _column_definitions.size(); }

const std::string& Table::column_name(const ColumnID column_id) const {
  DebugAssert(column_id < _column_definitions.size(), "ColumnID out of range");
  return _column_definitions[column_id].name;
}

std::vector<std::string> Table::column_names() const {
  std::vector<std::string> names;
  names.reserve(_column_definitions.size());
  for (const auto& column_definition : _column_definitions) {
    names.emplace_back(column_definition.name);
  }
  return names;
}

DataType Table::column_data_type(const ColumnID column_id) const {
  DebugAssert(column_id < _column_definitions.size(), "ColumnID out of range");
  return _column_definitions[column_id].data_type;
}

std::vector<DataType> Table::column_data_types() const {
  std::vector<DataType> data_types;
  data_types.reserve(_column_definitions.size());
  for (const auto& column_definition : _column_definitions) {
    data_types.emplace_back(column_definition.data_type);
  }
  return data_types;
}

bool Table::column_is_nullable(const ColumnID column_id) const {
  DebugAssert(column_id < _column_definitions.size(), "ColumnID out of range");
  return _column_definitions[column_id].nullable;
}

std::vector<bool> Table::columns_are_nullable() const {
  std::vector<bool> nullable(column_count());
  for (size_t column_idx = 0; column_idx < column_count(); ++column_idx) {
    nullable[column_idx] = _column_definitions[column_idx].nullable;
  }
  return nullable;
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  const auto iter = std::find_if(_column_definitions.begin(), _column_definitions.end(),
                                 [&](const auto& column_definition) { return column_definition.name == column_name; });
  Assert(iter != _column_definitions.end(), "Couldn't find column '" + column_name + "'");
  return static_cast<ColumnID>(std::distance(_column_definitions.begin(), iter));
}

void Table::append(std::vector<AllTypeVariant> values) {
  if (_chunks.empty() || _chunks.back()->size() >= _max_chunk_size) {
    append_mutable_chunk();
  }

  _chunks.back()->append(values);
}

void Table::append_mutable_chunk() {
  ChunkColumns columns;
  for (const auto& column_definition : _column_definitions) {
    resolve_data_type(column_definition.data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      columns.push_back(std::make_shared<ValueColumn<ColumnDataType>>(column_definition.nullable));
    });
  }
  append_chunk(columns);
}

uint64_t Table::row_count() const {
  uint64_t ret = 0;
  for (const auto& chunk : _chunks) {
    ret += chunk->size();
  }
  return ret;
}

bool Table::empty() const { return row_count() == 0u; }

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

const std::vector<std::shared_ptr<Chunk>>& Table::chunks() const { return _chunks; }

uint32_t Table::max_chunk_size() const { return _max_chunk_size; }

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return _chunks[chunk_id];
}

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return _chunks[chunk_id];
}

ProxyChunk Table::get_chunk_with_access_counting(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return ProxyChunk(_chunks[chunk_id]);
}

const ProxyChunk Table::get_chunk_with_access_counting(ChunkID chunk_id) const {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return ProxyChunk(_chunks[chunk_id]);
}

void Table::append_chunk(const ChunkColumns& columns, const std::optional<PolymorphicAllocator<Chunk>>& alloc,
                         const std::shared_ptr<ChunkAccessCounter>& access_counter) {
  const auto chunk_size = columns.empty() ? 0u : columns[0]->size();

#if IS_DEBUG
  for (const auto& column : columns) {
    DebugAssert(column->size() == chunk_size, "Columns don't have the same length");
    const auto is_reference_column = std::dynamic_pointer_cast<ReferenceColumn>(column) != nullptr;
    switch (_type) {
      case TableType::References:
        DebugAssert(is_reference_column, "Invalid column type");
        break;
      case TableType::Data:
        DebugAssert(!is_reference_column, "Invalid column type");
        break;
    }
  }
#endif

  std::shared_ptr<MvccColumns> mvcc_columns;

  if (_use_mvcc == UseMvcc::Yes) {
    mvcc_columns = std::make_shared<MvccColumns>(chunk_size);
  }

  _chunks.emplace_back(std::make_shared<Chunk>(columns, mvcc_columns, alloc, access_counter));
}

std::unique_lock<std::mutex> Table::acquire_append_mutex() { return std::unique_lock<std::mutex>(*_append_mutex); }

std::vector<IndexInfo> Table::get_indexes() const { return _indexes; }

size_t Table::estimate_memory_usage() const {
  auto bytes = size_t{sizeof(*this)};

  for (const auto& chunk : _chunks) {
    bytes += chunk->estimate_memory_usage();
  }

  for (const auto& column_definition : _column_definitions) {
    bytes += column_definition.name.size();
  }

  // TODO(anybody) Statistics and Indices missing from Memory Usage Estimation
  // TODO(anybody) TableLayout missing

  return bytes;
}

std::vector<std::shared_ptr<AbstractTask>> Table::populate_quotient_filters(ColumnID column_id, uint8_t quotient_bits,
                                                                            uint8_t remainder_bits) {
  auto type = column_data_type(column_id);
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count(); ++chunk_id) {
    jobs.push_back(get_chunk(chunk_id)->populate_quotient_filter(column_id, type, quotient_bits, remainder_bits));
  }
  return jobs;
}

void Table::delete_quotient_filters(ColumnID column_id) {
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count(); ++chunk_id) {
    get_chunk(chunk_id)->delete_quotient_filter(column_id);
  }
}

void Table::populate_art_index(ColumnID column_id) {
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count(); ++chunk_id) {
    get_chunk(chunk_id)->populate_art_index(column_id);
  }
}

void Table::delete_art_index(ColumnID column_id) {
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count(); ++chunk_id) {
    get_chunk(chunk_id)->delete_art_index(column_id);
  }
}

void Table::populate_btree_index(ColumnID column_id) {
  auto result = _btree_indices.find(column_id);
  if (result == _btree_indices.end() || result->second == nullptr) {
    //std::cout << "creating btree" << std::endl;
    _btree_indices[column_id] = make_shared_by_data_type<BaseBTreeIndex, BTreeIndex>(column_data_type(column_id),
                                                                                     *this, column_id);
  }
}

void Table::delete_btree_index(ColumnID column_id) {
  _btree_indices[column_id] = nullptr;
}

std::shared_ptr<const BaseBTreeIndex> Table::get_btree_index(ColumnID column_id) const {
  auto result = _btree_indices.find(column_id);
  if (result == _btree_indices.end()) {
    return nullptr;
  } else {
    return result->second;
  }
}

void Table::create_interval_map(ColumnID column_id) {
  auto result = _interval_maps.find(column_id);
  if (result == _interval_maps.end() || result->second == nullptr) {
    //std::cout << "interval map" << std::endl;
    auto interval_map = make_shared_by_data_type<BaseIntervalMap, IntervalMap>(column_data_type(column_id));
    _interval_maps[column_id] = interval_map;
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count(); chunk_id++) {
      auto column = get_chunk(chunk_id)->get_column(column_id);
      interval_map->add_column_chunk(chunk_id, column);
    }
  }
}

void Table::delete_interval_map(ColumnID column_id) {
  _interval_maps[column_id] = nullptr;
}

std::shared_ptr<const BaseIntervalMap> Table::get_interval_map(ColumnID column_id) const {
  auto result = _interval_maps.find(column_id);
  if (result == _interval_maps.end()) {
    return nullptr;
  } else {
    return result->second;
  }
}

uint64_t Table::ma_memory_consumption(ColumnID column_id) const {
  auto memory_consumption = 0;

  // B-Tree
  auto btree = get_btree_index(column_id);
  if (btree != nullptr) {
    memory_consumption += btree->memory_consumption();
  }

  // Interval Map
  auto interval_map = get_interval_map(column_id);
  if (interval_map != nullptr) {
    memory_consumption += interval_map->memory_consumption();
  }

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count(); chunk_id++) {
    auto chunk = get_chunk(chunk_id);

    // Filter
    auto filter = chunk->get_filter(column_id);
    if (filter != nullptr) {
      memory_consumption += filter->memory_consumption();
    }

    // ART
    auto art = std::dynamic_pointer_cast<AdaptiveRadixTreeIndex>(chunk->get_art_index(column_id));
    if (art != nullptr) {
      memory_consumption += art->memory_consumption();
    }

    // Diciontary Column
    auto dictionary_column = std::dynamic_pointer_cast<const BaseDictionaryColumn>(chunk->get_column(column_id));
    if (dictionary_column != nullptr) {
      memory_consumption += dictionary_column->memory_consumption();
    }

    // Value Column
    auto value_column = std::dynamic_pointer_cast<const BaseValueColumn>(chunk->get_column(column_id));
    if (value_column != nullptr) {
      memory_consumption += value_column->memory_consumption();
    }
  }

  return memory_consumption;
}

}  // namespace opossum
