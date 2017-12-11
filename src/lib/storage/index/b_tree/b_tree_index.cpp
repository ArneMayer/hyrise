#include "b_tree_index.hpp"

#include "storage/base_attribute_vector.hpp"
#include "storage/base_column.hpp"
#include "storage/base_dictionary_column.hpp"
#include "storage/index/base_index.hpp"
#include "types.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"
#include "storage/iterables/create_iterable_from_column.hpp"

namespace opossum {

template <typename DataType>
BTreeIndex<DataType>::BTreeIndex(const std::vector<std::shared_ptr<const BaseColumn>>& index_columns)
    : BaseBTreeIndex{index_columns} {
  DebugAssert((index_columns.size() == 1), "BTree only works with a single column");
  resolve_column_type<DataType>(*_index_column, [&](const auto& typed_column) {
    auto iterable_left = create_iterable_from_column<DataType>(typed_column);
    iterable_left.for_each([&](const auto& value) {
      if (value.is_null()) return;
      _btree[value.value()].push_back(value.chunk_offset());
    });
  });
}

template <typename DataType>
Iterator BTreeIndex<DataType>::_lower_bound(const std::vector<AllTypeVariant>& values) const {
  return std::vector<ChunkOffset>().begin();
}

template <typename DataType>
Iterator BTreeIndex<DataType>::_upper_bound(const std::vector<AllTypeVariant>& values) const {
  return std::vector<ChunkOffset>().begin();
}

template <typename DataType>
Iterator BTreeIndex<DataType>::_cbegin() const {
  return std::vector<ChunkOffset>().begin();
}

template <typename DataType>
Iterator BTreeIndex<DataType>::_cend() const {
  return std::vector<ChunkOffset>().begin();
}

template <typename DataType>
std::vector<std::shared_ptr<const BaseColumn>> BTreeIndex<DataType>::_get_index_columns() const {
  return {_index_column};
}

} // namespace opossum