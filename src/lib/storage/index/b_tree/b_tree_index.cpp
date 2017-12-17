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
BTreeIndex<DataType>::BTreeIndex(const Table& table, const ColumnID column_id) : BaseBTreeIndex{table, column_id} {
  _btree = std::make_shared<btree::btree_map<DataType, PosList>>();
  _bulk_insert(table, column_id);
}

template <typename DataType>
const PosList& BTreeIndex<DataType>::point_query_all_type(AllTypeVariant all_type_value) const {
  //DebugAssert(all_type_value.type() == typeid(DataType), "Value does not have the same type as the index elements");
  return point_query(type_cast<DataType>(all_type_value));
}

template <typename DataType>
const PosList& BTreeIndex<DataType>::point_query(DataType value) const {
  auto result = _btree->find(value);
  if (result != _btree->end()) {
    return result->second;
  } else {
    return _empty_list;
  }
}

template <typename DataType>
void BTreeIndex<DataType>::_bulk_insert(const Table& table, const ColumnID column_id) {
  for (auto chunk_id = ChunkID{0}; chunk_id < _table.chunk_count(); chunk_id++) {
    auto& chunk = _table.get_chunk(chunk_id);
    auto column = chunk.get_column(_column_id);
    resolve_column_type<DataType>(*column, [&](const auto& typed_column) {
      auto iterable_left = create_iterable_from_column<DataType>(typed_column);
      iterable_left.for_each([&](const auto& value) {
        if (value.is_null()) return;
        (*_btree)[value.value()].push_back(RowID{chunk_id , value.chunk_offset()});
      });
    });
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(BTreeIndex);

} // namespace opossum
