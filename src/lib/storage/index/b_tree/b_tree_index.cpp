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
BTreeIndex<DataType>::BTreeIndex(std::shared_ptr<const Table> table, const ColumnID column_id)
    : BaseBTreeIndex{table, column_id} {
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); chunk_id++) {
    auto& chunk = table->get_chunk(chunk_id);
    auto column = chunk.get_column(column_id);
    resolve_column_type<DataType>(*column, [&](const auto& typed_column) {
      auto iterable_left = create_iterable_from_column<DataType>(typed_column);
      iterable_left.for_each([&](const auto& value) {
        if (value.is_null()) return;
        _btree[value.value()].push_back(RowID{chunk_id , value.chunk_offset()});
      });
    });
  }
}

template <typename DataType>
const std::vector<RowID>& BTreeIndex<DataType>::point_query_all_type(AllTypeVariant all_type_value) const {
  DebugAssert(all_type_value.type() == typeid(DataType), "Value does not have the same type as the index elements");
  return point_query(type_cast<DataType>(all_type_value));
}

template <typename DataType>
const std::vector<RowID>& BTreeIndex<DataType>::point_query(DataType value) const {
  return _btree.find(value)->second;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(BTreeIndex);

} // namespace opossum