#include "b_tree_index.hpp"

#include "storage/base_attribute_vector.hpp"
#include "storage/base_column.hpp"
#include "storage/base_dictionary_column.hpp"
#include "storage/index/base_index.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

BTreeIndex::BTreeIndex(const std::vector<std::shared_ptr<const BaseColumn>>& index_columns)
    : BaseIndex{get_index_type_of<BTreeIndex>()},
      _index_column(std::dynamic_pointer_cast<const BaseDictionaryColumn>(index_columns.front())) {
  // TODO
}

Iterator BTreeIndex::_lower_bound(const std::vector<AllTypeVariant>& values) const {
  return std::vector<ChunkOffset>().begin();
}

Iterator BTreeIndex::_upper_bound(const std::vector<AllTypeVariant>& values) const {
  return std::vector<ChunkOffset>().begin();
}

Iterator BTreeIndex::_cbegin() const {
  return std::vector<ChunkOffset>().begin();
}

Iterator BTreeIndex::_cend() const {
  return std::vector<ChunkOffset>().begin();
}

std::vector<std::shared_ptr<const BaseColumn>> BTreeIndex::_get_index_columns() const {
  return {_index_column};
}

} // namespace opossum