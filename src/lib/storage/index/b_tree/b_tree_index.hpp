#pragma once

#include "base_b_tree_index.hpp"
#include "storage/index/base_index.hpp"
#include "types.hpp"
#include "types.hpp"

#include <btree_map.h>

namespace opossum {

class BaseColumn;
class BaseDictionaryColumn;

using Iterator = std::vector<ChunkOffset>::const_iterator;

/**
* Implementation: https://code.google.com/archive/p/cpp-btree/
* Note: does not support null values right now.
*/
template <typename DataType>
class BTreeIndex : public BaseBTreeIndex {
 public:
  explicit BTreeIndex(const std::vector<std::shared_ptr<const BaseColumn>>& index_columns);
  BTreeIndex(BTreeIndex&&) = default;
  BTreeIndex& operator=(BTreeIndex&&) = default;
  virtual ~BTreeIndex() = default;

 private:
  Iterator _lower_bound(const std::vector<AllTypeVariant>& values) const final;
  Iterator _upper_bound(const std::vector<AllTypeVariant>& values) const final;
  Iterator _cbegin() const final;
  Iterator _cend() const final;
  std::vector<std::shared_ptr<const BaseColumn>> _get_index_columns() const;

  btree::btree_map<DataType, std::vector<ChunkOffset>> _btree;
};

} // namespace opossum