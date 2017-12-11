#pragma once

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
*/
class BTreeIndex : public BaseIndex {
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

  btree::btree_map<AllTypeVariant, std::vector<ChunkOffset>> _btree;
  const std::shared_ptr<const BaseDictionaryColumn> _index_column;

};

} // namespace opossum