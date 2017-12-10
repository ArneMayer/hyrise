#pragma once

#include "storage/index/base_index.hpp"
#include "types.hpp"
#include <btree_map.h>

namespace opossum {

class BaseColumn;
class BaseDictionaryColumn;

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

  btree::btree_map _btree;

};

} // namespace opossum