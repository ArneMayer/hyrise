#pragma once

#include "base_b_tree_index.hpp"
#include "storage/index/base_index.hpp"
#include "types.hpp"

#include <btree_map.h>

namespace opossum {

/**
* Implementation: https://code.google.com/archive/p/cpp-btree/
* Note: does not support null values right now.
*/
template <typename DataType>
class BTreeIndex : public BaseBTreeIndex {
 public:
  BTreeIndex() = delete;
  explicit BTreeIndex(const Table& table, const ColumnID column_id);
  BTreeIndex(BTreeIndex&&) = default;
  BTreeIndex& operator=(BTreeIndex&&) = default;
  virtual ~BTreeIndex() = default;

  virtual const std::vector<RowID>& point_query_all_type(AllTypeVariant value) const;
  const std::vector<RowID>& point_query(DataType value) const;

 private:
  btree::btree_map<DataType, std::vector<RowID>> _btree;
};

} // namespace opossum
