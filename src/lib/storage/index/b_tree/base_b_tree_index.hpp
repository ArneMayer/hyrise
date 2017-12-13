#pragma once

#include "storage/index/base_index.hpp"

namespace opossum {

class Table;

class BaseBTreeIndex : private Noncopyable {
 public:
  BaseBTreeIndex() = delete;
  explicit BaseBTreeIndex(std::shared_ptr<const Table> table, const ColumnID column_id);
  BaseBTreeIndex(BaseBTreeIndex&&) = default;
  BaseBTreeIndex& operator=(BaseBTreeIndex&&) = default;
  virtual ~BaseBTreeIndex() = default;

  virtual const std::vector<RowID>& point_query_all_type(AllTypeVariant value) const = 0;

 protected:
  const std::shared_ptr<const Table> _table;
  const ColumnID _column_id;
};

} // namespace opossum