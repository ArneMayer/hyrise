#pragma once

#include "storage/index/base_index.hpp"

namespace opossum {

class BaseBTreeIndex : public BaseIndex {
 public:
  explicit BaseBTreeIndex(const std::vector<std::shared_ptr<const BaseColumn>>& index_columns);
  BaseBTreeIndex(BaseBTreeIndex&&) = default;
  BaseBTreeIndex& operator=(BaseBTreeIndex&&) = default;
  virtual ~BaseBTreeIndex() = default;

 protected:
  const std::shared_ptr<const BaseColumn> _index_column;
};

} // namespace opossum