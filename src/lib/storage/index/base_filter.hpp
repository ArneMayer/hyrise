#pragma once

#include "types.hpp"
#include "storage/base_column.hpp"
#include <string>

namespace opossum {

class BaseFilter : private Noncopyable {
 public:
  BaseFilter() = default;
  virtual ~BaseFilter() = default;
  BaseFilter(BaseFilter&&) = default;
  BaseFilter& operator=(BaseFilter&&) = default;
  
  virtual void populate(std::shared_ptr<const BaseColumn> column) = 0;
};

} // namespace opossum
