#pragma once

#include "types.hpp"
#include "storage/base_column.hpp"
#include <string>

namespace opossum {

class BaseFilter : private Noncopyable {
 public:
  void populate(std::shared_ptr<const BaseColumn> column);
};

} // namespace opossum
