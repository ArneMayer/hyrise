#pragma once

#include "types.hpp"
#include "storage/base_column.hpp"


namespace opossum {

class BaseFilter : private Noncopyable {
 public:
  BaseFilter() = default;
  virtual ~BaseFilter() = default;
  BaseFilter(BaseFilter&&) = default;
  BaseFilter& operator=(BaseFilter&&) = default;

  virtual uint64_t count_all_type(AllTypeVariant value) const = 0;
  virtual void populate(std::shared_ptr<const BaseColumn> column) = 0;
  virtual uint64_t memory_consumption() const = 0;
  virtual double load_factor() const = 0;
  virtual bool is_full() const = 0;
};

} // namespace opossum
