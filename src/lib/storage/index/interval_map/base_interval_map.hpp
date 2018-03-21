#pragma once

#include <set>

#include "storage/base_column.hpp"
#include "types.hpp"

namespace opossum {

class BaseIntervalMap {
 public:
  BaseIntervalMap() = default;
  virtual ~BaseIntervalMap() = default;
  BaseIntervalMap(BaseIntervalMap&&) = default;
  BaseIntervalMap& operator=(BaseIntervalMap&&) = default;

  virtual void add_column_chunk(ChunkID chunk_id, std::shared_ptr<const BaseColumn> column) = 0;
  virtual std::set<ChunkID> point_query_all_type(AllTypeVariant value) const = 0;
  virtual uint64_t memory_consumption() const = 0;
};

} // namespace opossum
