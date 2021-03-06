#pragma once

#include "base_interval_map.hpp"

#include <set>
#include <boost/icl/interval_map.hpp>

#include "types.hpp"
#include "storage/base_column.hpp"


namespace opossum {

template <typename T>
class IntervalMap : public BaseIntervalMap {
 public:
  virtual void add_column_chunk(ChunkID chunk_id, std::shared_ptr<const BaseColumn> column) override;
  virtual std::set<ChunkID> point_query_all_type(AllTypeVariant value) const override;
  std::set<ChunkID> point_query(const T & value) const;
  virtual uint64_t memory_consumption() const override;

 private:
   boost::icl::interval_map<T, std::set<ChunkID>> _interval_map;
};

} // namespace opossum
