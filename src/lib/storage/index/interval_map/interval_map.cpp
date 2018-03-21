#include "interval_map.hpp"

#include <boost/icl/interval.hpp>
#include <boost/icl/interval_map.hpp>

#include "storage/create_iterable_from_column.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
void IntervalMap<T>::add_column_chunk(ChunkID chunk_id, std::shared_ptr<const BaseColumn> column) {
  T min;
  T max;
  bool initialized = false;
  resolve_column_type<T>(*column, [&](const auto& typed_column) {
    auto iterable_left = create_iterable_from_column<T>(typed_column);
    iterable_left.for_each([&](const auto& value) {
      if (value.is_null()) return;
      if (!initialized) {
        min = value.value();
        max = value.value();
        initialized = true;
      }
      if (value.value() > max) {
        max = value.value();
      }
      if (value.value() < min) {
        min = value.value();
      }
    });
  });
  _interval_map += make_pair(boost::icl::interval<T>::closed(min, max), std::set<ChunkID>{chunk_id});
}

template <typename T>
std::set<ChunkID> IntervalMap<T>::point_query_all_type(AllTypeVariant value) const {
  return point_query(type_cast<T>(value));
}

template <typename T>
std::set<ChunkID> IntervalMap<T>::point_query(const T & value) const {
  return _interval_map.find(value)->second;
}

template <typename T>
uint64_t IntervalMap<T>::memory_consumption() const {
  auto memory_estimation = 0;

  auto begin_it = _interval_map.begin();
  for (auto it = begin_it; it != _interval_map.end(); it++) {
    memory_estimation += 8; // Interval Bounds
    memory_estimation += it->second.size() * 4; // Chunk Ids
  }

  return memory_estimation;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(IntervalMap);

} // namespace opossum
