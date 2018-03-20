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
  _interval_map += make_pair(boost::icl::interval<T>::closed(min, max), chunk_id);
}

template <typename T>
std::set<ChunkID> IntervalMap<T>::point_query_all_type(AllTypeVariant value) const {
  return point_query(type_cast<T>(value));
}

template <typename T>
std::set<ChunkID> IntervalMap<T>::point_query(const T & value) const {
  return {}; //_interval_map[value];
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(IntervalMap);

} // namespace opossum
