#include "counting_quotient_filter.hpp"
#include "cqf.hpp"
#include "utils/xxhash.hpp"
#include "resolve_type.hpp"
#include "storage/iterables/create_iterable_from_column.hpp"

#include <cmath>
#include <iostream>

namespace opossum {

template <typename ElementType>
CountingQuotientFilter<ElementType>::CountingQuotientFilter() {
  _quotient_bits = 16;
  _remainder_bits = 8;
  _number_of_slots = std::pow(2, _quotient_bits);
  _hash_bits = _quotient_bits + _remainder_bits;

  gqf::qf_init(&_quotient_filter, _number_of_slots, _hash_bits, 0);
}

template <typename ElementType>
void CountingQuotientFilter<ElementType>::insert(ElementType element, uint64_t count) {
  uint64_t bitmask = static_cast<uint64_t>(std::pow(2, _hash_bits)) - 1;
  uint16_t hash = bitmask & _hash(element);
  gqf::qf_insert(&_quotient_filter, hash, 0, count);
}

template <typename ElementType>
void CountingQuotientFilter<ElementType>::insert(ElementType element) {
  insert(element, 1);
}

template <typename ElementType>
uint64_t CountingQuotientFilter<ElementType>::count(ElementType element) {
  uint16_t hash = static_cast<uint16_t>(_hash(element));
  return gqf::qf_count_key_value(&_quotient_filter, hash, 0);
}

/**
* Computes the hash for a value.
**/
template <typename ElementType>
uint64_t CountingQuotientFilter<ElementType>::_hash(ElementType value) {
  uint32_t seed = 384812094;
  auto hash = xxh::xxhash<64, ElementType>(&value, 1, seed);
  return static_cast<uint64_t>(hash);
}

template <typename ElementType>
uint64_t CountingQuotientFilter<ElementType>::memory_consumption() {
  // TODO
  uint64_t consumption = sizeof(CountingQuotientFilter<ElementType>);
  return consumption;
}

template <typename ElementType>
void CountingQuotientFilter<ElementType>::populate(const Table& table, ChunkID chunk_id, ColumnID column_id) {
  auto column = table->get_chunk(chunk_id).get_column(column_id);
  resolve_column_type<ElementType>(*column, [&](const auto& typed_column) {
    auto iterable_left = create_iterable_from_column<ElementType>(typed_column);
    iterable_left.for_each([&](const auto& left_value) {
      if (value.is_null()) return;
      insert(value.value());
    }
  }
}

} // namespace opossum
