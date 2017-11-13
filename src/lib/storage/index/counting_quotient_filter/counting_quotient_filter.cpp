#include "counting_quotient_filter.hpp"
#include "utils/xxhash.hpp"
#include "resolve_type.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/iterables/create_iterable_from_column.hpp"

#include <cmath>
#include <iostream>
#include <string>

namespace opossum {

template <typename ElementType>
CountingQuotientFilter<ElementType>::CountingQuotientFilter(uint8_t quotient_bits, uint8_t remainder_bits) {
  DebugAssert(remainder_bits == 8 || remainder_bits == 16 || remainder_bits == 32,
              "Only the remainder sizes 8, 16, and 32 are supported");
  DebugAssert(quotient_bits == 8 || quotient_bits == 16 || quotient_bits == 32,
              "Quotient size can only be 8, 16 or 32 bits");
  DebugAssert(quotient_bits + remainder_bits <= 64, "The hash length can not exceed 64 bits.");

  _quotient_bits = quotient_bits;
  _remainder_bits = remainder_bits;
  _number_of_slots = std::pow(2, _quotient_bits);
  _hash_bits = _quotient_bits + _remainder_bits;

  if (remainder_bits == 8) {
    _quotient_filter8 = gqf8::quotient_filter();
    gqf8::qf_init(&_quotient_filter8.value(), _number_of_slots, _hash_bits, 0);
  } else if (remainder_bits == 16) {
    _quotient_filter16 = gqf16::quotient_filter();
    gqf16::qf_init(&_quotient_filter16.value(), _number_of_slots, _hash_bits, 0);
  } else if (remainder_bits == 32) {
    _quotient_filter32 = gqf32::quotient_filter();
    gqf32::qf_init(&_quotient_filter32.value(), _number_of_slots, _hash_bits, 0);
  }
}

template <typename ElementType>
void CountingQuotientFilter<ElementType>::insert(ElementType element, uint64_t count) {
  uint64_t bitmask = static_cast<uint64_t>(std::pow(2, _hash_bits)) - 1;
  uint64_t hash = bitmask & _hash(element);
  if (_remainder_bits == 8) {
    gqf8::qf_insert(&_quotient_filter8.value(), hash, 0, count);
  } else if (_remainder_bits == 16) {
    gqf16::qf_insert(&_quotient_filter16.value(), hash, 0, count);
  } else if (_remainder_bits == 32) {
    gqf32::qf_insert(&_quotient_filter32.value(), hash, 0, count);
  }
}

template <typename ElementType>
void CountingQuotientFilter<ElementType>::insert(ElementType element) {
  insert(element, 1);
}

template <typename ElementType>
uint64_t CountingQuotientFilter<ElementType>::count(ElementType element) {
  uint64_t bitmask = static_cast<uint64_t>(std::pow(2, _hash_bits)) - 1;
  uint64_t hash = bitmask & _hash(element);
  if (_remainder_bits == 8) {
    return gqf8::qf_count_key_value(&_quotient_filter8.value(), hash, 0);
  } else if (_remainder_bits == 16) {
    return gqf16::qf_count_key_value(&_quotient_filter16.value(), hash, 0);
  } else {
    return gqf32::qf_count_key_value(&_quotient_filter32.value(), hash, 0);
  }
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

/**
* Computes the hash for a string.
**/
template <>
uint64_t CountingQuotientFilter<std::string>::_hash(std::string value) {
  uint32_t seed = 384812094;
  auto hash = xxh::xxhash<64, char>(value.data(), value.length(), seed);
  return static_cast<uint64_t>(hash);
}

template <typename ElementType>
uint64_t CountingQuotientFilter<ElementType>::memory_consumption() {
  // TODO
  uint64_t consumption = sizeof(CountingQuotientFilter<ElementType>);
  return consumption;
}

template <typename ElementType>
void CountingQuotientFilter<ElementType>::populate(std::shared_ptr<const BaseColumn> column) {
  resolve_column_type<ElementType>(*column, [&](const auto& typed_column) {
    auto iterable_left = create_iterable_from_column<ElementType>(typed_column);
    iterable_left.for_each([&](const auto& value) {
      if (value.is_null()) return;
      insert(value.value());
    });
  });
}

EXPLICITLY_INSTANTIATE_COLUMN_TYPES(CountingQuotientFilter);

} // namespace opossum
