#pragma once

#include "cqf.hpp"

#include <cstdint>
#include <vector>

namespace opossum {

/**
Following the idea and implementation of Pandey, Johnson and Patro:
Paper: A General-Purpose Counting Filter: Making Every Bit Count
Repository: https://github.com/splatlab/cqf
**/
template <typename ElementType>
class CountingQuotientFilter : BaseFilter
{
 public:
  CountingQuotientFilter();
  void insert(ElementType value, uint64_t count);
  void insert(ElementType value);
  void populate(const Table& table, ChunkID chunk_id, ColumnID column_id);
  uint64_t count(ElementType value);
  uint64_t memory_consumption();

 private:
  gqf::quotient_filter _quotient_filter;
  uint64_t _quotient_bits;
  uint64_t _remainder_bits;
  uint64_t _number_of_slots;
  uint64_t _hash_bits;
  uint64_t _hash(ElementType value);

};

} // namespace opossum
