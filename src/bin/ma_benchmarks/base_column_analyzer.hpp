#pragma once

#include "storage/create_iterable_from_column.hpp"
#include "types.hpp"
#include "storage/table.hpp"

using namespace opossum;

class BaseColumnAnalyzer {
 public:
  BaseColumnAnalyzer(std::shared_ptr<const Table> table, ColumnID column_id) {
    _table = table;
    _column_id = column_id;
  }

  virtual ~BaseColumnAnalyzer() = default;
  BaseColumnAnalyzer() = delete;
  BaseColumnAnalyzer(BaseColumnAnalyzer&&) = default;
  BaseColumnAnalyzer& operator=(BaseColumnAnalyzer&&) = default;

  virtual std::vector<uint> get_chunk_distribution(ChunkID chunk_id) = 0;
  virtual double get_pruning_rate(AllTypeVariant scan_value);

 protected:
  std::shared_ptr<const Table> _table;
  ColumnID _column_id;
};
