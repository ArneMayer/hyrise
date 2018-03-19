#pragma once

#include "../utils.hpp"
#include "table_scan_configuration.hpp"

#include "storage/table.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/table_scan.hpp"

#include <chrono>

using namespace opossum;

class TableScanBenchmark {
 public:
  TableScanBenchmark(TableScanConfiguration config) {
    _config = config;
  }

  void prepare() {
    _table = get_table();
    _scan_column_id = get_column_id();
    _scan_value = get_scan_value();
  }

  int execute() {
    create_quotient_filters(_table, _scan_column_id, _config.quotient_size, _config.remainder_size);
    if (_config.use_btree) {
      _table->populate_btree_index(_scan_column_id);
    } else {
      _table->delete_btree_index(_scan_column_id);
    }
    if (_config.use_art) {
      _table->populate_art_index(_scan_column_id);
    } else {
      _table->delete_art_index(_scan_column_id);
    }

    auto table_wrapper = std::make_shared<TableWrapper>(_table);
    table_wrapper->execute();
    auto table_scan =
        std::make_shared<TableScan>(table_wrapper, _scan_column_id, PredicateCondition::Equals, _scan_value);

    clear_cache();

    auto start = std::chrono::steady_clock::now();
    table_scan->execute();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);
    return duration.count();
  }

  int memory_consumption_kB() {
    return _table->ma_memory_consumption(_scan_column_id) / 1000;
  }

  int row_count() {
    return _table->row_count();
  }

  std::string data_type() {
    return data_type_to_string(_table->column_data_type(_scan_column_id));
  }

  double actual_pruning_rate() {
    auto chunk_count = _table->chunk_count();
    auto prunable_chunks = 0;
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; chunk_id++) {
      auto filter = _table->get_chunk(chunk_id)->get_filter(_scan_column_id);
      if (filter != nullptr && filter->count_all_type(_scan_value) == 0) {
        prunable_chunks++;
      }
    }

    return prunable_chunks / static_cast<double>(chunk_count);
  }

  virtual std::shared_ptr<Table> get_table() = 0;
  virtual ColumnID get_column_id() = 0;
  virtual AllTypeVariant get_scan_value() = 0;

protected:
  TableScanConfiguration _config;

 private:
  AllTypeVariant _scan_value;
  ColumnID _scan_column_id;
  std::shared_ptr<Table> _table;
};
