#pragma once

#include "../utils.hpp"

#include "storage/table.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/table_scan.hpp"

#include <chrono>

using namespace opossum;

class TableScanBenchmark {
 public:
  TableScanBenchmark(bool btree, bool art, int quotient_size, int remainder_size) {
    _use_btree = btree;
    _use_art = art;
    _quotient_size = quotient_size;
    _remainder_size = remainder_size;
  }

  int execute() {
    _table = get_table();
    _scan_column_id = get_column_id();
    _scan_value = get_scan_value();

    create_quotient_filters(_table, _scan_column_id, _quotient_size, _remainder_size);
    if (_use_btree) {
      _table->populate_btree_index(column_id);
    } else {
      _table->delete_btree_index(column_id);
    }
    if (_use_art) {
      _table->populate_art_index(column_id);
    } else {
      _table->delete_art_index(column_id);
    }

    auto table_wrapper = std::make_shared<TableWrapper>(_table);
    table_wrapper->execute();
    auto table_scan =
    std::make_shared<TableScan>(table_wrapper, _scan_column_id, PredicateCondition::Equals, _scan_value);

    clear_cache();

    auto start = std::chrono::steady_clock::now();
    query->execute();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);
    return duration.count();
  }

  int memory_consumption_kB() {
    return _table->ma_memory_consumption(_scan_column_id) / 1000;
  }

  virtual std::shared_ptr<Table> get_table() = 0;
  virtual ColumnID get_column_id() = 0;
  virtual AllTypeVariant get_scan_value() = 0;

 private:
  AllTypeVariant _scan_value;
  ColumnID _scan_column_id;
  std::shared_ptr<Table> _table;
  bool _use_btree;
  bool _use_art;
  int _quotient_size;
  int _remainder_size;
}
