#pragma once

#include "table_scan_benchmark.hpp"
#include "../data_generation.hpp"

#include "storage/storage_manager.hpp"

using namespace opossum;

class TpccBenchmark : public TableScanBenchmark {

 public:
  TpccBenchmark(TableScanConfiguration config) : TableScanBenchmark(config) {}

  virtual std::shared_ptr<Table> get_table() override {
    int warehouse_size = 0;
    if (_config.table_name == "ORDER") {
      warehouse_size = _config.row_count / 30'000;
    } else if (_config.table_name == "ITEM") {
      warehouse_size = 1;
    } else if (_config.table_name == "STOCK") {
      warehouse_size = _config.row_count / 100'000;
    } else if (_config.table_name == "DISTRICT") {
      warehouse_size = _config.row_count / 10;
    } else if (_config.table_name == "CUSTOMER") {
      warehouse_size = _config.row_count / 30'000;
    } else if (_config.table_name == "HISTORY") {
      warehouse_size = _config.row_count / 30'000;
    } else if (_config.table_name == "NEW_ORDER") {
      warehouse_size = _config.row_count / (30'000 * 0.3);
    } else if (_config.table_name == "ORDER_LINE") {
      warehouse_size = _config.row_count / 300'000;
    } else {
      throw std::logic_error("tpcc table not known: " + _config.table_name);
    }
    auto table_name = tpcc_load_or_generate(_config.table_name, warehouse_size, _config.chunk_size, _config.use_dictionary);
    return StorageManager::get().get_table(table_name);
  }

  virtual ColumnID get_column_id() override {
    return get_table()->column_id_by_name(_config.column_name);
  }

  virtual AllTypeVariant get_scan_value() override {
    return 3000;
  }
};
