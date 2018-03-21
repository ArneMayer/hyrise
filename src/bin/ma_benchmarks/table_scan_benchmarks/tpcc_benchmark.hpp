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
    auto table = get_table();
    auto column_id = get_column_id();
    auto data_type = table->column_data_type(column_id);
    if (data_type == DataType::Int) {
      //return 3000;
      return table->get_value<int>(column_id, 1000);
    } else if (data_type == DataType::String) {
      //return std::string("1992-02-24");
      return table->get_value<std::string>(column_id, 1000);
    } else if (data_type == DataType::Double) {
      return table->get_value<double>(column_id, 1000);
    } else {
      throw std::logic_error("jcch scan type not implemented: " + data_type_to_string(data_type));
    }
  }
};
