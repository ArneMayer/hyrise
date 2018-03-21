#pragma once

#include "table_scan_benchmark.hpp"
#include "../data_generation.hpp"

#include "storage/storage_manager.hpp"

using namespace opossum;

class AcdocaBenchmark : public TableScanBenchmark {

 public:
  AcdocaBenchmark(TableScanConfiguration config) : TableScanBenchmark(config) {}

  virtual std::shared_ptr<Table> get_table() override {
    auto table_name = acdoca_load_or_generate(_config.column_name, _config.row_count, _config.chunk_size, _config.use_dictionary);
    return StorageManager::get().get_table(table_name);
  }

  virtual ColumnID get_column_id() override {
    return ColumnID{0};
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

 private:
};
