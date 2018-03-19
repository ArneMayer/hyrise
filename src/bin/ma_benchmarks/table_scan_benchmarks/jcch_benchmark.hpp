#pragma once

#include "table_scan_benchmark.hpp"
#include "../data_generation.hpp"

#include "storage/storage_manager.hpp"

using namespace opossum;

class JcchBenchmark : public TableScanBenchmark {

 public:
  JcchBenchmark(TableScanConfiguration config) : TableScanBenchmark(config) { }

  virtual std::shared_ptr<Table> get_table() override {
    auto table_name = jcch_load_or_generate(_config.table_name, _config.row_count, _config.chunk_size, _config.use_dictionary);
    return StorageManager::get().get_table(table_name);
  }

  virtual ColumnID get_column_id() override {
    auto table = get_table();
    return table->column_id_by_name(_config.column_name);
  }

  virtual AllTypeVariant get_scan_value() override {
    auto table = get_table();
    auto column_id = get_column_id();
    auto data_type = table->column_data_type(column_id);
    if (data_type == DataType::Int) {
      return 3000;
    } else if (data_type == DataType::String) {
      return std::string("1992-02-24");
    } else {
      throw std::logic_error("jcch scan type not implemented: " + data_type_to_string(data_type));
    }
  }

 private:
};
