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
    return get_table()->get_value<std::string>(get_column_id(), 1000);
  }

 private:
};
