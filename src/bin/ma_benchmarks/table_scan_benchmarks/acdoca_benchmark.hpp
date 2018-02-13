#pragma once

#include "table_scan_benchmark.hpp"
#include "../data_generation.hpp"

#include "storage/storage_manager.hpp"

using namespace opossum;

class CustomBenchmark : public TableScanBenchmark {

 public:
  CustomBenchmark(TableScanConfiguration config) : TableScanBenchmark(config) {}

  virtual std::shared_ptr<Table> get_table() override {
    auto table_name = acdoca_load_or_generate(row_count, chunk_size, dictionary);
    return StorageManager::get().get_table(table_name);
  }

  virtual ColumnID get_column_id() override {
    return ColumnID{0};
  }

  virtual AllTypeVariant get_scan_value() override {
    return 3000;
  }

 private:
};
