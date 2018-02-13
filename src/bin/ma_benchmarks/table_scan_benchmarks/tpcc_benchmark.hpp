#pragma once

#include "table_scan_benchmark.hpp"
#include "../data_generation.hpp"

#include "storage/storage_manager.hpp"

using namespace opossum;

class TpccBenchmark : public TableScanBenchmark {

 public:
  TpccBenchmark(TableScanConfiguration config) : TableScanBenchmark(config) {
    if (_config.column_name == "columnInt") {
      _data_type = DataType::Int;
    } else if (_config.column_name == "columnString") {
      _data_type = DataType::String;
    } else {
      throw std::logic_error("unknown column name: " + _config.column_name);
    }
  }

  virtual std::shared_ptr<Table> get_table() override {
    auto chunk_count = static_cast<int>(std::ceil(_config.row_count / static_cast<double>(_config.chunk_size)));
    auto prunable_chunks = static_cast<int>(chunk_count * _config.pruning_rate);
    auto table_name = custom_load_or_generate(_data_type, _config.row_count, _config.chunk_size,
      prunable_chunks, _config.selectivity, _config.use_dictionary, get_scan_value());
    return StorageManager::get().get_table(table_name);
  }

  virtual ColumnID get_column_id() override {
    return ColumnID{0};
  }

  virtual AllTypeVariant get_scan_value() override {
    if (_data_type == DataType::Int) {
      return 1;
    } else if (_data_type == DataType::String) {
      return std::string("l_scan_value");
    } else {
      throw std::logic_error("custom scan type not implemented: " + data_type_to_string(_data_type));
    }
  }

 private:
  DataType _data_type;
};
