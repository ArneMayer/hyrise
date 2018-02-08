#pragma once

using namespace opossum;

class CustomBenchmark : public TableScanBenchmark {

 public:
  CustomBenchmark(DataType data_type, int row_count, int chunk_size, bool btree, bool art, bool dict,
      int quotient_size, int remainder_size, double pruning_rate, double selectivity)
      : TableScanBenchmark(btree, art, quotient_size, remainder_size) {
    _data_type = data_type;
    _row_count = row_count;
    _chunk_size = chunk_size;
    _use_dictionary = dict;
    _pruning_rate = pruning_rate;
    _selectivity = selectivity;
  }

  virtual std::shared_ptr<Table> get_table() override {
    auto chunk_count = static_cast<int>(std::ceil(_row_count / static_cast<double>(_chunk_size)));
    auto prunable_chunks = static_cast<int>(chunk_count * pruning_rate);
    auto table_name = custom_load_or_generate(_data_type, _row_count, _chunk_size, prunable_chunks, _selectivity, _use_dictionary);
    return StorageManager::get().get_table(table_name);
  }

  virtual ColumnID get_column_id() override {
    return ColumnID{0};
  }

  virtual AllTypeVariant get_scan_value() {
    if (_data_type == DataType::Int) {
      return 1;
    } else if (_data_type == DataType::String) {
      return std::string("l_scan_value");
    }
  }

 private:
  DataType _data_type;
  int _row_count;
  int _chunk_size;
  int _use_dictionary;
  double _pruning_rate;
  double _selectivity;
}
