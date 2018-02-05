#pragma once

using namespace opossum;

class CustomBenchmarkGenerator : TableScanBenchmarkGenerator {

 public:
  CustomBenchmarkGenerator(DataType data_type, int row_count, int chunk_size, bool btree, bool art, bool dict, int quotient_size, int remainder_size) {
    _data_type = data_type;
    _row_count = row_count;
    _chunk_size = chunk_size;
    _use_btree = btree;
    _use_art = art;
    _use_dictionary = dict;
    _quotient_size = quotient_size;
    _remainder_size = remainder_size;
  }

  virtual std::shared_ptr<Table> get_table() override {
    return nullptr;
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
}
