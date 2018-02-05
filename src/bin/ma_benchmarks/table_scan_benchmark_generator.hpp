#pragma once

using namespace opossum;

class TableScanBenchmarkGenerator {
 public:
  virtual std::shared_ptr<Table> get_table() = 0;
  virtual ColumnID get_column_id() = 0;
  virtual AllTypeVariant get_scan_value() = 0;

  TableScanBenchmark generate() {
     auto table = get_table();
     auto column_id = get_column_id();
     auto scan_value = get_scan_value();
     return TableScanBenchmark(table, column_id, scan_value, _use_btree, _use_art, _quotient_size, _remainder_size);
   }

 private:
  bool _use_btree;
  bool _use_art;
  bool _use_dictionary;
  int _quotient_size;
  int _remainder_size;
}
