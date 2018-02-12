#pragma once

class TableScanConfiguration {
public:
  std::string table_name;
  std::string column_name;
  int remainder_size;
  int quotient_size;
  bool use_btree;
  bool use_art;
  bool use_dictionary;
  double selectivity;
  double pruning_rate;
  int row_count;
  int chunk_size;
};
