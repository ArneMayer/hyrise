#pragma once

#include "utils.hpp"

#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/load_table.hpp"
#include "all_type_variant.hpp"
#include "storage/index/counting_quotient_filter/counting_quotient_filter.hpp"
#include "import_export/csv_meta.hpp"
#include "storage/table.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/storage_manager.hpp"
#include "operators/import_binary.hpp"
#include "operators/export_binary.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/import_csv.hpp"
#include "tpcc/tpcc_table_generator.hpp"

#include <string>
#include <iostream>
#include <sstream>
#include <vector>
#include <limits>

using namespace opossum;

const double PI = 3.141592653589793238463;

int random_int(int min, int max) {
  DebugAssert(min <= max, "min value must be <= max value");

  return std::rand() % (max - min) + min;
}

int random_int(int min, int max, int except) {
  DebugAssert(!(min == max && max == except), "value range empty");

  int value;
  do {
    value = random_int(min, max);
  } while (value == except);

  return value;
}

char random_char() {
  const char lowest = 'A';
  const char highest = 'z';
  return lowest + std::rand() % (highest - lowest);
}

std::string random_string(int length) {
  auto sstream = std::stringstream();
  for (int i = 0; i < length; i++) {
    sstream << random_char();
  }
  return sstream.str();
}

std::string random_string(int length, std::string except) {
  std::string result;
  do {
    result = random_string(length);
  } while (result == except);
  return result;
}

std::vector<AllTypeVariant> generate_row(int scan_column, AllTypeVariant scan_column_value, int column_count) {
  auto row = std::vector<AllTypeVariant>(column_count);
  for (int i = 0; i < column_count; i++) {
    row[i] = 0;
  }
  row[scan_column] = scan_column_value;

  return row;
}

std::shared_ptr<Table> generate_table_string(int chunk_size, int row_count, int prunable_chunks, double selectivity, std::string scan_value) {
  const auto column_count = 5;
  const auto scan_column = 0;
  const auto string_size = 16;
  const auto min_value  = std::string("a_") + random_string(string_size - 2, "");
  const auto max_value = std::string("z_") + random_string(string_size - 2, "");;

  // Generate table header
  auto table = std::make_shared<Table>(chunk_size);
  table->add_column("column0", DataType::String, false);
  for (int i = 1; i < column_count; i++) {
    table->add_column("column" + std::to_string(i), DataType::Int, false);
  }

  // Generate table data
  for (int row_number = 0; row_number < row_count; row_number++) {
    auto chunk_offset = row_number % chunk_size;
    auto chunk_id = static_cast<int>(row_number / chunk_size);
    auto prunable = chunk_id < prunable_chunks;

    // Ensure minimum value
    if (chunk_offset == 0) {
      table->append(generate_row(scan_column, min_value, column_count));
    // Ensure maximum value
    } else if (chunk_offset == 1) {
      table->append(generate_row(scan_column, max_value, column_count));
    // Ensure non-prunability
    } else if (!prunable && chunk_offset < chunk_size * selectivity + 1) {
        table->append(generate_row(scan_column, scan_value, column_count));
    }
    else {
      table->append(generate_row(scan_column, random_string(string_size, scan_value), column_count));
    }
  }

  return table;
}

std::shared_ptr<Table> generate_table_int(int chunk_size, int row_count, int prunable_chunks, double selectivity, int scan_value) {
  const auto column_count = 10;
  const auto scan_column = 0;
  int min_value  = 0;
  int max_value;
  if (selectivity == 0.0) {
    max_value = std::numeric_limits<int>::max();
  } else {
    max_value = static_cast<int>(1.0 / selectivity);
  }

  // Generate table header
  auto table = std::make_shared<Table>(chunk_size);
  table->add_column("column0", DataType::Int, false);
  for (int i = 1; i < column_count; i++) {
    table->add_column("column" + std::to_string(i), DataType::Int, false);
  }

  // Generate table data
  for (int row_number = 0; row_number < row_count; row_number++) {
    auto chunk_offset = row_number % chunk_size;
    auto chunk_id = static_cast<int>(row_number / chunk_size);
    auto prunable = chunk_id < prunable_chunks;

    // Ensure minimum value
    if (chunk_offset == 0) {
      table->append(generate_row(scan_column, min_value, column_count));
    // Ensure maximum value
    } else if (chunk_offset == 1) {
      table->append(generate_row(scan_column, max_value, column_count));
    // Ensure non-prunability
  } else if (chunk_offset == 2 && !prunable) {
      table->append(generate_row(scan_column, scan_value, column_count));
    }
    else {
      // Ensure prunability
      if (prunable) {
        table->append(generate_row(scan_column, random_int(min_value, max_value, scan_value), column_count));
      } else {
        table->append(generate_row(scan_column, random_int(min_value, max_value), column_count));
      }
    }
  }

  return table;
}

std::shared_ptr<CountingQuotientFilter<int>> generate_filter_int(int size, int quotient_size, int remainder_size) {
  auto filter = std::make_shared<CountingQuotientFilter<int>>(quotient_size, remainder_size);
  for(int i = 0; i < size; i++) {
    filter->insert(random_int(0, 100000));
  }
  return filter;
}

std::shared_ptr<CountingQuotientFilter<std::string>> generate_filter_string(int size, int string_length, int quotient_size, int remainder_size) {
  auto filter = std::make_shared<CountingQuotientFilter<std::string>>(quotient_size, remainder_size);
  for(int i = 0; i < size; i++) {
    filter->insert(random_string(string_length));
  }
  return filter;
}

std::shared_ptr<std::vector<int>> generate_dictionary_int(int size) {
  auto v = std::make_shared<std::vector<int>>();
  for (int i = 0; i < size; i++) {
    v->push_back(random_int(0, 100000));
  }
  std::sort(v->begin(), v->end());
  return v;
}

std::shared_ptr<std::vector<std::string>> generate_dictionary_string(int size, int string_length) {
  auto v = std::make_shared<std::vector<std::string>>();
  for(int i = 0; i < size; i++) {
    v->push_back(random_string(string_length));
  }
  std::sort(v->begin(), v->end());
  return v;
}

bool import_table(std::string table_name) {
  if (StorageManager::get().has_table(table_name)) {
    return true;
  }
  bool file_exists = std::ifstream(table_name).good();
  if (file_exists) {
    std::cout << " > Loading table " << table_name << " from disk" << "...";
    auto import = std::make_shared<ImportBinary>(table_name, table_name);
    import->execute();
    std::cout << "OK!" << std::endl;
    return true;
  } else {
    return false;
  }
}

void save_table(std::shared_ptr<const Table> table, std::string file_name) {
  std::cout << " > Saving table " << file_name << " to disk" << "...";
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  auto export_operator = std::make_shared<ExportBinary>(table_wrapper, file_name);
  export_operator->execute();
  std::cout << "OK!" << std::endl;
}

std::string custom_load_or_generate(DataType type, int row_count, int chunk_size, int prunable_chunks,
                                    double selectivity, bool compressed, AllTypeVariant scan_value) {
  auto selectivity_label = static_cast<int>(selectivity * 10'000'000);
  auto table_name = "custom_" + data_type_to_string(type) + "_"
                              + std::to_string(row_count) + "_"
                              + std::to_string(chunk_size) + "_"
                              + std::to_string(prunable_chunks) + "_"
                              + std::to_string(selectivity_label) + "_";
  auto compressed_name = table_name + std::to_string(true);
  auto uncompressed_name = table_name + std::to_string(false);
  table_name = table_name + std::to_string(compressed);
  auto loaded = import_table(table_name);
  if (loaded) {
    return table_name;
  }

  std::cout << " > Generating table " << uncompressed_name << "...";

  std::shared_ptr<Table> table = nullptr;
  if (type == DataType::String) {
    table = generate_table_string(chunk_size, row_count, prunable_chunks, selectivity, type_cast<std::string>(scan_value));
  } else if (type == DataType::Int) {
    table = generate_table_int(chunk_size, row_count, prunable_chunks, selectivity, type_cast<int>(scan_value));
  } else {
    throw std::logic_error("No data generator for this data type");
  }

  // Save uncompressed
  std::cout << "OK!" << std::endl;
  save_table(table, uncompressed_name);

  // Save compressed
  std::cout << " > Generating table " << compressed_name << "...";
  DictionaryCompression::compress_table(*table);
  std::cout << "OK!" << std::endl;
  save_table(table, compressed_name);

  table = nullptr;

  import_table(table_name);

  return table_name;
}

std::string tpcc_load_or_generate(std::string tpcc_table_name, int warehouse_size, int chunk_size, bool compressed) {
  auto table_name = tpcc_table_name + "_"
                            + std::to_string(warehouse_size) + "_"
                            + std::to_string(chunk_size) + "_";
  auto compressed_name = table_name + std::to_string(true);
  auto uncompressed_name = table_name + std::to_string(false);
  table_name = table_name + std::to_string(compressed);

  auto loaded = import_table(table_name);
  if (loaded) {
    return table_name;
  }

  // Save uncompressed
  std::cout << " > Generating table " << uncompressed_name << "..." << std::flush;
  auto table = tpcc::TpccTableGenerator::generate_tpcc_table(tpcc_table_name, chunk_size, warehouse_size, false);
  std::cout << "OK!" << std::endl;
  save_table(table, uncompressed_name);

  // Save compressed
  std::cout << " > Generating table " << compressed_name << "..." << std::flush;
  DictionaryCompression::compress_table(*table);
  std::cout << "OK!" << std::endl;
  save_table(table, compressed_name);

  table = nullptr;
  import_table(table_name);
  return table_name;
}

std::string jcch_load_or_generate(std::string tpch_table_name, int row_count, int chunk_size, bool compressed) {
  Assert(tpch_table_name == "LINEITEM", "Only the LINEITEM table is supported right now.");
  Assert(row_count == 6'000'000, "Only a row count of 6'000'000 is supported right now.");

  auto table_name = tpch_table_name + "_"
                            + std::to_string(row_count) + "_"
                            + std::to_string(chunk_size) + "_";
  auto compressed_name = table_name + std::to_string(true);
  auto uncompressed_name = table_name + std::to_string(false);
  table_name = table_name + std::to_string(compressed);

  auto loaded = import_table(table_name);
  if (loaded) {
    return table_name;
  }

  // Save uncompressed
  std::cout << " > Generating table " << uncompressed_name << "..." << std::flush;
  auto file_name = "/home/" + getUserName() + "/data/jcch/lineitem.tbl";
  auto table = load_table(file_name, chunk_size);
  std::cout << "OK!" << std::endl;
  save_table(table, uncompressed_name);

  // Save compressed
  std::cout << " > Generating table " << compressed_name << "..." << std::flush;
  DictionaryCompression::compress_table(*table);
  std::cout << "OK!" << std::endl;
  save_table(table, compressed_name);

  table = nullptr;
  import_table(table_name);
  return table_name;
}


std::string acdoca_load_or_generate(int row_count, int chunk_size, bool compressed) {
  if (row_count != 1'000'000) {
    std::logic_error("row count has to be 1'000'000");
  }

  //auto table_name = "acdoca_" + std::to_string(row_count) + "_" + std::to_string(chunk_size) + "_";
  auto table_name = std::string("acdoca");
  auto compressed_name = table_name + std::to_string(true);
  auto uncompressed_name = table_name + std::to_string(false);
  table_name = table_name + std::to_string(compressed);

  auto loaded = import_table(table_name);
  if (loaded) {
    return table_name;
  }

  // Parse csv
  auto tmp_table_name = std::string("acdoca_tmp");
  std::cout << " > Generating table " << uncompressed_name << "..." << std::flush;
  auto file = "/mnt/data2/acdoca/acdoca1M.csv";
  auto meta_file = "home" + getUserName() + "/data/acdoca/acdoca.csv.json";
  auto csvMeta = process_csv_meta_file(meta_file);
  csvMeta.chunk_size = chunk_size;
  auto import = std::make_shared<ImportCsv>(file, tmp_table_name, csvMeta);
  import->execute();

  // Save uncompressed
  auto table = StorageManager::get().get_table(tmp_table_name);
  save_table(table, uncompressed_name);

  // Save compressed
  std::cout << " > Generating table " << compressed_name << "..." << std::flush;
  DictionaryCompression::compress_table(*table);
  std::cout << "OK!" << std::endl;
  save_table(table, compressed_name);

  StorageManager::get().drop_table(tmp_table_name);

  table.reset();
  import_table(table_name);

  return table_name;
}

double normal(double expectation, double variance, double x) {
  auto variance_sq = std::pow(variance, 2);
  return 1.0 / std::sqrt(2 * PI * variance_sq) * std::exp(-std::pow(x - expectation, 2) / (2 * variance_sq));
}

std::vector<uint> generate_zipfian_distribution(int value_count, int distinct_values) {
  int sum = 0;
  auto distribution = std::vector<uint>(distinct_values);
  for (int i = 0; i < distinct_values; i++) {
    distribution[i] = value_count / (static_cast<double>(i) + std::log(1.78 * value_count));
    sum += distribution[i];
  }

  for (int i = 0; i < distinct_values; i++) {
    distribution[i] = distribution[i] * value_count / sum;
  }

  std::cout << "sum: " << sum << std::endl;

  return distribution;
}

std::vector<uint> generate_normal_distribution(int value_cont, int distinct_values, double variance) {
  auto distribution = std::vector<uint>(distinct_values);
  double expectation = distinct_values / 2.0;
  for (int i = 0; i < distinct_values; i++) {
    auto value = normal(expectation, variance, static_cast<double>(i)) * value_cont;
    distribution[i] = static_cast<uint>(value);
  }

  return distribution;
}

std::vector<uint> generate_uniform_distribution(int value_cont, int distinct_values) {
  auto distribution = std::vector<uint>(distinct_values);
  for (int i = 0; i < distinct_values; i++) {
    distribution[i] = static_cast<uint>(value_cont / distinct_values);
  }

  return distribution;
}
