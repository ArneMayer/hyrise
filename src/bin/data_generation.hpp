#pragma once

#include "types.hpp"
#include "utils/assert.hpp"
#include "all_type_variant.hpp"
#include "storage/index/counting_quotient_filter/counting_quotient_filter.hpp"
#include "storage/table.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/storage_manager.hpp"
#include "operators/import_binary.hpp"
#include "operators/export_binary.hpp"
#include "operators/table_wrapper.hpp"

#include <string>
#include <iostream>
#include <sstream>
#include <vector>

using namespace opossum;

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

std::vector<int> generate_dictionary_int(int size) {
  std::vector<int> v;
  for (int i = 0; i < size; i++) {
    v.push_back(random_int(0, 100000));
  }
  std::sort (v.begin(), v.end());
  return v;
}

std::vector<std::string> generate_dictionary_string(int size, int string_length) {
  std::vector<std::string> v;
  for(int i = 0; i < size; i++) {
    v.push_back(random_string(string_length));
  }
  std::sort (v.begin(), v.end());
  return v;
}

bool load_table(std::string table_name) {
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

/*
std::string tpcc_load_or_generate(int warehouse_size, int chunk_size, std::string tpcc_table_name) {
  auto table_name = tpcc_table_name + "_" + std::to_string(warehouse_size) + "_" + std::to_string(chunk_size);
  auto loaded = load_table(table_name);
  if (!loaded) {
    std::cout << " > Generating table " << table_name << "...";
    auto table = tpcc::TpccTableGenerator::generate_tpcc_table(tpcc_table_name, chunk_size, warehouse_size);
    StorageManager::get().add_table(table_name, table);
    std::cout << "OK!" << std::endl;
    save_table(table, table_name);
  }

  return table_name;
}
*/

std::shared_ptr<Table> generate_table_string(int chunk_size, int row_count, int prunable_chunks) {
  const auto column_count = 1;
  const auto scan_column = 0;
  const auto string_size = 4;
  const auto scan_value = std::string("l_scan_value");
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
    // Ensure minimum value
    if (row_number % chunk_size == 0) {
      table->append(generate_row(scan_column, min_value, column_count));
    // Ensure maximum value
    } else if (row_number % chunk_size == 1) {
      table->append(generate_row(scan_column, max_value, column_count));
    // Ensure non-prunability
    } else if (row_number % chunk_size == 2) {
      if(static_cast<int>(row_number / chunk_size) >= prunable_chunks) {
        table->append(generate_row(scan_column, scan_value, column_count));
      }
    }
    else {
      table->append(generate_row(scan_column, random_string(string_size, scan_value), column_count));
    }
  }

  return table;
}

std::shared_ptr<Table> generate_table_int(int chunk_size, int row_count, int prunable_chunks) {
  const auto column_count = 1;
  const auto scan_column = 0;
  const auto scan_value = 3000;
  const auto min_value  = 0;
  const auto max_value = 5000;

  // Generate table header
  auto table = std::make_shared<Table>(chunk_size);
  table->add_column("column0", DataType::Int, false);
  for (int i = 1; i < column_count; i++) {
    table->add_column("column" + std::to_string(i), DataType::Int, false);
  }

  // Generate table data
  for (int row_number = 0; row_number < row_count; row_number++) {
    // Ensure minimum value
    if (row_number % chunk_size == 0) {
      table->append(generate_row(scan_column, min_value, column_count));
    // Ensure maximum value
    } else if (row_number % chunk_size == 1) {
      table->append(generate_row(scan_column, max_value, column_count));
    // Ensure non-prunability
    } else if (row_number % chunk_size == 2) {
      if(static_cast<int>(row_number / chunk_size) >= prunable_chunks) {
        table->append(generate_row(scan_column, scan_value, column_count));
      }
    }
    else {
      table->append(generate_row(scan_column, random_int(min_value, max_value, scan_value), column_count));
    }
  }

  return table;
}

std::string load_or_generate(std::string type, int row_count, int chunk_size, int prunable_chunks, bool compressed) {
  auto table_name = "bench_" + type + "_"
                             + std::to_string(row_count) + "_"
                             + std::to_string(chunk_size) + "_"
                             + std::to_string(prunable_chunks) + "_"
                             + std::to_string(compressed);
  auto loaded = load_table(table_name);
  if (loaded) {
    return table_name;
  }

  std::cout << " > Generating table " << table_name << "...";

  std::shared_ptr<Table> table = nullptr;
  if (type == "string") {
    table = generate_table_string(chunk_size, row_count, prunable_chunks);
  } else {
    table = generate_table_int(chunk_size, row_count, prunable_chunks);
  }

  if (compressed) {
    DictionaryCompression::compress_table(*table);
  }

  StorageManager::get().add_table(table_name, table);

  std::cout << "OK!" << std::endl;
  save_table(table, table_name);

  return table_name;
}