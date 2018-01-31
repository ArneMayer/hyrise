#pragma once

#include "utils.hpp"
#include "data_generation.hpp"

using namespace opossum;

int analyze_skippable_chunks_filter(std::string table_name, std::string column_name, AllTypeVariant scan_value) {
  auto table = StorageManager::get().get_table(table_name);
  auto column_id = table->column_id_by_name(column_name);

  auto skippable_count = 0;
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); chunk_id++) {
    auto chunk = table->get_chunk(chunk_id);
    auto filter = chunk->get_filter(column_id);
    if (filter != nullptr && filter->count_all_type(scan_value) == 0) {
      skippable_count++;
    }
  }

  return skippable_count;
}

template <typename T>
int analyze_skippable_chunks_actual(std::string table_name, std::string column_name, T scan_value) {
  auto table = StorageManager::get().get_table(table_name);
  auto column_id = table->column_id_by_name(column_name);

  auto skippable_count = 0;
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); chunk_id++) {
    auto chunk = table->get_chunk(chunk_id);
    auto column = chunk->get_column(column_id);
    bool skippable = true;
    resolve_column_type<T>(*column, [&](const auto& typed_column) {
      auto iterable_left = create_iterable_from_column<T>(typed_column);
      iterable_left.for_each([&](const auto& value) {
        if (!value.is_null() && value.value() == scan_value) {
          skippable = false;
        }
      });
    });
    if (skippable) {
      skippable_count++;
    }
  }
  return skippable_count;
}

template <typename T>
std::pair<T, T> analyze_value_interval(std::string table_name, ColumnID column_id, ChunkID chunk_id) {
  auto table = opossum::StorageManager::get().get_table(table_name);
  auto chunk = table->get_chunk(chunk_id);
  auto column = chunk->get_column(column_id);

  auto min_value = table->get_value<T>(column_id, 0);
  auto max_value = table->get_value<T>(column_id, 0);
  bool initialized = false;

  // Find min and max values
  resolve_column_type<T>(*column, [&](const auto& typed_column) {
    auto iterable_left = create_iterable_from_column<T>(typed_column);
    iterable_left.for_each([&](const auto& value) {
      if (value.is_null()) return;
      if (!initialized) {
        min_value = value.value();
        max_value = value.value();
        initialized = true;
      }
      if (value.value() < min_value) {
        min_value = value.value();
      }
      if (value.value() > max_value) {
        max_value = value.value();
      }
    });
  });

  return std::make_pair(min_value, max_value);
}

void analyze_value_interval(std::string table_name, ColumnID column_id) {
  //std::cout << " > Analyzing " << table_name << "::" << column_name << std::endl;
  auto table = opossum::StorageManager::get().get_table(table_name);
  auto column_type = table->column_type(column_id);

  for (auto chunk_id = opossum::ChunkID{0}; chunk_id < table->chunk_count(); chunk_id++) {
    if (column_type == DataType::Int) {
      auto interval = analyze_value_interval<int>(table_name, column_id, chunk_id);
      std::cout << "[" << interval.first << ", " << interval.second << "], ";
    } else if (column_type == DataType::Double) {
      auto interval = analyze_value_interval<double>(table_name, column_id, chunk_id);
      std::cout << "[" << interval.first << ", " << interval.second << "], ";
    } else if (column_type == DataType::String) {
      auto interval = analyze_value_interval<std::string>(table_name, column_id, chunk_id);
      std::cout << "[" << interval.first << ", " << interval.second << "], ";
    }
  }
  std::cout << std::endl;
}

void analyze_value_interval(std::string table_name, std::string column_name) {
  auto table = opossum::StorageManager::get().get_table(table_name);
  auto column_id = table->column_id_by_name(column_name);
  analyze_value_interval(table_name, column_id);
}

void print_table_layout(std::string table_name) {
  auto table = StorageManager::get().get_table(table_name);
  std::cout << "table: " << table_name << std::endl;
  std::cout << "rows: " << table->row_count() << std::endl;
  std::cout << "chunks: " << table->chunk_count() << std::endl;
  std::cout << "columns: " << table->column_count() << std::endl;
  for (auto column_id = ColumnID{0}; column_id < table->column_count(); column_id++) {
    auto column_name = table->column_name(column_id);
    auto column_type = table->column_type(column_id);
    std::cout << "(" << data_type_to_string(column_type) << ") " << column_name << ": ";
    analyze_value_interval(table_name, column_name);
  }
  std::cout << std::endl;
  std::cout << "------------------------" << std::endl;
}

void analyze_all_tpcc_tables() {
  auto tpcc_table_names = {std::string("ORDER"),
                           std::string("ITEM"),
                           std::string("WAREHOUSE"),
                           std::string("STOCK"),
                           std::string("DISTRICT"),
                           std::string("CUSTOMER"),
                           std::string("HISTORY"),
                           std::string("NEW-ORDER"),
                           std::string("ORDER-LINE")
                          };
  auto warehouse_size = 3;
  auto chunk_size = 100000;
  auto dictionary = false;

  for (auto tpcc_table_name : tpcc_table_names) {
    auto table_name = tpcc_load_or_generate(tpcc_table_name, warehouse_size, chunk_size, dictionary);
    auto table = StorageManager::get().get_table(table_name);
    print_table_layout(table_name);
  }
}

void analyze_jcch_lineitem() {
  auto tpch_table_name = std::string("LINEITEM");
  auto row_count = 6'000'000;
  auto chunk_size = 100'000;
  auto dictionary = false;

  auto table_name = jcch_load_or_generate(tpch_table_name, row_count, chunk_size, dictionary);
  auto table = StorageManager::get().get_table(table_name);
  print_table_layout(table_name);
}
