#include "data_generation.hpp"

#include <iostream>
#include <chrono>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <cmath>

#include "types.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/export_csv.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "resolve_type.hpp"
#include "storage/iterables/create_iterable_from_column.hpp"

using namespace opossum;

void clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
}

template <typename T>
std::pair<T, T> analyze_value_interval(std::string table_name, std::string column_name, ChunkID chunk_id) {
  auto table = opossum::StorageManager::get().get_table(table_name);
  auto column_id = table->column_id_by_name(column_name);
  auto& chunk = table->get_chunk(chunk_id);
  auto column = chunk.get_column(column_id);

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

void analyze_value_interval(std::string table_name, std::string column_name) {
  //std::cout << " > Analyzing " << table_name << "::" << column_name << std::endl;
  auto table = opossum::StorageManager::get().get_table(table_name);
  auto column_id = table->column_id_by_name(column_name);
  auto column_type = table->column_type(column_id);

  for (auto chunk_id = opossum::ChunkID{0}; chunk_id < table->chunk_count(); chunk_id++) {
    if (column_type == DataType::Int) {
      auto interval = analyze_value_interval<int>(table_name, column_name, chunk_id);
      std::cout << "[" << interval.first << ", " << interval.second << "], ";
    } else if (column_type == DataType::Double) {
      auto interval = analyze_value_interval<double>(table_name, column_name, chunk_id);
      std::cout << "[" << interval.first << ", " << interval.second << "], ";
    } else if (column_type == DataType::String) {
      auto interval = analyze_value_interval<std::string>(table_name, column_name, chunk_id);
      std::cout << "[" << interval.first << ", " << interval.second << "], ";
    }
  }
  std::cout << std::endl;
}

std::string data_type_to_string(DataType data_type) {
  if (data_type == DataType::Int) {
    return "int";
  } else if (data_type == DataType::Double) {
    return "double";
  } else if (data_type == DataType::String) {
    return "string";
  } else {
    return "unknown";
  }
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

int analyze_skippable_chunks_filter(std::string table_name, std::string column_name, AllTypeVariant scan_value) {
  auto table = StorageManager::get().get_table(table_name);
  auto column_id = table->column_id_by_name(column_name);

  auto skippable_count = 0;
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); chunk_id++) {
    auto& chunk = table->get_chunk(chunk_id);
    auto filter = chunk.get_filter(column_id);
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
    auto& chunk = table->get_chunk(chunk_id);
    auto column = chunk.get_column(column_id);
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

void print_chunk_sizes(std::shared_ptr<const Table> table) {
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); chunk_id++) {
    auto& chunk = table->get_chunk(chunk_id);
    std::cout << "Chunk " << chunk_id << " size: " << chunk.size() << std::endl;
  }
}

void create_quotient_filters(std::shared_ptr<Table> table, ColumnID column_id, uint8_t quotient_size,
    	                       uint8_t remainder_size) {
  if (remainder_size > 0 && quotient_size > 0) {
    auto filter_insert_jobs = table->populate_quotient_filters(column_id, quotient_size, remainder_size);
    for (auto job : filter_insert_jobs) {
      job->schedule();
    }
    CurrentScheduler::wait_for_tasks(filter_insert_jobs);
  } else {
    table->delete_quotient_filters(column_id);
  }
}

std::pair<std::shared_ptr<AbstractOperator>, std::shared_ptr<const Table>> generate_custom_benchmark(
              std::string type, uint8_t remainder_size, bool compressed, bool btree, bool art, int rows, int chunk_size,
                                                                         double pruning_rate, double selectivity) {
  auto chunk_count = static_cast<int>(std::ceil(rows / static_cast<double>(chunk_size)));
  auto prunable_chunks = static_cast<int>(chunk_count * pruning_rate);
  auto quotient_size = static_cast<int>(std::ceil(std::log(chunk_size) / std::log(2)));
  auto table_name = custom_load_or_generate(type, rows, chunk_size, prunable_chunks, selectivity, compressed);
  auto table = StorageManager::get().get_table(table_name);

  create_quotient_filters(table, ColumnID{0}, quotient_size, remainder_size);
  if (btree) {
    table->populate_btree_index(ColumnID{0});
  } else {
    table->delete_btree_index(ColumnID{0});
  }
  if (art) {
    table->populate_art_index(ColumnID{0});
  } else {
    table->delete_art_index(ColumnID{0});
  }

  AllTypeVariant scan_value;
  if (type == "string") {
    scan_value = std::string("l_scan_value");
  } else {
    scan_value = 1;
  }

  auto get_table = std::make_shared<GetTable>(table_name);
  auto table_scan = std::make_shared<TableScan>(get_table, ColumnID{0}, ScanType::OpEquals, scan_value);
  get_table->execute();

  //print_table_layout(table_name);
  //analyze_value_interval<int>(table_name, "column0");
  //std::cout << analyze_skippable_chunks(table_name, "column0", 3000) << " chunks skippable" << std::endl;
  return std::make_pair(table_scan, table);
}

std::pair<std::shared_ptr<AbstractOperator>, std::shared_ptr<const Table>> generate_tpcc_benchmark(
      std::string tpcc_table_name, std::string column_name, int warehouse_size, int chunk_size, uint8_t remainder_size,
                                                                                bool dictionary, bool btree, bool art) {
  auto table_name = tpcc_load_or_generate(tpcc_table_name, warehouse_size, chunk_size, dictionary);
  auto table = StorageManager::get().get_table(table_name);
  auto column_id = table->column_id_by_name(column_name);
  auto quotient_size = static_cast<int>(std::ceil(std::log(chunk_size) / std::log(2)));
  create_quotient_filters(table, column_id, quotient_size, remainder_size);
  if (btree) {
    table->populate_btree_index(column_id);
  } else {
    table->delete_btree_index(column_id);
  }
  if (art) {
    table->populate_art_index(column_id);
  } else {
    table->delete_art_index(column_id);
  }
  auto get_table = std::make_shared<GetTable>(table_name);
  auto table_scan = std::make_shared<TableScan>(get_table, column_id, ScanType::OpEquals, 3000);
  get_table->execute();
  return std::make_pair(table_scan, table);
}

void serialize_results_csv(std::string benchmark_name, std::shared_ptr<Table> table) {
  std::cout << "Writing results to csv...";
  //auto user_name = std::string("arne");
  //auto user_name = std::string("osboxes");
  auto user_name = std::string("Arne.Mayer");
  auto file_name = "/home/" + user_name + "/dev/MasterarbeitJupyter/" + benchmark_name + "_results.csv";
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  auto export_csv = std::make_shared<ExportCsv>(table_wrapper, file_name);
  export_csv->execute();
  std::cout << "OK!" << std::endl;
}

void run_tpcc_benchmark(std::string table_name, std::string column_name, int warehouse_size, int chunk_size,
                        int remainder_size, bool dictionary, bool btree, bool art, int sample_size,
                        std::shared_ptr<Table> results_table) {
  auto sum_time = std::chrono::microseconds(0);
  int size = -1;
  for (int i = 0; i < sample_size; i++) {
    auto benchmark = generate_tpcc_benchmark(table_name, column_name, warehouse_size, chunk_size, remainder_size,
                                             dictionary, btree, art);
    auto query = benchmark.first;
    auto table = benchmark.second;
    clear_cache();
    auto start = std::chrono::steady_clock::now();
    query->execute();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);
    sum_time += duration;
    if (size == -1) {
      size = table->ma_memory_consumption(table->column_id_by_name(column_name));
    }
    results_table->append({table_name, column_name, warehouse_size, chunk_size, remainder_size,
                      static_cast<int>(dictionary), static_cast<int>(btree), static_cast<int>(art),
                      static_cast<int>(size), duration.count()});
  }


  auto avg_time = sum_time / sample_size;
  std::cout << "table: " << table_name
            << ", column: " << column_name
            << ", warehouse_size: " << warehouse_size
            << ", chunk_size: " << chunk_size
            << ", remainder_size: " << remainder_size
            << ", dictionary: " << dictionary
            << ", btree: " << btree
            << ", art: " << art
            << ", size: " << size
            << ", avg_time: " << avg_time.count()
            << std::endl;
}

void run_custom_benchmark(std::string type, int remainder_size, bool dictionary, bool btree, bool art, int row_count,
                   int chunk_size, double pruning_rate, double selectivity, int sample_size,
                   std::shared_ptr<Table> results_table) {
  auto sum_time = std::chrono::microseconds(0);

  int size = -1;
  for (int i = 0; i < sample_size; i++) {
    auto benchmark = generate_custom_benchmark(type, remainder_size, dictionary, btree, art, row_count, chunk_size,
                                        pruning_rate, selectivity);
    auto query = benchmark.first;
    auto table = benchmark.second;
    clear_cache();
    auto start = std::chrono::steady_clock::now();
    query->execute();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
    sum_time += duration;
    if (size == -1) {
      size = table->ma_memory_consumption(ColumnID{0});
    }
    results_table->append({type, row_count, chunk_size, pruning_rate, selectivity, remainder_size,
                           static_cast<int>(dictionary), static_cast<int>(btree), static_cast<int>(art),
                           static_cast<int>(size), duration.count()});
  }

  auto avg_time = sum_time / sample_size;
  std::cout << "type: " << type
            << ", row_count: " << row_count
            << ", chunk_size: " << chunk_size
            << ", remainder_size: " << remainder_size
            << ", dictionary: " << dictionary
            << ", btree: " << btree
            << ", art: " << art
            << ", pruning_rate: " << pruning_rate
            << ", selectivity: " << selectivity
            << ", size: " << size
            << ", avg_time: " << avg_time.count()
            << std::endl;
}

void dict_query_benchmark_string(std::vector<std::string>& dictionary, int n, int string_length) {
  auto value = random_string(string_length);
  for (int i = 0; i < n; i++) {
    std::binary_search(dictionary.begin(), dictionary.end(), value);
  }
}

void filter_query_benchmark_string(std::shared_ptr<CountingQuotientFilter<std::string>> filter, int n, int string_length) {
  auto value = random_string(string_length);
  for (int i = 0; i < n; i++) {
    filter->count(value);
  }
}

void dict_query_benchmark_int(std::vector<int>& dictionary, int n) {
  auto value = random_int(0, 100000);
  for (int i = 0; i < n; i++) {
    std::binary_search(dictionary.begin(), dictionary.end(), value);
  }
}

void filter_query_benchmark_int(std::shared_ptr<CountingQuotientFilter<int>> filter, int n) {
  auto value = random_int(0, 100000);
  for (int i = 0; i < n; i++) {
    filter->count(value);
  }
}

void dict_vs_filter_series() {
  auto string_length = 64;
  auto size = 1000000;
  auto n = 100;
  auto quotient_size = 20;
  auto remainder_size = 2;

  auto dict_string = generate_dictionary_string(size, string_length);
  clear_cache();
  auto start = std::chrono::steady_clock::now();
  dict_query_benchmark_string(dict_string, n, string_length);
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
  std::cout << "string dictionary: " << duration.count() << std::endl;
  auto filter_string = generate_filter_string(size, string_length, quotient_size, remainder_size);
  clear_cache();
  start = std::chrono::steady_clock::now();
  filter_query_benchmark_string(filter_string, n, string_length);
  duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
  std::cout << "string filter: " << duration.count() << std::endl;

  auto dict_int = generate_dictionary_int(size);
  clear_cache();
  start = std::chrono::steady_clock::now();
  dict_query_benchmark_int(dict_int, n);
  duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
  std::cout << "int dictionary: " << duration.count() << std::endl;
  auto filter_int = generate_filter_int(size, quotient_size, remainder_size);
  clear_cache();
  start = std::chrono::steady_clock::now();
  filter_query_benchmark_int(filter_int, n);
  duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
  std::cout << "int filter: " << duration.count() << std::endl;
}

void tpcc_benchmark_series() {
  auto sample_size = 1;
  auto tpcc_table_name = std::string("ORDER-LINE");
  auto column_name = std::string("OL_I_ID");
  auto warehouse_size = 10;
  auto chunk_size = 100000;
  auto remainder_sizes = {0, 2, 4, 8};

  auto results_table = std::make_shared<Table>();
  results_table->add_column("table_name", DataType::String, false);
  results_table->add_column("column_name", DataType::String, false);
  results_table->add_column("warehouse_size", DataType::Int, false);
  results_table->add_column("chunk_size", DataType::Int, false);
  results_table->add_column("remainder_size", DataType::Int, false);
  results_table->add_column("dictionary", DataType::Int, false);
  results_table->add_column("btree", DataType::Int, false);
  results_table->add_column("art", DataType::Int, false);
  results_table->add_column("size", DataType::Int, false);
  results_table->add_column("run_time", DataType::Int, false);

  std::cout << "------------------------" << std::endl;
  std::cout << "Benchmark configuration: " << std::endl;
  std::cout << "sample_size:  " << sample_size << std::endl;
  std::cout << "data:           " << "tpcc" << std::endl;
  std::cout << "table_name:     " << tpcc_table_name << std::endl;
  std::cout << "column_name:    " << column_name << std::endl;
  std::cout << "warehouse_size: " << warehouse_size << std::endl;
  std::cout << "chunk_size:     " << chunk_size << std::endl;
  std::cout << "------------------------" << std::endl;

  auto start = std::chrono::steady_clock::now();

  // analyze_value_interval<int>(table_name, column_name);
  auto dictionary = false;
  auto art = false;
  auto btree = false;
  for (auto remainder_size : remainder_sizes) {
    run_tpcc_benchmark(tpcc_table_name, column_name, warehouse_size, chunk_size, remainder_size, dictionary=false,
                       btree=false, art=false, sample_size, results_table);
    run_tpcc_benchmark(tpcc_table_name, column_name, warehouse_size, chunk_size, remainder_size, dictionary=true,
                       btree=false, art=false, sample_size, results_table);
  }
  auto remainder_size = 0;
  run_tpcc_benchmark(tpcc_table_name, column_name, warehouse_size, chunk_size, remainder_size=0, dictionary=false,
                     btree=true, art=false, sample_size, results_table);
  run_tpcc_benchmark(tpcc_table_name, column_name, warehouse_size, chunk_size, remainder_size=0, dictionary=true,
                     btree=false, art=true, sample_size, results_table);
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now()-start);
  std::cout << "Benchmark ran " << duration.count() << " seconds" << std::endl;

  serialize_results_csv("tpcc", results_table);
  StorageManager::get().reset();
}

void custom_benchmark_series() {
  auto sample_size = 1;
  auto row_counts = {10'000'000};
  auto remainder_sizes = {0, 2, 4, 8};
  auto chunk_sizes = {1'000'000};
  auto pruning_rates = {1.0, 0.5};
  auto selectivity = 1.0 / 3000.0;
  auto scan_types = {std::string("int"), std::string("string")};

  std::cout << "------------------------" << std::endl;
  std::cout << "Benchmark configuration: " << std::endl;
  std::cout << "sample_size:  " << sample_size << std::endl;
  std::cout << "data:         " << "custom" << std::endl;
  std::cout << "selectivity:  " << selectivity << std::endl;
  std::cout << "------------------------" << std::endl;

  auto results_table = std::make_shared<Table>();
  results_table->add_column("data_type", DataType::String, false);
  results_table->add_column("row_count", DataType::Int, false);
  results_table->add_column("chunk_size", DataType::Int, false);
  results_table->add_column("pruning_rate", DataType::Double, false);
  results_table->add_column("selectivity", DataType::Double, false);
  results_table->add_column("remainder_size", DataType::Int, false);
  results_table->add_column("dictionary", DataType::Int, false);
  results_table->add_column("btree", DataType::Int, false);
  results_table->add_column("art", DataType::Int, false);
  results_table->add_column("size", DataType::Int, false);
  results_table->add_column("run_time", DataType::Int, false);

  // analyze_value_interval<int>(table_name, column_name);
  auto start = std::chrono::steady_clock::now();
  for (auto scan_type : scan_types) {
    for (auto row_count : row_counts) {
      for (auto chunk_size : chunk_sizes) {
        for (auto pruning_rate : pruning_rates) {
          auto dictionary = false;
          auto btree = false;
          auto art = false;
          for (auto remainder_size : remainder_sizes) {
            run_custom_benchmark(scan_type, remainder_size, dictionary=false, btree=false, art=false, row_count,
               chunk_size, pruning_rate, selectivity, sample_size, results_table);
            run_custom_benchmark(scan_type, remainder_size, dictionary=true, btree=false, art=false, row_count,
              chunk_size, pruning_rate, selectivity, sample_size, results_table);
          }
          auto remainder_size = 0;
          run_custom_benchmark(scan_type, remainder_size=0, dictionary=false, btree=true, art=false, row_count,
            chunk_size, pruning_rate, selectivity, sample_size, results_table);
          run_custom_benchmark(scan_type, remainder_size=0, dictionary=true, btree=false, art=true, row_count,
            chunk_size, pruning_rate, selectivity, sample_size, results_table);
        }
      }
    }
  }
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now()-start);
  std::cout << "Benchmark ran " << duration.count() << " seconds" << std::endl;

  serialize_results_csv("custom", results_table);
  StorageManager::get().reset();
}

/* NOTES
* Best Case Benchmark:
* - Performance improvement comes from not having to do a dictionary lookup
* - The improvement if the dictionary lookup would take long
*    - for bigger dictionaries
*    - for non-present values
* - For a noticable improvement we need:
*   - big dictionaries
*   - lots of chunks
*   - vergleich bei uncompressed besser
* - Current best case is not the best case for a compressed setting
*   - quotient filter can shine when there is no dictionary
*   - this is the case in an uncompressed setting
*
* Worst Case Benchmark:
* - when dictionary lookup takes no time at all
* - this is the case when the scan value is out of the min/max range
**/

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

struct mystruct {
  uint8_t array[100];
};

int main() {
  custom_benchmark_series();
  tpcc_benchmark_series();
  //dict_vs_filter_series();
  //analyze_all_tpcc_tables()

/*
  auto table_name = tpcc_load_or_generate("ORDER-LINE", 3, 100'000, false);
  auto table = StorageManager::get().get_table(table_name);
  create_quotient_filters(table, table->column_id_by_name("OL_I_ID"), 17, 4);
  std::cout << "Actually prunable: " << analyze_skippable_chunks_actual<int>(table_name, "OL_I_ID", 3000)
            << "/" << table->chunk_count() << std::endl;
  std::cout << "Filter prunable: " << analyze_skippable_chunks_filter(table_name, "OL_I_ID", 3000)
            << "/" << table->chunk_count() << std::endl;
  return 0;
  */
}
