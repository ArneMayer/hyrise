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
#include "tpcc/tpcc_table_generator.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include <json.hpp>

using namespace opossum;

void clear_cache() {
  //std::cout << "clearing cache" << std::endl;
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
  //std::cout << "done clearing cache" << std::endl;
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
    std::cout << "(" << static_cast<int>(column_type) << ") " << column_name << ", ";
  }
  std::cout << std::endl;
  std::cout << "------------------------" << std::endl;
}

template <typename T>
void analyze_value_interval(std::string table_name, std::string column_name) {
  std::cout << " > Analyzing " << table_name << "::" << column_name << std::endl;
  auto table = opossum::StorageManager::get().get_table(table_name);
  auto column_id = table->column_id_by_name(column_name);

  size_t row_number_prefix = 0;
  for (auto chunk_id = opossum::ChunkID{0}; chunk_id < table->chunk_count(); chunk_id++) {
    uint32_t chunk_size = table->get_chunk(chunk_id).size();
    auto min_value = table->get_value<T>(column_id, 0);
    auto max_value = table->get_value<T>(column_id, 0);
    // Find the min and max values
    for (auto chunk_offset = opossum::ChunkOffset{0}; chunk_offset < chunk_size; chunk_offset++) {
      auto value = table->get_value<T>(column_id, row_number_prefix + chunk_offset);
      //std::cout << value << std::endl;
      if (value < min_value) {
        min_value = value;
      }
      if (value > max_value) {
        max_value = value;
      }
    }
    row_number_prefix += chunk_size;
    std::cout << "Chunk " << chunk_id << ": [" << min_value << ", " << max_value << "]" << std::endl;
  }
}

int analyze_skippable_chunks(std::string table_name, std::string column_name, AllTypeVariant scan_value) {
  auto table = StorageManager::get().get_table(table_name);
  auto column_id = table->column_id_by_name(column_name);

  auto skippable_count = 0;
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); chunk_id++) {
    auto& chunk = table->get_chunk(chunk_id);
    auto filter = chunk.get_filter(column_id);
    if (filter == nullptr) {
    } else if(filter->count_all_type(scan_value) == 0) {
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

std::shared_ptr<AbstractOperator> generate_benchmark(std::string type, uint8_t remainder_size, bool compressed,
                                                     bool btree, bool art, int rows, int chunk_size,
                                                     double pruning_rate, double selectivity) {
  auto chunk_count = static_cast<int>(std::ceil(rows / static_cast<double>(chunk_size)));
  auto prunable_chunks = static_cast<int>(chunk_count * pruning_rate);
  auto quotient_size = static_cast<int>(std::ceil(std::log(chunk_size) / std::log(2)));
  auto table_name = load_or_generate(type, rows, chunk_size, prunable_chunks, selectivity, compressed);
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
  return table_scan;
}

/*
std::shared_ptr<AbstractOperator> generate_benchmark_tpcc(std::string tpcc_table_name, std::string column_name,
                                                     uint8_t quotient_size, uint8_t remainder_size, int warehouse_size,
                                                     int chunk_size) {
  auto table_name = tpcc_load_or_generate(warehouse_size, chunk_size, tpcc_table_name);
  auto table = StorageManager::get().get_table(table_name);
  auto column_id = table->column_id_by_name(column_name);
  create_quotient_filters(table, column_id, quotient_size, remainder_size);
  auto get_table = std::make_shared<GetTable>(table_name);
  auto table_scan = std::make_shared<TableScan>(get_table, column_id, ScanType::OpEquals, 3000);
  get_table->execute();
  return table_scan;
}
*/

void serialize_results_csv(std::shared_ptr<Table> table) {
  std::cout << "Writing results to csv...";
  //auto user_name = std::string("arne");
  //auto user_name = std::string("osboxes");
  auto user_name = std::string("Arne.Mayer");
  auto file_name = "/home/" + user_name + "/dev/MasterarbeitJupyter/benchmark_results.csv";
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  auto export_csv = std::make_shared<ExportCsv>(table_wrapper, file_name);
  export_csv->execute();
  std::cout << "OK!" << std::endl;
}

/*
void tpcc_benchmark_series() {
  auto tpcc_table_name = std::string("ORDER");
  auto column_name = std::string("NO_O_ID");
  auto warehouse_size = 100;
  auto chunk_size = 100000;
  auto quotient_size = 14;
  //auto remainder_size = 8;
  auto remainder_sizes = {0, 8, 16, 32};
  //auto quotient_sizes = {0, 10, 16};

  auto results_table = std::make_shared<Table>();
  results_table->add_column("row_count", "int", false);
  results_table->add_column("chunk_size", "int", false);
  results_table->add_column("quotient_size", "int", false);
  results_table->add_column("remainder_size", "int", false);
  results_table->add_column("run_time", "int", false);

  std::cout << "------------------------" << std::endl;
  std::cout << "Benchmark configuration: " << std::endl;
  std::cout << "table_name:     " << tpcc_table_name << std::endl;
  std::cout << "column_name:    " << column_name << std::endl;
  std::cout << "row_count:      " << "?" << std::endl;
  std::cout << "chunk_size:     " << chunk_size << std::endl;
  std::cout << "quotient_size:  " << quotient_size << std::endl;
  std::cout << "------------------------" << std::endl;

  // analyze_value_interval<int>(table_name, column_name);
  for (auto remainder_size : remainder_sizes) {
    auto benchmark = generate_benchmark_tpcc(tpcc_table_name, column_name, quotient_size, remainder_size,
                                        warehouse_size, chunk_size);
    auto start = std::chrono::steady_clock::now();
    benchmark->execute();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);
    results["results"].push_back({
      {"quotient_size", quotient_size},
      {"remainder_size", remainder_size},
      {"runtime", duration.count()}
    });
    std::cout << "quotient_size: " << quotient_size
              << ", remainder_size: " << remainder_size
              << ", runtime: " << duration.count()
              << std::endl;
  }

  serialize_results(results);
  StorageManager::get().reset();
}
*/

void run_benchmark(std::string type, int remainder_size, bool dictionary, bool btree, bool art, int row_count,
                   int chunk_size, double pruning_rate, double selectivity, int sample_size,
                   std::shared_ptr<Table> results_table) {
  auto sum_time = std::chrono::microseconds(0);

  for (int i = 0; i < sample_size; i++) {
    auto benchmark = generate_benchmark(type, remainder_size, dictionary, btree, art, row_count, chunk_size,
                                        pruning_rate, selectivity);

    clear_cache();
    auto start = std::chrono::steady_clock::now();
    benchmark->execute();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
    sum_time += duration;
    results_table->append({type, row_count, chunk_size, pruning_rate, selectivity, remainder_size,
                      static_cast<int>(dictionary), static_cast<int>(btree), static_cast<int>(art), duration.count()});
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
            << ", avg_time: " << avg_time.count()
            << std::endl;
}

void dict_query_benchmark_string(std::vector<std::string>& dictionary, int n, int string_length) {
  auto value = random_string(string_length);
  for (int i = 0; i < n; i++) {
    //clear_cache();
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

void benchmark_series() {
  auto sample_size = 100;
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
            run_benchmark(scan_type, remainder_size, dictionary=false, btree=false, art=false, row_count, chunk_size,
                          pruning_rate, selectivity, sample_size, results_table);

            run_benchmark(scan_type, remainder_size, dictionary=true, btree=false, art=false, row_count, chunk_size,
                          pruning_rate, selectivity, sample_size, results_table);
          }

          auto remainder_size = 0;

          run_benchmark(scan_type, remainder_size=0, dictionary=true, btree=false, art=false, row_count, chunk_size, pruning_rate,
                        selectivity, sample_size, results_table);

          run_benchmark(scan_type, remainder_size=0, dictionary=false, btree=true, art=false, row_count, chunk_size, pruning_rate,
                        selectivity, sample_size, results_table);

          run_benchmark(scan_type, remainder_size=0, dictionary=true, btree=false, art=true, row_count, chunk_size,
                        pruning_rate, selectivity, sample_size, results_table);
        }
      }
    }
  }
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now()-start);
  std::cout << "Benchmark ran " << duration.count() << " seconds" << std::endl;

  serialize_results_csv(results_table);
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

int main() {
  benchmark_series();
  //dict_vs_filter_series();
  return 0;
}
