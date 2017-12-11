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
#include "operators/import_binary.hpp"
#include "operators/export_binary.hpp"
#include "operators/export_csv.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "storage/dictionary_compression.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include <json.hpp>

using namespace opossum;

void clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
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

/**
* Checks whether the table exist in the storage manager or on disk
*/
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

int random_int(int min, int max, int except) {
  DebugAssert(min <= max, "min value must be <= max value");
  DebugAssert(!(min == max && max == except), "value range empty");

  int value;
  do {
    value = std::rand() % (max - min) + min;
  } while (value == except);

  return value;
}

char random_char() {
  const char lowest = 'A';
  const char highest = 'z';
  return lowest + std::rand() % (highest - lowest);
}

std::string random_string(int length, std::string except) {
  std::string result;
  do {
    auto sstream = std::stringstream();
    for (int i = 0; i < length; i++) {
      sstream << random_char();
    }
    result = sstream.str();
  } while (result == except);

  return result;
}

template <typename T>
std::vector<AllTypeVariant> generate_row(int scan_column, T scan_column_value, int column_count) {
  auto row = std::vector<AllTypeVariant>(column_count);
  for (int i = 0; i < column_count; i++) {
    row[i] = 0;
  }
  row[scan_column] = scan_column_value;

  return row;
}

std::string best_case_load_or_generate(int row_count, int chunk_size) {
  auto table_name = "best_case_" + std::to_string(row_count) + "_" + std::to_string(chunk_size);
  auto loaded = load_table(table_name);
  if (loaded) {
    return table_name;
  }

  std::cout << " > Generating table " << table_name << "...";

  // Requirements:
  // - 10 columns minimum,
  // - Every chunk is compressed#
  // - strings

  // Best case: Dictionary pruning takes long -> strings, filter can prune everything
  // -> Scan value in between min/max
  // -> No chunk contains the actual scan value
  const auto column_count = 1;
  const auto scan_column = 0;
  const auto string_size = 64;
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
    if (row_number % chunk_size == 0) {
      table->append(generate_row<std::string>(scan_column, min_value, column_count));
    } else if (row_number % chunk_size == 1) {
      table->append(generate_row<std::string>(scan_column, max_value, column_count));
    } else {
      table->append(generate_row<std::string>(scan_column, random_string(string_size, scan_value), column_count));
    }
  }

  DictionaryCompression::compress_table(*table);
  StorageManager::get().add_table(table_name, table);

  std::cout << "OK!" << std::endl;
  save_table(table, table_name);

  return table_name;
}

void worst_case_load_or_generate(int rows, int chunk_size) {
}

void create_quotient_filters(std::shared_ptr<Table> table, ColumnID column_id, uint8_t quotient_size,
    	                       uint8_t remainder_size) {
  if (remainder_size > 0 && quotient_size > 0) {
    //std::cout << " > Populating quotient filter (" << static_cast<int>(quotient_size) << ", "
    //                                               <<  static_cast<int>(remainder_size) << ")...";
    auto filter_insert_jobs = table->populate_quotient_filters(column_id, quotient_size, remainder_size);
    for (auto job : filter_insert_jobs) {
      job->schedule();
    }
    CurrentScheduler::wait_for_tasks(filter_insert_jobs);
    //std::cout << "OK!" << std::endl;
  }
}

std::shared_ptr<AbstractOperator> generate_benchmark_best_case(uint8_t remainder_size, int rows, int chunk_size) {
  auto quotient_size = static_cast<int>(std::ceil(std::log(chunk_size) / std::log(2)));
  auto table_name = best_case_load_or_generate(rows, chunk_size);
  //print_table_layout(table_name);
  //analyze_value_interval<int>(table_name, "column0");
  auto table = StorageManager::get().get_table(table_name);
  create_quotient_filters(table, ColumnID{0}, quotient_size, remainder_size);
  //std::cout << analyze_skippable_chunks(table_name, "column0", 3000) << " chunks skippable" << std::endl;
  auto get_table = std::make_shared<GetTable>(table_name);
  auto table_scan = std::make_shared<TableScan>(get_table, ColumnID{0}, ScanType::OpEquals, std::string("l_scan_value"));
  get_table->execute();
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

void serialize_results_json(nlohmann::json results) {
  std::ofstream outfile;
  //auto user_name = std::string("arne");
  auto user_name = std::string("osboxes");
  //auto user_name = std::string("Arne.Mayer");
  outfile.open("/home/" + user_name + "/dev/MasterarbeitJupyter/benchmark_results.json");
  //outfile.open ("results.json");
  if (outfile.is_open()) {
    outfile << results;
    outfile.close();
    std::cout << " > result written" << std::endl;
  } else {
    std::cout << " > could not write results to file" << std::endl;
  }
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

void best_case_benchmark_series() {
  auto sample_size = 100;
  auto row_counts = {100'000  };
  auto remainder_sizes = {0, 2, 4, 8, 16};
  auto chunk_sizes = {10'000, 100'000, 1'000'000};
  //auto row_counts = {100'000};
  //auto remainder_sizes = {4};
  //auto chunk_sizes = {10'000};

  std::cout << "------------------------" << std::endl;
  std::cout << "Benchmark configuration: " << std::endl;
  std::cout << "sample_size:    " << sample_size << std::endl;
  std::cout << "table_name:     " << "best_case" << std::endl;
  std::cout << "column_name:    " << "column0" << std::endl;
  std::cout << "------------------------" << std::endl;

  auto results_table = std::make_shared<Table>();
  results_table->add_column("row_count", DataType::Int, false);
  results_table->add_column("chunk_size", DataType::Int, false);
  //results_table->add_column("quotient_size", "int", false);
  results_table->add_column("remainder_size", DataType::Int, false);
  results_table->add_column("run_time", DataType::Int, false);

  // analyze_value_interval<int>(table_name, column_name);
  for (auto row_count : row_counts) {
    for (auto chunk_size : chunk_sizes) {
      for (auto remainder_size : remainder_sizes) {
        std::chrono::microseconds min_time;
        std::chrono::microseconds max_time;
        std::chrono::microseconds sum_time = std::chrono::milliseconds(0);

        for (int i = 0; i < sample_size; i++) {
          //std::cout << i << ",";
          auto benchmark = generate_benchmark_best_case(remainder_size, row_count, chunk_size);
          clear_cache();
          auto start = std::chrono::steady_clock::now();
          benchmark->execute();
          auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
          if (i == 0) {
            min_time = duration;
            max_time = duration;
          }
          if (duration < min_time) min_time = duration;
          if (duration > max_time) max_time = duration;
          sum_time += duration;
          results_table->append({row_count, chunk_size, remainder_size, duration.count()});
        }
        //std::cout << std::endl;

        auto avg_time = sum_time / sample_size;
        std::cout << "row_count: " << row_count
                  << ", chunk_size: " << chunk_size
                  << ", remainder_size: " << remainder_size
                  << ", min_time: " << min_time.count()
                  << ", max_time: " << max_time.count()
                  << ", avg_time: " << avg_time.count()
                  << std::endl;
      }
    }
  }

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
  best_case_benchmark_series();
  return 0;
}
