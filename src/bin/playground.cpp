#include <iostream>
#include <chrono>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdlib>

#include "types.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/import_binary.hpp"
#include "operators/export_binary.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include <json.hpp>

using namespace opossum;

void print_table_layout(std::string table_name) {
  auto table = StorageManager::get().get_table(table_name);
  std::cout << "table: " << table_name << std::endl;
  std::cout << "rows: " << table->row_count() << std::endl;
  std::cout << "chunks: " << table->chunk_count() << std::endl;
  std::cout << "columns: " << table->column_count() << std::endl;
  for (auto column_id = ColumnID{0}; column_id < table->column_count(); column_id++) {
    auto column_name = table->column_name(column_id);
    auto column_type = table->column_type(column_id);
    std::cout << "(" << column_type << ") " << column_name << ", ";
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
    std::cout << " > Loading table " << table_name << " from disk" << std::endl;
    auto import = std::make_shared<ImportBinary>(table_name, table_name);
    import->execute();
    return true;
  } else {
    return false;
  }
}

void save_table(std::shared_ptr<const Table> table, std::string file_name) {
  std::cout << " > Saving table " << file_name << " to disk" << std::endl;
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  auto export_operator = std::make_shared<ExportBinary>(table_wrapper, file_name);
  export_operator->execute();
}

std::string tpcc_load_or_generate(int warehouse_size, int chunk_size, std::string tpcc_table_name) {
  auto table_name = tpcc_table_name + "_" + std::to_string(warehouse_size) + "_" + std::to_string(chunk_size);
  auto loaded = load_table(table_name);
  if (!loaded) {
    std::cout << " > Generating table " << table_name << std::endl;
    auto table = tpcc::TpccTableGenerator::generate_tpcc_table(tpcc_table_name, chunk_size, warehouse_size);
    StorageManager::get().add_table(table_name, table);
    save_table(table, table_name);
  }

  return table_name;
}

int random_int(int min, int max, int except) {
  DebugAssert(min <= max, "min value must be <= max value");
  DebugAssert(!(min == max && max == except), "value range empty");

  int value;
  do {
    value = std::rand() % (max - min) + min;
  } while (value == except);

  return value;
}

std::vector<AllTypeVariant> generate_row(int scan_column, int scan_column_value, int column_count) {
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

  std::cout << " > Generating table " << table_name << std::endl;

  // Requirements:
  // - 10 columns minimum,
  // - Every chunk is compressed

  // Best case: Dictionary can prune nothing, filter can prune everything
  // -> Scan value in between min/max
  // -> No chunk contains the actual scan value
  const auto column_count = 10;
  const auto scan_column = 0;
  const auto scan_value = 3000;
  const auto min_value = 0;
  const auto max_value = 200000;

  // Generate table header
  auto table = std::make_shared<Table>(chunk_size);
  for (int i = 0; i < column_count; i++) {
    table->add_column_definition("column" + i, "int", false);
  }

  // Generate table data
  for (int row_number = 0; row_number < row_count; row_number++) {
    if (row_number % chunk_size == 0) {
      table->append(generate_row(scan_column, min_value, column_count));
    } else if (row_number % chunk_size == 1) {
      table->append(generate_row(scan_column, max_value, column_count));
    } else {
      table->append(generate_row(scan_column, random_int(min_value, max_value, scan_value), column_count));
    }
  }

  StorageManager::get().add_table(table_name, table);
  save_table(table, table_name);

  return table_name;
}

void worst_case_load_or_generate(int rows, int chunk_size) {
}

std::shared_ptr<AbstractOperator> generate_benchmark(std::string tpcc_table_name, std::string column_name,
                                                     uint8_t quotient_size, uint8_t remainder_size, int warehouse_size,
                                                     int chunk_size) {
  auto table_name = tpcc_load_or_generate(warehouse_size, chunk_size, tpcc_table_name);
  auto table = StorageManager::get().get_table(table_name);
  auto column_id = table->column_id_by_name(column_name);
  if (remainder_size > 0 && quotient_size > 0) {
    //std::cout << " > Generating Filters" << std::endl;
    auto filter_insert_jobs = table->populate_quotient_filters(column_id, quotient_size, remainder_size);
    for (auto job : filter_insert_jobs) {
      job->schedule();
    }
    CurrentScheduler::wait_for_tasks(filter_insert_jobs);
    //std::cout << " > Done" << std::endl;
  }

  auto get_table = std::make_shared<GetTable>(table_name);
  auto table_scan = std::make_shared<TableScan>(get_table, column_id, ScanType::OpEquals, 3000);
  get_table->execute();
  return table_scan;
}

void serialize_results(nlohmann::json results) {
  std::ofstream outfile;
  outfile.open ("/home/osboxes/dev/jupyter/benchmark_results.json");
  //outfile.open ("results.json");
  if (outfile.is_open()) {
    outfile << results;
    outfile.close();
    std::cout << " > result written" << std::endl;
  } else {
    std::cout << " > could not write results to file" << std::endl;
  }
}

int main() {
  auto tpcc_table_name = std::string("ORDER");
  auto column_name = std::string("NO_O_ID");
  auto warehouse_size = 100;
  auto chunk_size = 100000;
  auto quotient_size = 14;
  //auto remainder_size = 8;
  auto remainder_sizes = {0, 8, 16, 32};
  //auto quotient_sizes = {0, 10, 16};
  nlohmann::json results;
  results["results"] = nlohmann::json::array();
  results["table_name"] = tpcc_table_name;
  results["column_name"] = column_name;
  results["warehouse_size"] = warehouse_size;
  results["chunk_size"] = chunk_size;

  std::cout << "------------------------" << std::endl;
  std::cout << "Benchmark configuration: " << std::endl;
  std::cout << "table_name:     " << tpcc_table_name << std::endl;
  std::cout << "column_name:    " << column_name << std::endl;
  std::cout << "warehouse_size: " << warehouse_size << std::endl;
  std::cout << "chunk_size:     " << chunk_size << std::endl;
  std::cout << "quotient_size:  " << quotient_size << std::endl;
  std::cout << "------------------------" << std::endl;

  // analyze_value_interval<int>(table_name, column_name);
  for (auto remainder_size : remainder_sizes) {
    auto benchmark = generate_benchmark(tpcc_table_name, column_name, quotient_size, remainder_size,
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

  return 0;
}
