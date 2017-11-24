#include <iostream>
#include <chrono>
#include <fstream>
#include <sstream>

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

void tpcc_load_or_generate(int warehouse_size, int chunk_size, std::string table_name) {
  auto sstream = std::stringstream();
  sstream << table_name << warehouse_size << "_" << chunk_size;
  auto file_name = sstream.str();
  bool file_exists = std::ifstream(file_name).good();
  if (file_exists) {
    std::cout << " > Loading from disk" << std::endl;
    auto import = std::make_shared<ImportBinary>(file_name, table_name);
    import->execute();
  } else {
    std::cout << " > Generating from scratch" << std::endl;
    auto table = tpcc::TpccTableGenerator::generate_tpcc_table(table_name, chunk_size, warehouse_size);
    StorageManager::get().add_table(table_name, table);
    auto table_wrapper = std::make_shared<TableWrapper>(table);
    table_wrapper->execute();
    auto export_operator = std::make_shared<ExportBinary>(table_wrapper, file_name);
    export_operator->execute();
  }
}

void generate_data(int warehouse_size, int chunk_size, std::string benchmark_table_name) {
  std::cout << "TPCC" << std::endl;
  std::cout << " > Generating tables" << std::endl;
  tpcc_load_or_generate(warehouse_size, chunk_size, benchmark_table_name);

  // Print table information
  auto table_names = StorageManager::get().table_names();
  for (auto table_name : table_names) {
    auto table = StorageManager::get().get_table(table_name);
    //print_chunk_sizes(table);
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
  std::cout << " > Done" << std::endl;
}

std::shared_ptr<AbstractOperator> generate_benchmark(std::string table_name, std::string column_name,
                                                     uint8_t quotient_size, uint8_t remainder_size) {
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
  auto table_name = "ORDER";
  auto column_name = "NO_O_ID";
  auto warehouse_size = 100;
  auto chunk_size = 100000;
  auto quotient_size = 14;
  //auto remainder_size = 8;
  auto remainder_sizes = {0, 8, 16, 32};
  //auto quotient_sizes = {0, 10, 16};
  nlohmann::json results;
  results["results"] = nlohmann::json::array();
  results["table_name"] = table_name;
  results["column_name"] = column_name;
  results["warehouse_size"] = warehouse_size;
  results["chunk_size"] = chunk_size;

  std::cout << "------------------------" << std::endl;
  std::cout << "Benchmark configuration: " << std::endl;
  std::cout << "table_name:     " << table_name << std::endl;
  std::cout << "column_name:    " << column_name << std::endl;
  std::cout << "warehouse_size: " << warehouse_size << std::endl;
  std::cout << "chunk_size:     " << chunk_size << std::endl;
  std::cout << "quotient_size:  " << quotient_size << std::endl;
  std::cout << "------------------------" << std::endl;

  generate_data(warehouse_size, chunk_size, table_name);
  analyze_value_interval<int>(table_name, column_name);
  for (auto remainder_size : remainder_sizes) {
    auto benchmark = generate_benchmark(table_name, column_name, quotient_size, remainder_size);
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

  //std::cout << results << std::endl;
  serialize_results(results);

  return 0;
}
