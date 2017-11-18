#include <iostream>
#include <chrono>

#include "types.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"

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

void generate_data(int warehouse_size, int chunk_size, std::string table_name, std::string column_name) {
  std::cout << "TPCC" << std::endl;
  std::cout << " > Generating tables" << std::endl;
  auto tables = tpcc::TpccTableGenerator(chunk_size, warehouse_size).generate_all_tables();

  // Add tables
  for (auto& pair : tables) {
    StorageManager::get().add_table(pair.first, pair.second);
    /*
    std::cout << "table: " << pair.first << std::endl;
    std::cout << "rows: " << pair.second->row_count() << std::endl;
    std::cout << "chunks: " << pair.second->chunk_count() << std::endl;
    std::cout << "columns: " << pair.second->column_count() << std::endl;
    for (auto column_id = ColumnID{0}; column_id < pair.second->column_count(); column_id++) {
      auto column_name = pair.second->column_name(column_id);
      auto column_type = pair.second->column_type(column_id);
      std::cout << "(" << column_type << ") " << column_name << ", ";
    }
    std::cout << std::endl;
    //std::cout << "------------------------" << std::endl;
    */
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
  auto table_scan = std::make_shared<TableScan>(get_table, column_id, ScanType::OpEquals, 1000);
  get_table->execute();
  return table_scan;
}

int main() {
  auto table_name = "CUSTOMER";
  auto column_name = "C_ID";
  auto warehouse_size = 10;
  auto chunk_size = 1000;
  auto quotient_size = 10;
  //auto remainder_size = 8;

  generate_data(warehouse_size, chunk_size, table_name, column_name);
  auto remainder_sizes = {0, 8, 16, 32};
  //auto quotient_sizes = {0, 10, 16};
  for (auto remainder_size : remainder_sizes) {
    auto benchmark = generate_benchmark(table_name, column_name, quotient_size, remainder_size);
    auto start = std::chrono::steady_clock::now();
    benchmark->execute();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);
    std::cout << "quotient size: " << quotient_size << ", remainder size: " << remainder_size << ", time: " <<  duration.count() << std::endl;
  }

  return 0;
}
