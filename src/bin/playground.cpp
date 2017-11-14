#include <iostream>
#include <chrono>

#include "types.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"

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

int main() {
  std::cout << "TPCC" << std::endl;
  std::cout << " > Generating tables" << std::endl;
  ChunkOffset chunk_size = 1000;
  size_t warehouse_size = 2;
  auto tables = tpcc::TpccTableGenerator(chunk_size, warehouse_size).generate_all_tables();

  // Add tables
  for (auto& pair : tables) {
    StorageManager::get().add_table(pair.first, pair.second);
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
    std::cout << "------------------------" << std::endl;
  }

  // Analyze value interval
  analyze_value_interval<int>("CUSTOMER", "C_ID");

  auto table = StorageManager::get().get_table("CUSTOMER");
  auto column_id = table->column_id_by_name("C_ID");
  table->populate_quotient_filters(column_id);

  auto get_table = std::make_shared<GetTable>("CUSTOMER");
  auto table_scan = std::make_shared<TableScan>(get_table, column_id, ScanType::OpEquals, 1000);

  auto start = std::chrono::steady_clock::now();

  get_table->execute();
  table_scan->execute();

  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);
  std::cout << "Time: " <<  duration.count() << std::endl;

  return 0;
}
