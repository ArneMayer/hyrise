#include <iostream>

#include "types.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"

template <typename T>
void analyze_value_interval(std::string table_name, std::string column_name) {
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
  opossum::ChunkOffset chunk_size = 1000;
  size_t warehouse_size = 1;
  auto tables = tpcc::TpccTableGenerator(chunk_size, warehouse_size).generate_all_tables();

  // Add tables
  for (auto& pair : tables) {
    opossum::StorageManager::get().add_table(pair.first, pair.second);
    std::cout << "table: " << pair.first << std::endl;
    std::cout << "rows: " << pair.second->row_count() << std::endl;
    std::cout << "chunks: " << pair.second->chunk_count() << std::endl;
    std::cout << "columns: " << pair.second->column_count() << std::endl;
    for (auto column_id = opossum::ColumnID{0}; column_id < pair.second->column_count(); column_id++) {
      auto column_name = pair.second->column_name(column_id);
      auto column_type = pair.second->column_type(column_id);
      std::cout << "(" << column_type << ") " << column_name << ", ";
    }
    std::cout << std::endl;
    std::cout << "------------------------" << std::endl;
  }

  // Analyze value interval
  analyze_value_interval<float>("ITEM", "I_PRICE");

  return 0;
}
