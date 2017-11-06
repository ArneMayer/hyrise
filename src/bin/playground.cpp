#include <iostream>

#include "types.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"

int main() {
  std::cout << "TPCC" << std::endl;
  std::cout << " > Generating tables" << std::endl;
  opossum::ChunkOffset chunk_size = 10000;
  size_t warehouse_size = 2;
  auto tables = tpcc::TpccTableGenerator(chunk_size, warehouse_size).generate_all_tables();

  for (auto& pair : tables) {
    opossum::StorageManager::get().add_table(pair.first, pair.second);
    std::cout << "table: " << pair.first << std::endl;
    std::cout << "rows: " << pair.second->row_count() << std::endl;
    std::cout << "chunks: " << pair.second->chunk_count() << std::endl;
    std::cout << "columns: " << pair.second->column_count() << std::endl;
    for (auto column_id = opossum::ColumnID{0}; column_id < pair.second->column_count(); column_id++) {
      auto column_name = pair.second->column_name(column_id);
      auto column_type = pair.second->column_name(column_id);
      std::cout << "(" << column_type << ") " << column_name << ", ";
    }
    std::cout << std::endl;
    std::cout << "------------------------" << std::endl;
  }
  return 0;
}
