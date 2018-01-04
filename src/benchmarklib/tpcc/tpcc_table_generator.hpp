#pragma once

#include <ctime>

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "benchmark_utilities/abstract_benchmark_table_generator.hpp"
#include "tpcc_random_generator.hpp"

namespace opossum {

class Table;

}  // namespace opossum

namespace tpcc {

using TpccTableGeneratorFunctions = std::unordered_map<std::string, std::function<std::shared_ptr<opossum::Table>()>>;

class TpccTableGenerator : public benchmark_utilities::AbstractBenchmarkTableGenerator {
  // following TPC-C v5.11.0
 public:
  explicit TpccTableGenerator(const opossum::ChunkOffset chunk_size = 1'000'000, const size_t warehouse_size = 1);

  virtual ~TpccTableGenerator() = default;

  std::shared_ptr<opossum::Table> generate_items_table(bool compress = true);

  std::shared_ptr<opossum::Table> generate_warehouse_table(bool compress = true);

  std::shared_ptr<opossum::Table> generate_stock_table(bool compress = true);

  std::shared_ptr<opossum::Table> generate_district_table(bool compress = true);

  std::shared_ptr<opossum::Table> generate_customer_table(bool compress = true);

  std::shared_ptr<opossum::Table> generate_history_table(bool compress = true);

  typedef std::vector<std::vector<std::vector<size_t>>> order_line_counts_type;

  order_line_counts_type generate_order_line_counts();

  std::shared_ptr<opossum::Table> generate_order_table(order_line_counts_type order_line_counts, bool compress  =true);

  std::shared_ptr<opossum::Table> generate_order_line_table(order_line_counts_type order_line_counts,
                                                            bool compress = true);

  std::shared_ptr<opossum::Table> generate_new_order_table(bool compress = true);

  std::map<std::string, std::shared_ptr<opossum::Table>> generate_all_tables(bool compress = true);

  std::map<std::string, std::shared_ptr<opossum::Table>> generate_all_tables() {
    return generate_all_tables(true);
  }

  static TpccTableGeneratorFunctions tpcc_table_generator_functions(bool compress = true);

  static std::shared_ptr<opossum::Table> generate_tpcc_table(const std::string& tablename, bool compress = true);

  static TpccTableGeneratorFunctions tpcc_table_generator_functions(const opossum::ChunkOffset chunk_size,
                                                                    const size_t warehouse_size,
                                                                    bool compress = true);

  static std::shared_ptr<opossum::Table> generate_tpcc_table(const std::string& tablename,
                                                             const opossum::ChunkOffset chunk_size,
                                                             const size_t warehouse_size,
                                                             bool compress = true);

  const size_t _warehouse_size;
  const time_t _current_date = std::time(0);

 protected:
  template <typename T>
  std::vector<T> generate_inner_order_line_column(std::vector<size_t> indices, order_line_counts_type order_line_counts,
                                                  const std::function<T(std::vector<size_t>)>& generator_function);

  template <typename T>
  void add_order_line_column(std::shared_ptr<opossum::Table> table, std::string name,
                             std::shared_ptr<std::vector<size_t>> cardinalities,
                             order_line_counts_type order_line_counts,
                             const std::function<T(std::vector<size_t>)>& generator_function);

  TpccRandomGenerator _random_gen;
};
}  // namespace tpcc
