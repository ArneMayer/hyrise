#pragma once

#include "../utils.hpp"
#include "table_scan_configuration.hpp"

#include "storage/table.hpp"

#include <list>
#include <iostream>

using namespace opossum;

template <typename T>
std::ostream & operator<<(std::ostream& stream, std::list<T>& list) {
  stream << "{";
  auto it = list.begin();
  while (it != list.end()) {
    stream << *it;
    ++it;
    if (it != list.end()) {
      stream << ", ";
    }
  }
  stream << "}";

  return stream;
}

template <typename Benchmark>
class TableScanBenchmarkSeries {
public:
  static const int AUTO_QUOTIENT_SIZE = -1234;
  std::string benchmark_name = "unspecified";
  int sample_size = 10;
  std::string table_name = "unspecified";
  std::list<std::string> column_names = {"unspecified"};
  std::list<int> row_counts = {};
  std::list<int> chunk_sizes = {};
  bool auto_quotient_size = false;
  int quotient_size = -1;
  std::list<int> remainder_sizes = {};
  std::list<double> selectivities = {-1.0};
  std::list<double> pruning_rates = {-1.0};

  TableScanBenchmarkSeries() {
    _results_table = std::make_shared<Table>();
    _results_table->add_column("table_name", DataType::String, false);
    _results_table->add_column("column_name", DataType::String, false);
    _results_table->add_column("data_type", DataType::String, false);
    _results_table->add_column("row_count", DataType::Int, false);
    _results_table->add_column("chunk_size", DataType::Int, false);
    _results_table->add_column("pruning_rate", DataType::Double, false);
    _results_table->add_column("selectivity", DataType::Double, false);
    _results_table->add_column("quotient_size", DataType::Int, false);
    _results_table->add_column("remainder_size", DataType::Int, false);
    _results_table->add_column("dictionary", DataType::Int, false);
    _results_table->add_column("btree", DataType::Int, false);
    _results_table->add_column("art", DataType::Int, false);
    _results_table->add_column("size", DataType::Int, false);
    _results_table->add_column("actual_pruning_rate", DataType::Double, false);
    _results_table->add_column("run_time", DataType::Int, false);
  }

  void print_info() {
    std::cout << "--------------------------------------------------" << std::endl;
    std::cout << "Benchmark configuration: " << std::endl;
    std::cout << "benchmark:       " << benchmark_name << std::endl;
    std::cout << "table_name:      " << table_name << std::endl;
    std::cout << "column_names:    " << column_names << std::endl;
    std::cout << "sample_size:     " << sample_size << std::endl;
    std::cout << "row_counts:      " << row_counts << std::endl;
    std::cout << "chunk_sizes:     " << chunk_sizes << std::endl;
    std::cout << "selectivities:   " << selectivities << std::endl;
    std::cout << "pruning_rates:   " << pruning_rates << std::endl;
    std::cout << "quotient_size:   " << quotient_size << std::endl;
    std::cout << "remainder_sizes: " << remainder_sizes << std::endl;
    std::cout << "----------------------------------------------------" << std::endl;
  }

  std::vector<TableScanConfiguration> get_configurations() {
    std::vector<TableScanConfiguration> configurations;
    for (auto column_name : column_names) {
      for (auto row_count : row_counts) {
        for (auto chunk_size : chunk_sizes) {
          for (auto pruning_rate : pruning_rates) {
            for (auto selectivity : selectivities) {
              auto base_config = TableScanConfiguration();
              base_config.table_name = table_name;
              base_config.column_name = column_name;
              base_config.row_count = row_count;
              base_config.chunk_size = chunk_size;
              base_config.pruning_rate = pruning_rate;
              base_config.selectivity = selectivity;
              if (auto_quotient_size) {
                base_config.quotient_size = static_cast<int>(std::ceil(std::log2(chunk_size)));
                std::cout << "chunk_size: " << chunk_size << ", quotient_size: " << base_config.quotient_size << std::endl;
              } else {
                base_config.quotient_size = quotient_size;
              }
              base_config.remainder_size = 0;
              base_config.use_art = false;
              base_config.use_dictionary = false;
              base_config.use_btree = false;
              for (auto remainder_size : remainder_sizes) {
                TableScanConfiguration value_config = base_config;
                value_config.remainder_size = remainder_size;
                configurations.push_back(value_config);

                TableScanConfiguration dict_config = base_config;
                dict_config.remainder_size = remainder_size;
                dict_config.use_dictionary = true;
                configurations.push_back(dict_config);
              }

              TableScanConfiguration btree_config = base_config;
              btree_config.use_btree = true;
              configurations.push_back(btree_config);

              TableScanConfiguration art_config = base_config;
              art_config.use_art = true;
              art_config.use_dictionary = true;
              configurations.push_back(art_config);
            }
          }
        }
      }
    }

    return configurations;
  }

  void base_run() {
    for (auto config : get_configurations()) {
      auto benchmark = Benchmark(config);
      int size = 0;
      int sum_time = 0;
      int row_count = 0;
      for (int i = 0; i < sample_size; i++) {
        benchmark.prepare();
        auto time = benchmark.execute();
        sum_time += time;
        if (i == 0) {
          size = benchmark.memory_consumption_kB();
        }
        if (i == 0) {
          row_count = benchmark.row_count();
        }
        auto actual_pruning_rate = benchmark.actual_pruning_rate();
        auto data_type = benchmark.data_type();
        _results_table->append({config.table_name, config.column_name, data_type, row_count, config.chunk_size,
          config.pruning_rate, config.selectivity, config.quotient_size, config.remainder_size,
          static_cast<int>(config.use_dictionary), static_cast<int>(config.use_btree), static_cast<int>(config.use_art),
          size, actual_pruning_rate, time});
      }

      auto avg_time = sum_time / sample_size;
      std::cout << "row_count: " << config.row_count
                << ", chunk_size: " << config.chunk_size
                << ", remainder_size: " << config.remainder_size
                << ", dictionary: " << config.use_dictionary
                << ", btree: " << config.use_btree
                << ", art: " << config.use_art
                << ", pruning_rate: " << config.pruning_rate
                << ", selectivity: " << config.selectivity
                << ", size: " << size
                << ", avg_time: " << avg_time
                << std::endl;
    }
  }

  virtual void run() {
    print_info();

    auto start = std::chrono::steady_clock::now();
    base_run();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start);
    std::cout << " > Benchmark ran " << duration.count() << " seconds" << std::endl;

    serialize_results_csv(benchmark_name, _results_table);
    StorageManager::get().reset();
  }

private:
  std::shared_ptr<Table> _results_table;

};
