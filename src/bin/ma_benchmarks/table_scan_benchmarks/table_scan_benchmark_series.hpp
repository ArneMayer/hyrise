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
  std::string benchmark_name = "unspecified";
  int sample_size = 10;
  std::string table_name = "unspecified";
  std::list<std::string> column_names = {"unspecified"};
  std::list<int> row_counts = {};
  std::list<int> chunk_sizes = {};
  bool auto_quotient_size = false;
  int quotient_size = -1;
  bool interval_map_run = true;
  bool dictionary_run = true;
  bool art_run = true;
  bool btree_run = true;
  std::list<int> remainder_sizes = {};
  std::list<double> selectivities = {-1.0};
  std::list<double> pruning_rates = {-1.0};

  TableScanBenchmarkSeries() {
    TableColumnDefinitions column_definitions = {
      TableColumnDefinition("table_name", DataType::String, false),
      TableColumnDefinition("column_name", DataType::String, false),
      TableColumnDefinition("data_type", DataType::String, false),
      TableColumnDefinition("row_count", DataType::Int, false),
      TableColumnDefinition("chunk_size", DataType::Int, false),
      TableColumnDefinition("pruning_rate", DataType::Double, false),
      TableColumnDefinition("selectivity", DataType::Double, false),
      TableColumnDefinition("quotient_size", DataType::Int, false),
      TableColumnDefinition("remainder_size", DataType::Int, false),
      TableColumnDefinition("interval_map", DataType::Int, false),
      TableColumnDefinition("dictionary", DataType::Int, false),
      TableColumnDefinition("btree", DataType::Int, false),
      TableColumnDefinition("art", DataType::Int, false),
      TableColumnDefinition("size", DataType::Int, false),
      TableColumnDefinition("actual_pruning_rate", DataType::Double, false),
      TableColumnDefinition("optimal_pruning_rate", DataType::Double, false),
      TableColumnDefinition("run_time", DataType::Int, false)
    };
    _results_table = std::make_shared<Table>(column_definitions, TableType::Data);
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

                // Filter only run
                TableScanConfiguration value_config = base_config;
                value_config.remainder_size = remainder_size;
                configurations.push_back(value_config);

                if (dictionary_run) {
                  TableScanConfiguration dict_config = base_config;
                  dict_config.remainder_size = remainder_size;
                  dict_config.use_dictionary = true;
                  configurations.push_back(dict_config);
                }

                if (interval_map_run) {
                  TableScanConfiguration int_map_config = base_config;
                  int_map_config.remainder_size = remainder_size;
                  int_map_config.use_interval_map = true;
                  configurations.push_back(int_map_config);
                }
              }

              if (btree_run) {
                TableScanConfiguration btree_config = base_config;
                btree_config.use_btree = true;
                configurations.push_back(btree_config);
              }

              if (art_run) {
                TableScanConfiguration art_config = base_config;
                art_config.use_art = true;
                art_config.use_dictionary = true;
                configurations.push_back(art_config);
              }
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
      double optimal_pruning_rate = 0;
      for (int i = 0; i < sample_size; i++) {
        benchmark.prepare();
        auto time = benchmark.execute();
        sum_time += time;
        if (i == 0) {
          size = benchmark.memory_consumption_kB();
          row_count = benchmark.row_count();
          optimal_pruning_rate = benchmark.optimal_pruning_rate();
        }
        auto actual_pruning_rate = benchmark.actual_pruning_rate();
        auto data_type = benchmark.data_type();
        _results_table->append({config.table_name, config.column_name, data_type, row_count, config.chunk_size,
          config.pruning_rate, config.selectivity, config.quotient_size, config.remainder_size, static_cast<int>(config.use_interval_map),
          static_cast<int>(config.use_dictionary), static_cast<int>(config.use_btree), static_cast<int>(config.use_art),
          size, actual_pruning_rate, optimal_pruning_rate, time});
      }

      auto avg_time = sum_time / sample_size;
      std::cout << "row_count: " << config.row_count
                << ", chunk_size: " << config.chunk_size
                << ", remainder_size: " << config.remainder_size
                << ", interval_map: " << config.use_interval_map
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
