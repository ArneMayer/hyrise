#pragma once

#include "../utils.hpp"

#include "storage/table.hpp"

#include <list>
#include <iostream>

class TableScanBenchmarkSeries {
public:
  CustomBenchmarkSeries() {
    _results_table = std::make_shared<Table>();
    results_table->add_column("table_name", DataType::String, false);
    results_table->add_column("column_name", DataType::String, false);
    results_table->add_column("data_type", DataType::String, false);
    results_table->add_column("row_count", DataType::Int, false);
    results_table->add_column("chunk_size", DataType::Int, false);
    results_table->add_column("pruning_rate", DataType::Double, false);
    results_table->add_column("selectivity", DataType::Double, false);
    results_table->add_column("quotient_size", DataType::Int, false);
    results_table->add_column("remainder_size", DataType::Int, false);
    results_table->add_column("dictionary", DataType::Int, false);
    results_table->add_column("btree", DataType::Int, false);
    results_table->add_column("art", DataType::Int, false);
    results_table->add_column("size", DataType::Int, false);
    results_table->add_column("run_time", DataType::Int, false);
  }

  void print_info() {
    std::cout << "--------------------------------------------------" << std::endl;
    std::cout << "Benchmark configuration: " << std::endl;
    std::cout << "benchmark:       " << benchmark_name() << std::endl;
    std::cout << "table_name:      " << _table_name << std::endl;
    std::cout << "column_name:     " << _column_name << std::endl;
    std::cout << "data_types       " << _data_types << std::endl;
    std::cout << "sample_size:     " << _sample_size << std::endl;
    std::cout << "row_count:       " << _row_count << std::endl;
    std::cout << "chunk_size:      " << _chunk_size << std::endl;
    std::cout << "selectivity:     " << _selectivity << std::endl;
    std::cout << "quotient_size:   " << _quotient_size << std::endl;
    std::cout << "remainder_sizes: " << _remainder_sizes << std::endl;
    std::cout << "----------------------------------------------------" << std::endl;
  }

  virtual std::string benchmark_name() = 0;

  std::vector<TableScanConfiguration> get_configurations() {
    std::vector<TableScanConfiguration> configurations;

    for (auto scan_type : scan_types) {
      for (auto row_count : row_counts) {
        for (auto chunk_size : chunk_sizes) {
          for (auto pruning_rate : pruning_rates) {
            for (auto remainder_size : remainder_sizes) {
              TableScanConfiguration config;
              // Value Column Scan
              config.quotient_size = _quotient_size;
              config.remainder_size = remainder_size;
              config.push_back(configuration);

              // Dictionary Column Scan
              config.dictionary = true;
              config.push_back(configuration);
            }

            TableScanConfiguration btree_config;
            btree_config.btree = true;
            btree_config.row_count = row_count;
            btree_config.chunk_size = chunk_size;
            btree_config.selectivity = _selectivity;
            btree_config.pruning_rate = _pruning_rate;
            btree_config.push_back(btree_config);

            TableScanConfiguration art_config;
            art_config.art = true;
            art_config.dictionary = true;
            art_config.row_count = row_count;
            art_config.chunk_size = chunk_size;
            art_config.selectivity = _selectivity;
            art_config.pruning_rate = _pruning_rate;
            configurations.push_back(art_config);
          }
        }
      }
    }
  }

  void base_run() {
    for (auto scan_config : get_configurations()) {
      // create and execute
    }
  }

  virtual void run() {
    print_info();

    auto start = std::chrono::steady_clock::now();
    base_run();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now()-start);
    std::cout << " > Benchmark ran " << duration.count() << " seconds" << std::endl;

    serialize_results_csv(benchmark_name(), _results_table);
    StorageManager::get().reset();
  }

private:
  int _sample_size;
  std::shared_ptr<Table> _results_table;

  int _row_count;
  int _chunk_size;
  int _quotient_size;
  double _selectivity;
  double _pruning_rates;
  std::list<int> _remainder_sizes;
  std::list<std::string> _column_names;

}

void custom_benchmark_series() {
  auto sample_size = 1;
  auto row_counts = {1'000'000};
  auto remainder_sizes = {0, 2, 4, 8};
  auto quotient_size = 17;
  auto chunk_sizes = {100'000};
  auto pruning_rates = {1.0, 0.5};
  auto selectivity = 1.0 / 3000.0;
  auto scan_types = {std::string("int"), std::string("string")};

  // analyze_value_interval<int>(table_name, column_name);

}
