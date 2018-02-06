#pragma once

#include "utils.hpp"
#include "analysis.hpp"

#include "types.hpp"
#include "storage/table.hpp"
#include "storage/index/counting_quotient_filter/counting_quotient_filter.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "operators/table_scan.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"

using namespace opossum;

void create_quotient_filters(std::shared_ptr<Table> table, ColumnID column_id, uint8_t quotient_size,
    	                       uint8_t remainder_size) {
  if (remainder_size > 0 && quotient_size > 0) {
    auto filter_insert_jobs = table->populate_quotient_filters(column_id, quotient_size, remainder_size);
    for (auto job : filter_insert_jobs) {
      job->schedule();
    }
    CurrentScheduler::wait_for_tasks(filter_insert_jobs);
  } else {
    table->delete_quotient_filters(column_id);
  }
}

std::pair<std::shared_ptr<AbstractOperator>, std::shared_ptr<const Table>> generate_custom_benchmark(
              std::string type, int quotient_size, int remainder_size, bool compressed, bool btree, bool art, int rows, int chunk_size,
                                                                         double pruning_rate, double selectivity) {
  auto chunk_count = static_cast<int>(std::ceil(rows / static_cast<double>(chunk_size)));
  auto prunable_chunks = static_cast<int>(chunk_count * pruning_rate);
  //auto quotient_size = static_cast<int>(std::ceil(std::log(chunk_size) / std::log(2)));
  auto table_name = custom_load_or_generate(type, rows, chunk_size, prunable_chunks, selectivity, compressed);
  auto table = StorageManager::get().get_table(table_name);

  create_quotient_filters(table, ColumnID{0}, quotient_size, remainder_size);
  if (btree) {
    table->populate_btree_index(ColumnID{0});
  } else {
    table->delete_btree_index(ColumnID{0});
  }
  if (art) {
    table->populate_art_index(ColumnID{0});
  } else {
    table->delete_art_index(ColumnID{0});
  }

  AllTypeVariant scan_value;
  if (type == "string") {
    scan_value = std::string("l_scan_value");
  } else {
    scan_value = 1;
  }

  /*
  int prunable_actual = 0;
  if (type == "string") {
    prunable_actual = analyze_skippable_chunks_actual<std::string>(table_name, "column0", std::string("l_scan_value"));
  } else {
    prunable_actual = analyze_skippable_chunks_actual<int>(table_name, "column0", 1);
  }


  auto prunable_filter = analyze_skippable_chunks_filter(table_name, "column0", scan_value);
  std::cout << "prunable by filter: " << prunable_filter << std::endl;
  std::cout << "prunable actual: " << prunable_actual << std::endl;
  */

  auto get_table = std::make_shared<GetTable>(table_name);
  auto table_scan = std::make_shared<TableScan>(get_table, ColumnID{0}, PredicateCondition::Equals, scan_value);
  get_table->execute();

  //print_table_layout(table_name);
  //analyze_value_interval<int>(table_name, "column0");
  //std::cout << analyze_skippable_chunks(table_name, "column0", 3000) << " chunks skippable" << std::endl;
  return std::make_pair(table_scan, table);
}

std::pair<std::shared_ptr<AbstractOperator>, std::shared_ptr<const Table>> generate_tpcc_benchmark(
      std::string tpcc_table_name, std::string column_name, int warehouse_size, int chunk_size,
      int quotient_size, int remainder_size, bool dictionary, bool btree, bool art) {
  auto table_name = tpcc_load_or_generate(tpcc_table_name, warehouse_size, chunk_size, dictionary);
  auto table = StorageManager::get().get_table(table_name);
  auto column_id = table->column_id_by_name(column_name);
  //auto quotient_size = static_cast<int>(std::ceil(std::log(chunk_size) / std::log(2)));
  create_quotient_filters(table, column_id, quotient_size, remainder_size);
  if (btree) {
    table->populate_btree_index(column_id);
  } else {
    table->delete_btree_index(column_id);
  }
  if (art) {
    table->populate_art_index(column_id);
  } else {
    table->delete_art_index(column_id);
  }
  auto get_table = std::make_shared<GetTable>(table_name);
  auto table_scan = std::make_shared<TableScan>(get_table, column_id, PredicateCondition::Equals, 3000);
  get_table->execute();
  return std::make_pair(table_scan, table);
}

std::pair<std::shared_ptr<AbstractOperator>, std::shared_ptr<const Table>> generate_jcch_benchmark(
      std::string tpch_table_name, std::string column_name, int row_count, int chunk_size, int quotient_size,
                                                          int remainder_size, bool dictionary, bool btree, bool art) {
  auto table_name = jcch_load_or_generate(tpch_table_name, row_count, chunk_size, dictionary);
  auto table = StorageManager::get().get_table(table_name);
  auto column_id = table->column_id_by_name(column_name);
  create_quotient_filters(table, column_id, quotient_size, remainder_size);
  if (btree) {
    table->populate_btree_index(column_id);
  } else {
    table->delete_btree_index(column_id);
  }
  if (art) {
    table->populate_art_index(column_id);
  } else {
    table->delete_art_index(column_id);
  }
  auto get_table = std::make_shared<GetTable>(table_name);
  //auto scan_value = std::string("1992-02-24");
  auto scan_value = 3000;
  auto table_scan = std::make_shared<TableScan>(get_table, column_id, PredicateCondition::Equals, scan_value);
  get_table->execute();
  return std::make_pair(table_scan, table);
}

std::pair<std::shared_ptr<AbstractOperator>, std::shared_ptr<const Table>> generate_acdoca_benchmark(
      std::string column_name, int row_count, int chunk_size, int quotient_size,
      int remainder_size, bool dictionary, bool btree, bool art) {
  auto table_name = acdoca_load_or_generate(row_count, chunk_size, dictionary);
  auto table = StorageManager::get().get_table(table_name);
  auto column_id = table->column_id_by_name("column_name");
  //auto quotient_size = static_cast<int>(std::ceil(std::log(chunk_size) / std::log(2)));
  create_quotient_filters(table, column_id, quotient_size, remainder_size);
  if (btree) {
    table->populate_btree_index(column_id);
  } else {
    table->delete_btree_index(column_id);
  }
  if (art) {
    table->populate_art_index(column_id);
  } else {
    table->delete_art_index(column_id);
  }
  auto get_table = std::make_shared<GetTable>(table_name);
  auto table_scan = std::make_shared<TableScan>(get_table, column_id, PredicateCondition::Equals, 3000);
  get_table->execute();
  return std::make_pair(table_scan, table);
}

void run_tpcc_benchmark(std::string table_name, std::string column_name, int warehouse_size, int chunk_size,
                        int quotient_size, int remainder_size, bool dictionary, bool btree, bool art, int sample_size,
                        std::shared_ptr<Table> results_table) {
  auto sum_time = std::chrono::microseconds(0);
  int size = -1;
  uint64_t row_count = 0;
  for (int i = 0; i < sample_size; i++) {
    auto benchmark = generate_tpcc_benchmark(table_name, column_name, warehouse_size, chunk_size, quotient_size,
                                             remainder_size, dictionary, btree, art);
    auto query = benchmark.first;
    auto table = benchmark.second;
    clear_cache();
    auto start = std::chrono::steady_clock::now();
    query->execute();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);
    sum_time += duration;
    if (size == -1) {
      size = table->ma_memory_consumption(table->column_id_by_name(column_name)) / 1000;
    }
    row_count = table->row_count();
    results_table->append({table_name, column_name, static_cast<int>(row_count), chunk_size, quotient_size,
                      remainder_size, static_cast<int>(dictionary), static_cast<int>(btree), static_cast<int>(art),
                      size, duration.count()});
    //auto print = std::make_shared<Print>(query);
    //print->execute();
  }

  auto avg_time = sum_time / sample_size;
  std::cout << "row_count: " << row_count
            << ", chunk_size: " << chunk_size
            << ", remainder_size: " << remainder_size
            << ", dictionary: " << dictionary
            << ", btree: " << btree
            << ", art: " << art
            << ", size: " << size
            << ", avg_time: " << avg_time.count()
            << std::endl;
}

void run_jcch_benchmark(std::string table_name, std::string column_name, int row_count, int chunk_size,
                        int quotient_size, int remainder_size, bool dictionary, bool btree, bool art, int sample_size,
                        std::shared_ptr<Table> results_table) {
  auto sum_time = std::chrono::microseconds(0);
  int size = -1;
  for (int i = 0; i < sample_size; i++) {
    auto benchmark = generate_jcch_benchmark(table_name, column_name, row_count, chunk_size, quotient_size,
                                             remainder_size, dictionary, btree, art);
    auto query = benchmark.first;
    auto table = benchmark.second;
    clear_cache();
    auto start = std::chrono::steady_clock::now();
    query->execute();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);
    sum_time += duration;
    if (size == -1) {
      size = table->ma_memory_consumption(table->column_id_by_name(column_name)) / 1000;
    }
    results_table->append({table_name, column_name, static_cast<int>(row_count), chunk_size, quotient_size,
                           remainder_size, static_cast<int>(dictionary), static_cast<int>(btree), static_cast<int>(art),
                           size, duration.count()});
    //std::cout << "Output size: " << query->get_output()->row_count() << std::endl;
   //auto print = std::make_shared<Print>(query);
   //print->execute();
  }

  auto avg_time = sum_time / sample_size;
  std::cout << "row_count: " << row_count
            << ", chunk_size: " << chunk_size
            << ", quotient_size: " << remainder_size
            << ", remainder_size: " << remainder_size
            << ", dictionary: " << dictionary
            << ", btree: " << btree
            << ", art: " << art
            << ", size: " << size
            << ", avg_time: " << avg_time.count()
            << std::endl;
}

void run_custom_benchmark(std::string type, int quotient_size, int remainder_size, bool dictionary, bool btree,
                    bool art, int row_count, int chunk_size, double pruning_rate, double selectivity, int sample_size,
                    std::shared_ptr<Table> results_table) {
  auto sum_time = std::chrono::microseconds(0);

  int size = -1;
  for (int i = 0; i < sample_size; i++) {
    auto benchmark = generate_custom_benchmark(type, quotient_size, remainder_size, dictionary, btree, art, row_count,
                                               chunk_size, pruning_rate, selectivity);
    auto query = benchmark.first;
    auto table = benchmark.second;
    clear_cache();
    auto start = std::chrono::steady_clock::now();
    query->execute();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
    sum_time += duration;
    if (size == -1) {
      size = table->ma_memory_consumption(ColumnID{0}) / 1000;
    }
    results_table->append({type, row_count, chunk_size, pruning_rate, selectivity, quotient_size, remainder_size,
                           static_cast<int>(dictionary), static_cast<int>(btree), static_cast<int>(art),
                           static_cast<int>(size), duration.count()});
    //auto print = std::make_shared<Print>(query);
    //print->execute();
    //std::cout << "Output size: " << query->get_output()->row_count() << std::endl;
  }

  auto avg_time = sum_time / sample_size;
  std::cout << "row_count: " << row_count
            << ", chunk_size: " << chunk_size
            << ", remainder_size: " << remainder_size
            << ", dictionary: " << dictionary
            << ", btree: " << btree
            << ", art: " << art
            << ", pruning_rate: " << pruning_rate
            << ", selectivity: " << selectivity
            << ", size: " << size
            << ", avg_time: " << avg_time.count()
            << std::endl;
}

void run_acdoca_benchmark(std::string column_name, int quotient_size, int remainder_size, bool dictionary, bool btree,
                      bool art, int row_count, int chunk_size, int sample_size, std::shared_ptr<Table> results_table) {
  auto sum_time = std::chrono::microseconds(0);

  int size = -1;
  for (int i = 0; i < sample_size; i++) {
    auto benchmark = generate_acdoca_benchmark(column_name, row_count, chunk_size, quotient_size, remainder_size,
                                               dictionary, btree, art);
    auto query = benchmark.first;
    auto table = benchmark.second;
    auto column_id = table->column_id_by_name(column_name);
    clear_cache();
    auto start = std::chrono::steady_clock::now();
    query->execute();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
    sum_time += duration;
    if (size == -1) {
      size = table->ma_memory_consumption(column_id) / 1000;
    }
    results_table->append({column_name, row_count, chunk_size, quotient_size, remainder_size,
                           static_cast<int>(dictionary), static_cast<int>(btree), static_cast<int>(art),
                           static_cast<int>(size), duration.count()});
  }

  auto avg_time = sum_time / sample_size;
  std::cout << "column_name: " << column_name
            << ", row_count: " << row_count
            << ", chunk_size: " << chunk_size
            << ", quotient_size: " << quotient_size
            << ", remainder_size: " << remainder_size
            << ", dictionary: " << dictionary
            << ", btree: " << btree
            << ", art: " << art
            << ", size: " << size
            << ", avg_time: " << avg_time.count()
            << std::endl;
}


void jcch_benchmark_series() {
  auto sample_size = 50;
  auto tpch_table_name = std::string("LINEITEM");
  auto column_name = std::string("L_PARTKEY");
  auto row_count = 6'000'000;
  auto chunk_size = 100'000;
  auto remainder_sizes = {0, /*2,*/ 4, 8};
  auto quotient_size = 15;

  auto results_table = std::make_shared<Table>();
  results_table->add_column("table_name", DataType::String, false);
  results_table->add_column("column_name", DataType::String, false);
  results_table->add_column("row_count", DataType::Int, false);
  results_table->add_column("chunk_size", DataType::Int, false);
  results_table->add_column("quotient_size", DataType::Int, false);
  results_table->add_column("remainder_size", DataType::Int, false);
  results_table->add_column("dictionary", DataType::Int, false);
  results_table->add_column("btree", DataType::Int, false);
  results_table->add_column("art", DataType::Int, false);
  results_table->add_column("size", DataType::Int, false);
  results_table->add_column("run_time", DataType::Int, false);

  std::cout << "------------------------" << std::endl;
  std::cout << "Benchmark configuration: " << std::endl;
  std::cout << "sample_size:  " << sample_size << std::endl;
  std::cout << "data:           " << "jcch" << std::endl;
  std::cout << "table_name:     " << tpch_table_name << std::endl;
  std::cout << "column_name:    " << column_name << std::endl;
  std::cout << "row_count:      " << row_count << std::endl;
  std::cout << "chunk_size:     " << chunk_size << std::endl;
  std::cout << "------------------------" << std::endl;

  auto start = std::chrono::steady_clock::now();

  // analyze_value_interval<int>(table_name, column_name);
  auto dictionary = false;
  auto art = false;
  auto btree = false;
  for (auto remainder_size : remainder_sizes) {
    run_jcch_benchmark(tpch_table_name, column_name, row_count, chunk_size, quotient_size, remainder_size,
      dictionary=false, btree=false, art=false, sample_size, results_table);
    run_jcch_benchmark(tpch_table_name, column_name, row_count, chunk_size, quotient_size, remainder_size,
      dictionary=true, btree=false, art=false, sample_size, results_table);
  }
  auto remainder_size = 0;
  run_jcch_benchmark(tpch_table_name, column_name, row_count, chunk_size, quotient_size, remainder_size=0,
    dictionary=false, btree=true, art=false, sample_size, results_table);
  run_jcch_benchmark(tpch_table_name, column_name, row_count, chunk_size, quotient_size, remainder_size=0,
    dictionary=true, btree=false, art=true, sample_size, results_table);
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now()-start);
  std::cout << "Benchmark ran " << duration.count() << " seconds" << std::endl;

  serialize_results_csv("jcch", results_table);
  StorageManager::get().reset();
}

void tpcc_benchmark_series() {
  auto sample_size = 50;
  auto tpcc_table_name = std::string("ORDER-LINE");
  auto column_name = std::string("OL_I_ID");
  auto warehouse_size = 10;
  auto chunk_size = 100000;
  auto quotient_size = 17;
  auto remainder_sizes = {0, 2, 4, 8};

  auto results_table = std::make_shared<Table>();
  results_table->add_column("table_name", DataType::String, false);
  results_table->add_column("column_name", DataType::String, false);
  results_table->add_column("row_count", DataType::Int, false);
  results_table->add_column("chunk_size", DataType::Int, false);
  results_table->add_column("quotient_size", DataType::Int, false);
  results_table->add_column("remainder_size", DataType::Int, false);
  results_table->add_column("dictionary", DataType::Int, false);
  results_table->add_column("btree", DataType::Int, false);
  results_table->add_column("art", DataType::Int, false);
  results_table->add_column("size", DataType::Int, false);
  results_table->add_column("run_time", DataType::Int, false);

  std::cout << "------------------------" << std::endl;
  std::cout << "Benchmark configuration: " << std::endl;
  std::cout << "sample_size:  " << sample_size << std::endl;
  std::cout << "data:           " << "tpcc" << std::endl;
  std::cout << "table_name:     " << tpcc_table_name << std::endl;
  std::cout << "column_name:    " << column_name << std::endl;
  std::cout << "warehouse_size: " << warehouse_size << std::endl;
  std::cout << "chunk_size:     " << chunk_size << std::endl;
  std::cout << "------------------------" << std::endl;

  auto start = std::chrono::steady_clock::now();

  // analyze_value_interval<int>(table_name, column_name);
  auto dictionary = false;
  auto art = false;
  auto btree = false;
  for (auto remainder_size : remainder_sizes) {
    run_tpcc_benchmark(tpcc_table_name, column_name, warehouse_size, chunk_size, quotient_size, remainder_size, dictionary=false,
                       btree=false, art=false, sample_size, results_table);
    run_tpcc_benchmark(tpcc_table_name, column_name, warehouse_size, chunk_size, quotient_size, remainder_size, dictionary=true,
                       btree=false, art=false, sample_size, results_table);
  }
  auto remainder_size = 0;
  run_tpcc_benchmark(tpcc_table_name, column_name, warehouse_size, chunk_size, quotient_size, remainder_size=0, dictionary=false,
                     btree=true, art=false, sample_size, results_table);
  run_tpcc_benchmark(tpcc_table_name, column_name, warehouse_size, chunk_size, quotient_size, remainder_size=0, dictionary=true,
                     btree=false, art=true, sample_size, results_table);
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now()-start);
  std::cout << "Benchmark ran " << duration.count() << " seconds" << std::endl;

  serialize_results_csv("tpcc", results_table);
  StorageManager::get().reset();
}

void custom_benchmark_series() {
  auto sample_size = 50;
  auto row_counts = {10'000'000};
  auto remainder_sizes = {0, 2, 4, 8};
  auto quotient_size = 17;
  auto chunk_sizes = {100'000};
  auto pruning_rates = {1.0, 0.5};
  auto selectivity = 1.0 / 3000.0;
  auto scan_types = {std::string("int"), std::string("string")};

  std::cout << "------------------------" << std::endl;
  std::cout << "Benchmark configuration: " << std::endl;
  std::cout << "sample_size:  " << sample_size << std::endl;
  std::cout << "data:         " << "custom" << std::endl;
  std::cout << "selectivity:  " << selectivity << std::endl;
  std::cout << "------------------------" << std::endl;

  auto results_table = std::make_shared<Table>();
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

  // analyze_value_interval<int>(table_name, column_name);
  auto start = std::chrono::steady_clock::now();
  for (auto scan_type : scan_types) {
    std::cout << " > Data Type: " << scan_type << std::endl;
    for (auto row_count : row_counts) {
      for (auto chunk_size : chunk_sizes) {
        for (auto pruning_rate : pruning_rates) {
          auto dictionary = false;
          auto btree = false;
          auto art = false;

          for (auto remainder_size : remainder_sizes) {
            run_custom_benchmark(scan_type, quotient_size, remainder_size, dictionary=false, btree=false, art=false, row_count,
               chunk_size, pruning_rate, selectivity, sample_size, results_table);
            run_custom_benchmark(scan_type, quotient_size, remainder_size, dictionary=true, btree=false, art=false, row_count,
              chunk_size, pruning_rate, selectivity, sample_size, results_table);
          }

          auto remainder_size = 0;
          run_custom_benchmark(scan_type, quotient_size, remainder_size=0, dictionary=false, btree=true, art=false, row_count,
            chunk_size, pruning_rate, selectivity, sample_size, results_table);
          run_custom_benchmark(scan_type, quotient_size, remainder_size=0, dictionary=true, btree=false, art=true, row_count,
            chunk_size, pruning_rate, selectivity, sample_size, results_table);
        }
      }
    }
  }
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now()-start);
  std::cout << "Benchmark ran " << duration.count() << " seconds" << std::endl;

  serialize_results_csv("custom", results_table);
  StorageManager::get().reset();
}

void acdoca_benchmark_series() {
  auto sample_size = 10;
  auto row_counts = {100'000'000};
  auto remainder_sizes = {0, 2, 4, 8};
  auto quotient_size = 17;
  auto chunk_sizes = {100'000};
  auto column_name = std::string("todo");

  std::cout << "------------------------" << std::endl;
  std::cout << "Benchmark configuration: " << std::endl;
  std::cout << "sample_size:  " << sample_size << std::endl;
  std::cout << "data:         " << "acdoca" << std::endl;
  std::cout << "column name:  " << column_name << std::endl;
  std::cout << "------------------------" << std::endl;

  auto results_table = std::make_shared<Table>();
  results_table->add_column("column_name", DataType::String, false);
  results_table->add_column("row_count", DataType::Int, false);
  results_table->add_column("chunk_size", DataType::Int, false);
  results_table->add_column("quotient_size", DataType::Int, false);
  results_table->add_column("remainder_size", DataType::Int, false);
  results_table->add_column("dictionary", DataType::Int, false);
  results_table->add_column("btree", DataType::Int, false);
  results_table->add_column("art", DataType::Int, false);
  results_table->add_column("size", DataType::Int, false);
  results_table->add_column("run_time", DataType::Int, false);

  // analyze_value_interval<int>(table_name, column_name);
  auto start = std::chrono::steady_clock::now();
  for (auto row_count : row_counts) {
    for (auto chunk_size : chunk_sizes) {
      auto dictionary = false;
      auto btree = false;
      auto art = false;
      for (auto remainder_size : remainder_sizes) {
        run_acdoca_benchmark(column_name, quotient_size, remainder_size, dictionary=false, btree=false, art=false, row_count,
           chunk_size, sample_size, results_table);
        run_acdoca_benchmark(column_name, quotient_size, remainder_size, dictionary=true, btree=false, art=false, row_count,
          chunk_size, sample_size, results_table);
      }
      auto remainder_size = 0;
      run_acdoca_benchmark(column_name, quotient_size, remainder_size=0, dictionary=false, btree=true, art=false, row_count,
        chunk_size, sample_size, results_table);
      run_acdoca_benchmark(column_name, quotient_size, remainder_size=0, dictionary=true, btree=false, art=true, row_count,
        chunk_size, sample_size, results_table);
    }
  }
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now()-start);
  std::cout << "Benchmark ran " << duration.count() << " seconds" << std::endl;

  serialize_results_csv("custom", results_table);
  StorageManager::get().reset();
}
