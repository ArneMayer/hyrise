/*
#include "ma_benchmarks/data_generation.hpp"
#include "ma_benchmarks/cardinality_estimation.hpp"
#include "ma_benchmarks/data_structure_query.hpp"
#include "ma_benchmarks/table_scan_benchmarks.hpp"
#include "ma_benchmarks/utils.hpp"
#include "ma_benchmarks/analysis.hpp"
*/

#include "ma_benchmarks/table_scan_benchmarks/table_scan_benchmark_series.hpp"
#include "ma_benchmarks/table_scan_benchmarks/custom_benchmark.hpp"
#include "ma_benchmarks/table_scan_benchmarks/tpcc_benchmark.hpp"
#include "ma_benchmarks/table_scan_benchmarks/jcch_benchmark.hpp"
#include "ma_benchmarks/table_scan_benchmarks/acdoca_benchmark.hpp"

/*
#include <iostream>
#include <chrono>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <cmath>

#include "types.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/print.hpp"

#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "resolve_type.hpp"
#include "storage/iterables/create_iterable_from_column.hpp"

#include <stdlib.h>
#include <pwd.h>
#include <stdio.h>

*/

#include "storage/storage_manager.hpp"

int main() {
  //print_table_layout(acdoca_load_or_generate(100'000'000, 100'000, false));
  /*
  auto custom_series = TableScanBenchmarkSeries<CustomBenchmark>();
  custom_series.benchmark_name = "custom";
  custom_series.sample_size = 10;
  custom_series.column_names = {std::string("columnInt"), std::string("columnString")};
  custom_series.row_counts = {1'000'000};
  custom_series.chunk_sizes = {100'000};
  custom_series.remainder_sizes = {0, 2, 4, 8};
  custom_series.quotient_size = 17;
  custom_series.pruning_rates = {1.0, 0.5};
  custom_series.selectivities = {1.0 / 3000.0};
  custom_series.run();

  auto tpcc_series = TableScanBenchmarkSeries<TpccBenchmark>();
  tpcc_series.benchmark_name = "tpcc";
  tpcc_series.sample_size = 10;
  tpcc_series.table_name = "ORDER-LINE";
  tpcc_series.column_names = {"OL_I_ID"};
  tpcc_series.row_counts = {1'000'000};
  tpcc_series.chunk_sizes = {100'000};
  tpcc_series.remainder_sizes = {0, 2, 4, 8};
  tpcc_series.quotient_size = 17;
  tpcc_series.run();

  auto jcch_series = TableScanBenchmarkSeries<JcchBenchmark>();
  jcch_series.benchmark_name = "jcch";
  jcch_series.sample_size = 10;
  jcch_series.table_name = "LINEITEM";
  jcch_series.column_names = {"L_PARTKEY"};
  jcch_series.row_counts = {6'000'000};
  jcch_series.chunk_sizes = {100'000};
  jcch_series.remainder_sizes = {0, 2, 4, 8};
  jcch_series.quotient_size = 17;
  jcch_series.run();
  */

  auto acdoca_series = TableScanBenchmarkSeries<AcdocaBenchmark>();
  acdoca_series.benchmark_name = "acdoca";
  acdoca_series.sample_size = 10;
  acdoca_series.column_names = {"RCLNT"};
  acdoca_series.row_counts = {1'000'000};
  acdoca_series.chunk_sizes = {100'000};
  acdoca_series.remainder_sizes = {0, 2, 4, 8};
  acdoca_series.quotient_size = 17;
  acdoca_series.run();

  //custom_benchmark_series();
  //tpcc_benchmark_series();
  //jcch_benchmark_series();
  /*
  auto table_name = jcch_load_or_generate("LINEITEM", 6000000, 100000, false);
  auto table = StorageManager::get().get_table(table_name);
  create_quotient_filters(table, table->column_id_by_name("L_SHIPDATE"), 17, 4);
  auto prunable_actual = analyze_skippable_chunks_actual(table_name, "L_SHIPDATE", std::string("1992-02-24"));
  auto prunable_filter = analyze_skippable_chunks_filter(table_name, "L_SHIPDATE", std::string("1992-02-24"));
  std::cout << "prunable actual: " << prunable_actual << std::endl;
  std::cout << "prunable filter: " << prunable_filter << std::endl;
  std::cout << "total chunks: " << table->chunk_count() << std::endl;
  */
  //acdoca_benchmark_series();
  /*
  dict_vs_filter_series_cached();
  dict_vs_filter_series_uncached();
  filter_cardinality_estimation_series("normal", 3'000);
  filter_cardinality_estimation_series("normal", 10'000);
  filter_cardinality_estimation_series("normal", 25'000);
  filter_cardinality_estimation_series("normal", 50'000);

  cardinality_misestimation_series("normal", 3'000);
  cardinality_misestimation_series("normal", 10'000);
  cardinality_misestimation_series("normal", 25'000);
  cardinality_misestimation_series("normal", 50'000);

  filter_cardinality_estimation_series("uniform", 3000);
  cardinality_misestimation_series("uniform", 3000);

  filter_cardinality_estimation_series("zipf", 3000);
  cardinality_misestimation_series("zipf", 3000);
  */
  //analyze_all_tpcc_tables();
  //analyze_jcch_lineitem();
}
