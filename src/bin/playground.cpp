#include "ma_benchmarks/data_generation.hpp"
#include "ma_benchmarks/cardinality_estimation.hpp"
#include "ma_benchmarks/data_structure_query.hpp"
#include "ma_benchmarks/table_scan_benchmarks.hpp"
#include "ma_benchmarks/utils.hpp"
#include "ma_benchmarks/analysis.hpp"

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
  //custom_benchmark_series();
  //tpcc_benchmark_series();
  //jcch_benchmark_series();
  auto table_name = jcch_load_or_generate("LINEITEM", 6000000, 100000, false);
  auto table = StorageManager::get().get_table(table_name);
  create_quotient_filters(table, table->column_id_by_name("L_SHIPDATE"), 17, 4);
  auto prunable_actual = analyze_skippable_chunks_actual(table_name, "L_SHIPDATE", std::string("1992-02-24"));
  auto prunable_filter = analyze_skippable_chunks_filter(table_name, "L_SHIPDATE", std::string("1992-02-24"));
  std::cout << "prunable actual: " << prunable_actual << std::endl;
  std::cout << "prunable filter: " << prunable_filter << std::endl;
  std::cout << "total chunks: " << table->chunk_count() << std::endl;
  //acdoca_benchmark_series();
  //dict_vs_filter_series_cached();
  //dict_vs_filter_series_uncached();
  //filter_cardinality_estimation_series("normal", 3'000);
  //filter_cardinality_estimation_series("normal", 10'000);
  //filter_cardinality_estimation_series("normal", 25'000);
  //filter_cardinality_estimation_series("normal", 50'000);

  //cardinality_misestimation_series("normal", 3'000);
  //cardinality_misestimation_series("normal", 10'000);
  //cardinality_misestimation_series("normal", 25'000);
  //cardinality_misestimation_series("normal", 50'000);

  //filter_cardinality_estimation_series("uniform", 3000);
  //cardinality_misestimation_series("uniform", 3000);

  //filter_cardinality_estimation_series("zipf", 3000);
  //cardinality_misestimation_series("zipf", 3000);

  //analyze_all_tpcc_tables();
  //analyze_jcch_lineitem();
}
