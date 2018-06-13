#include "ma_benchmarks/table_scan_benchmarks/table_scan_benchmark_series.hpp"
#include "ma_benchmarks/table_scan_benchmarks/custom_benchmark.hpp"
#include "ma_benchmarks/table_scan_benchmarks/tpcc_benchmark.hpp"
#include "ma_benchmarks/table_scan_benchmarks/jcch_benchmark.hpp"
#include "ma_benchmarks/table_scan_benchmarks/acdoca_benchmark.hpp"
#include "ma_benchmarks/data_structure_query.hpp"
#include "ma_benchmarks/selectivity_estimation.hpp"
#include "ma_benchmarks/filter_inserts.hpp"
#include "ma_benchmarks/analysis.hpp"

#include "storage/storage_manager.hpp"

int main() {
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
  //print_table_layout(acdoca_load_or_generate(100'000'000, 100'000, false));

  inserts_per_second_int();
  inserts_per_second_string();



  auto custom_series = TableScanBenchmarkSeries<CustomBenchmark>();
  custom_series.benchmark_name = "custom";
  custom_series.table_name = "Custom";
  custom_series.sample_size = 100;
  custom_series.column_names = {std::string("columnInt"), std::string("columnString")};
  custom_series.row_counts = {10'000'000};
  custom_series.chunk_sizes = {100'000};
  custom_series.remainder_sizes = {0, 2, 4, 8};
  custom_series.quotient_size = 17;
  custom_series.pruning_rates = {1.0, 0.5};
  custom_series.selectivities = {1.0 / 3000.0};
  custom_series.run();



  auto tpcc_series = TableScanBenchmarkSeries<TpccBenchmark>();
  tpcc_series.benchmark_name = "tpcc";
  tpcc_series.sample_size = 100;
  tpcc_series.table_name = "ORDER_LINE";
  tpcc_series.column_names = {"OL_I_ID", "OL_O_ID"};
  tpcc_series.row_counts = {10'000'000};
  tpcc_series.chunk_sizes = {100'000};
  tpcc_series.remainder_sizes = {0, 2, 4, 8};
  tpcc_series.quotient_size = 17;
  tpcc_series.run();



  auto jcch_series = TableScanBenchmarkSeries<JcchBenchmark>();
  jcch_series.benchmark_name = "jcch";
  jcch_series.sample_size = 100;
  jcch_series.table_name = "LINEITEM";
  jcch_series.column_names = {"L_PARTKEY", "L_SHIPDATE", "L_SHIPMODE"};
  jcch_series.row_counts = {6'000'000};
  jcch_series.chunk_sizes = {100'000};
  jcch_series.remainder_sizes = {0, 2, 4, 8};
  jcch_series.quotient_size = 17;
  jcch_series.run();



  auto acdoca_series = TableScanBenchmarkSeries<AcdocaBenchmark>();
  acdoca_series.benchmark_name = "acdoca";
  acdoca_series.table_name = "Acdoca";
  acdoca_series.sample_size = 100;
  acdoca_series.column_names = {"BELNR", "RBUKRS"};
  acdoca_series.row_counts = {10'000'000};
  acdoca_series.chunk_sizes = {100'000};
  acdoca_series.remainder_sizes = {0, 2, 4, 8};
  acdoca_series.quotient_size = 17;
  acdoca_series.run();


  /*
  auto tpcc_chunk_sizes_series = TableScanBenchmarkSeries<TpccBenchmark>();
  tpcc_chunk_sizes_series.benchmark_name = "tpcc-chunk-sizes";
  tpcc_chunk_sizes_series.sample_size = 100;
  tpcc_chunk_sizes_series.table_name = "ORDER_LINE";
  tpcc_chunk_sizes_series.column_names = {"OL_I_ID"};
  tpcc_chunk_sizes_series.row_counts = {10'000'000};
  tpcc_chunk_sizes_series.chunk_sizes = {1'000, 3'162, 10'000, 31'622, 100'000, 316'228, 1'000'000};
  tpcc_chunk_sizes_series.remainder_sizes = {0, 16};
  tpcc_chunk_sizes_series.auto_quotient_size = true;
  tpcc_chunk_sizes_series.interval_map_run = false;
  tpcc_chunk_sizes_series.dictionary_run = false;
  tpcc_chunk_sizes_series.art_run = false;
  tpcc_chunk_sizes_series.btree_run = false;
  tpcc_chunk_sizes_series.run();



  auto jcch_chunk_sizes_series = TableScanBenchmarkSeries<JcchBenchmark>();
  jcch_chunk_sizes_series.benchmark_name = "jcch-chunk-sizes";
  jcch_chunk_sizes_series.sample_size = 100;
  jcch_chunk_sizes_series.table_name = "LINEITEM";
  jcch_chunk_sizes_series.column_names = {"L_SHIPDATE"};
  jcch_chunk_sizes_series.row_counts = {6'000'000};
  jcch_chunk_sizes_series.chunk_sizes = {1'000, 3'162, 10'000, 31'622, 100'000, 316'228, 1'000'000};
  jcch_chunk_sizes_series.remainder_sizes = {0, 16};
  jcch_chunk_sizes_series.auto_quotient_size = true;
  jcch_chunk_sizes_series.interval_map_run = false;
  jcch_chunk_sizes_series.dictionary_run = false;
  jcch_chunk_sizes_series.art_run = false;
  jcch_chunk_sizes_series.btree_run = false;
  jcch_chunk_sizes_series.run();
  */

  dict_vs_filter_series_cached();
  dict_vs_filter_series_uncached();


/*
  // SELECTIVITY ESTIMATION
  auto estimation_results = create_estimation_results_table();
  auto example_results = create_estimation_examples_table();
  auto data_names = {"normal", "normal_shuffled", "uniform", "zipf", "zipf_shuffled"};
  auto distinct_counts = {3'000};
  auto postgres_granularities = {10, 50, 100};
  for (auto data_name : data_names) {
    for (auto distinct_count : distinct_counts) {
      // Filters
      filter_estimation_examples(example_results, data_name, distinct_count);
      filter_estimation_series(estimation_results, data_name, distinct_count);

      // Uniform estimation
      uniform_estimation_example(example_results, data_name, distinct_count);
      uniform_estimation_series(estimation_results, data_name, distinct_count);

      for (auto granularity : postgres_granularities) {
        // Postgres1
        postgres1_estimation_example(example_results, data_name, distinct_count, granularity);
        postgres1_estimation_series(estimation_results, data_name, distinct_count, granularity);

        // Postgres2
        postgres2_estimation_example(example_results, data_name, distinct_count, granularity);
        postgres2_estimation_series(estimation_results, data_name, distinct_count, granularity);
      }
    }
  }

  auto acdoca_columns = {"BELNR", "RBUKRS", "KUNNR", "RYEAR", "EBELN", "BUZEI"};
  for (auto column_name : acdoca_columns) {
    auto data_name = std::string("acdoca_") + column_name;
    // Filters
    filter_estimation_examples(example_results, data_name, 0);
    filter_estimation_series(estimation_results, data_name, 0);

    // Uniform estimation
    uniform_estimation_example(example_results, data_name, 0);
    uniform_estimation_series(estimation_results, data_name, 0);

    for (auto granularity : postgres_granularities) {
      // Postgres1
      postgres1_estimation_example(example_results, data_name, 0, granularity);
      postgres1_estimation_series(estimation_results, data_name, 0, granularity);

      // Postgres2
      postgres2_estimation_example(example_results, data_name, 0, granularity);
      postgres2_estimation_series(estimation_results, data_name, 0, granularity);
    }
  }


  serialize_results_csv("estimation", estimation_results);
  serialize_results_csv("estimation_examples", example_results);
  */

  //analyze_acdoca_pruning_rates();


  //analyze_all_tpcc_tables();
  //analyze_jcch_lineitem();
}
