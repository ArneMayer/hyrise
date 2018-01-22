#pragma once

#include "utils.hpp"

using namespace opossum;

void dict_query_benchmark_string(std::shared_ptr<std::vector<std::string>> dictionary, int n, int string_length) {
  auto value = random_string(string_length);
  for (int i = 0; i < n; i++) {
    std::binary_search(dictionary->begin(), dictionary->end(), value);
  }
}

void filter_query_benchmark_string(std::shared_ptr<CountingQuotientFilter<std::string>> filter, int n, int string_length) {
  auto value = random_string(string_length);
  for (int i = 0; i < n; i++) {
    filter->count(value);
  }
}

void dict_query_benchmark_int(std::shared_ptr<std::vector<int>> dictionary, int n) {
  auto value = random_int(0, 100000);
  for (int i = 0; i < n; i++) {
    std::binary_search(dictionary->begin(), dictionary->end(), value);
  }
}

void filter_query_benchmark_int(std::shared_ptr<CountingQuotientFilter<int>> filter, int n) {
  auto value = random_int(0, 100000);
  for (int i = 0; i < n; i++) {
    filter->count(value);
  }
}

void dict_query_benchmark_string_uncached(std::vector<std::shared_ptr<std::vector<std::string>>>& dicts,
                                          int n, int string_length) {
  auto value = random_string(string_length);
  for (int i = 0; i < n; i++) {
    auto dictionary = dicts[i % dicts.size()];
    std::lower_bound(dictionary->cbegin(), dictionary->cend(), value);
  }
}

void filter_query_benchmark_string_uncached(std::vector<std::shared_ptr<CountingQuotientFilter<std::string>>>& filters,
                                            int n, int string_length) {
  auto value = random_string(string_length);
  for (int i = 0; i < n; i++) {
    auto filter = filters[i % filters.size()];
    filter->count(value);
  }
}

void dict_query_benchmark_int_uncached(std::vector<std::shared_ptr<std::vector<int>>>& dicts, int n) {
  auto value = random_int(0, 100000);
  for (int i = 0; i < n; i++) {
    auto dictionary = dicts[i % dicts.size()];
    std::lower_bound(dictionary->begin(), dictionary->end(), value);
  }
}

void filter_query_benchmark_int_uncached(std::vector<std::shared_ptr<CountingQuotientFilter<int>>>& filters, int n) {
  auto value = random_int(0, 100000);
  for (int i = 0; i < n; i++) {
    auto filter = filters[i % filters.size()];
    filter->count(value);
  }
}

void dict_vs_filter_series_cached() {
  auto string_length = 16;
  auto size = 1'000'000;
  auto n = 10'000'000;
  auto quotient_size = 20;
  auto remainder_size = 4;

  std::cout << " >> Dictionary Lookup vs. Filter Query Cached Series" << std::endl;

  auto results_table = std::make_shared<Table>();
  results_table->add_column("data_type", DataType::String, false);
  results_table->add_column("data_structure", DataType::String, false);
  results_table->add_column("value_count", DataType::Int, false);
  results_table->add_column("sample_size", DataType::Int, false);
  results_table->add_column("run_time", DataType::Int, false);

  auto dict_string = generate_dictionary_string(size, string_length);
  clear_cache();
  auto start = std::chrono::steady_clock::now();
  dict_query_benchmark_string(dict_string, n, string_length);
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
  std::cout << "string dictionary: " << duration.count() << std::endl;
  results_table->append({std::string("string"), std::string("dictionary"), size, n, static_cast<int>(duration.count())});

  auto filter_string = generate_filter_string(size, string_length, quotient_size, remainder_size);
  clear_cache();
  start = std::chrono::steady_clock::now();
  filter_query_benchmark_string(filter_string, n, string_length);
  duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
  std::cout << "string filter: " << duration.count() << std::endl;
  results_table->append({std::string("string"), std::string("filter"), size, n, static_cast<int>(duration.count())});

  auto dict_int = generate_dictionary_int(size);
  clear_cache();
  start = std::chrono::steady_clock::now();
  dict_query_benchmark_int(dict_int, n);
  duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
  std::cout << "int dictionary: " << duration.count() << std::endl;
  results_table->append({std::string("int"), std::string("dictionary"), size, n, static_cast<int>(duration.count())});

  auto filter_int = generate_filter_int(size, quotient_size, remainder_size);
  clear_cache();
  start = std::chrono::steady_clock::now();
  filter_query_benchmark_int(filter_int, n);
  duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
  std::cout << "int filter: " << duration.count() << std::endl;
  results_table->append({std::string("int"), std::string("filter"), size, n, static_cast<int>(duration.count())});

  serialize_results_csv("data_structure_query_cached", results_table);
}

void dict_vs_filter_series_uncached() {
  auto string_length = 16;
  int size = 1'000'000;
  int n = 10'000'000;
  auto quotient_size = 20;
  auto remainder_size = 4;
  auto number_of_filters = 1000;

  std::cout << " >> Dictionary Lookup vs. Filter Query Uncached Series" << std::endl;

  auto results_table = std::make_shared<Table>();
  results_table->add_column("data_type", DataType::String, false);
  results_table->add_column("data_structure", DataType::String, false);
  results_table->add_column("value_count", DataType::Int, false);
  results_table->add_column("sample_size", DataType::Int, false);
  results_table->add_column("run_time", DataType::Int, false);

  std::vector<std::shared_ptr<CountingQuotientFilter<int>>> int_filters;
  std::vector<std::shared_ptr<CountingQuotientFilter<std::string>>> string_filters;
  std::vector<std::shared_ptr<std::vector<int>>> int_dicts;
  std::vector<std::shared_ptr<std::vector<std::string>>> string_dicts;

  for (int i = 0; i < number_of_filters; i++) {
    int_filters.push_back(generate_filter_int(size, quotient_size, remainder_size));
    string_filters.push_back(generate_filter_string(size, string_length, quotient_size, remainder_size));
    int_dicts.push_back(generate_dictionary_int(size));
    string_dicts.push_back(generate_dictionary_string(size, string_length));
  }

  clear_cache();
  auto start = std::chrono::steady_clock::now();
  dict_query_benchmark_string_uncached(string_dicts, n, string_length);
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
  std::cout << "string dictionary uncached: " << duration.count() << std::endl;
  results_table->append({std::string("string"), std::string("dictionary"), size, n, static_cast<int>(duration.count())});

  clear_cache();
  start = std::chrono::steady_clock::now();
  filter_query_benchmark_string_uncached(string_filters, n, string_length);
  duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
  std::cout << "string filter uncached: " << duration.count() << std::endl;
  results_table->append({std::string("string"), std::string("filter"), size, n, static_cast<int>(duration.count())});

  clear_cache();
  start = std::chrono::steady_clock::now();
  dict_query_benchmark_int_uncached(int_dicts, n);
  duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
  std::cout << "int dictionary uncached: " << duration.count() << std::endl;
  results_table->append({std::string("int"), std::string("dictionary"), size, n, static_cast<int>(duration.count())});

  clear_cache();
  start = std::chrono::steady_clock::now();
  filter_query_benchmark_int_uncached(int_filters, n);
  duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
  std::cout << "int filter uncached: " << duration.count() << std::endl;
  results_table->append({std::string("int"), std::string("filter"), size, n, static_cast<int>(duration.count())});

  serialize_results_csv("data_structure_query_uncached", results_table);
}
