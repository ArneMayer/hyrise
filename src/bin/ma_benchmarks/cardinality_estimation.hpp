#pragma once

#include "utils.hpp"

std::shared_ptr<Table> create_estimation_results_table() {
  auto results_table = std::make_shared<Table>();
  results_table->add_column("sample_size", DataType::Int, false);
  results_table->add_column("row_count", DataType::Int, false);
  results_table->add_column("distinct_values", DataType::Int, false);
  results_table->add_column("data_name", DataType::String, false);
  results_table->add_column("estimation_technique", DataType::String, false);
  results_table->add_column("error", DataType::Int, false);
  results_table->add_column("occurrences", DataType::Int, false);

  return results_table;
}

std::vector<uint> generate_distribution(std::string distribution_type, uint row_count, uint distinct_values) {
  std::vector<uint> distribution;
  if (distribution_type == "normal") {
    double variance = distinct_values / 6.0;
    distribution = generate_normal_distribution(row_count, distinct_values, variance);
  } else if (distribution_type == "uniform") {
    distribution = generate_uniform_distribution(row_count, distinct_values);
  } else if (distribution_type == "zipf") {
    distribution = generate_zipfian_distribution(row_count, distinct_values);
  } else {
    throw std::invalid_argument("data: " + distribution_type);
  }

  return distribution;
}

void filter_estimation_series(std::shared_ptr<Table> results_table, std::string data_name, int distinct_values) {
  //int sample_size = 300'000;
  int sample_size = 300'000;
  int row_count = 100'000;
  auto quotient_sizes = {12, 13, 14, 15, 16, 17};
  auto remainder_sizes = {2, 4, 8, 16};

  std::cout << "-----------------------------------" << std::endl;
  std::cout << " >> Filter Misestimation Series" << std::endl;
  std::cout << "sample size: " << sample_size << std::endl;
  std::cout << "distinct values: " << distinct_values << std::endl;
  std::cout << "data distribution: " << data_name << std::endl;
  std::cout << std::endl;

  auto distribution = generate_distribution(data_name, row_count, distinct_values);
  for (auto quotient_size : quotient_sizes) {
    for (auto remainder_size : remainder_sizes) {
      auto over_estimation = std::map<int, int>();
      std::shared_ptr<CountingQuotientFilter<int>> filter = nullptr;
      auto sum_error = 0;
      auto under_estimation = 0;
      auto filter_too_small = false;

      for (int sample = 0; sample < sample_size; sample++) {
        if (sample % distinct_values == 0) {
          filter = std::make_shared<CountingQuotientFilter<int>>(quotient_size, remainder_size);
          for (int i = 0; i < distinct_values; i++) {
            if (filter->is_full()) {
              filter_too_small = true;
              break;
            } else {
              filter->insert(i, distribution[i]);
            }
          }
        }

        if (filter_too_small) {
          break;
        }

        auto actual_count = distribution[sample % distinct_values];
        auto filter_count = filter->count(sample % distinct_values);
        if (filter_count < actual_count) {
          under_estimation++;
        }
        over_estimation[filter_count - actual_count]++;
        sum_error += filter_count - actual_count;
      }

      if (filter_too_small) {
        continue;
      }

      auto description = std::string("filter_") + std::to_string(quotient_size) + "_" + std::to_string(remainder_size);
      for (auto pair : over_estimation) {
        results_table->append({sample_size, row_count, distinct_values, data_name, description, pair.first, pair.second});
      }

      auto mean_error = sum_error / static_cast<double>(sample_size);
      std::cout << "quotient_size: " << quotient_size;
      std::cout << ", remainder_size: " << remainder_size;
      std::cout << ", mean error: " << mean_error << std::endl;
      if (under_estimation > 0) {
        std::cout << "UNDERCOUNTS: " << under_estimation << std::endl;
      }
    }
  }
}

void postgres1_estimation_series(std::shared_ptr<Table> results_table, std::string data_name,
                                   int distinct_values, uint granularity) {
  int row_count = 100'000;

  std::cout << "-----------------------------------" << std::endl;
  std::cout << " >> Postgres1 Misestimation Series" << std::endl;
  std::cout << "distinct values: " << distinct_values << std::endl;
  std::cout << "data distribution: " << data_name << std::endl;
  std::cout << "granularity: " << granularity << std::endl;
  std::cout << std::endl;

  auto distribution = generate_distribution(data_name, row_count, distinct_values);
  auto estimation = generate_postgres1_estimation(distribution, granularity);
  auto errors = std::map<int, int>();
  auto sum_error = 0;

  for(uint i = 0; i < distribution.size(); i++) {
    auto actual_count = static_cast<int>(distribution[i]);
    auto estimated_count = static_cast<int>(estimation[i]);
    auto error = estimated_count - actual_count;

    errors[error]++;
    sum_error += std::abs(error);
  }

  auto description = std::string("postgres1_") + std::to_string(granularity);
  for (auto pair : errors) {
    results_table->append({distinct_values, row_count, distinct_values, data_name, description, pair.first, pair.second});
  }

  auto mean_error = sum_error / static_cast<double>(distribution.size());
  std::cout << "Mean Error: " << std::to_string(mean_error) << std::endl;
}

void postgres2_estimation_series(std::shared_ptr<Table> results_table, std::string data_name,
                                   int distinct_values, uint granularity) {
  int row_count = 100'000;

  std::cout << "-----------------------------------" << std::endl;
  std::cout << " >> Postgres2 Misestimation Series" << std::endl;
  std::cout << "distinct values: " << distinct_values << std::endl;
  std::cout << "data distribution: " << data_name << std::endl;
  std::cout << "granularity: " << granularity << std::endl;
  std::cout << std::endl;

  auto distribution = generate_distribution(data_name, row_count, distinct_values);
  auto estimation = generate_postgres2_estimation(distribution, granularity);
  auto errors = std::map<int, int>();
  auto sum_error = 0;

  for(uint i = 0; i < distribution.size(); i++) {
    auto actual_count = static_cast<int>(distribution[i]);
    auto estimated_count = static_cast<int>(estimation[i]);
    auto error = estimated_count - actual_count;

    errors[error]++;
    sum_error += std::abs(error);
  }

  auto description = std::string("postgres2_") + std::to_string(granularity);
  for (auto pair : errors) {
    results_table->append({distinct_values, row_count, distinct_values, data_name, description, pair.first, pair.second});
  }

  auto mean_error = sum_error / static_cast<double>(distribution.size());
  std::cout << "Mean Error: " << std::to_string(mean_error) << std::endl;
}

std::shared_ptr<Table> create_estimation_examples_table() {
  auto results_table = std::make_shared<Table>();
  results_table->add_column("row_count", DataType::Int, false);
  results_table->add_column("distinct_values", DataType::Int, false);
  results_table->add_column("data_name", DataType::String, false);
  results_table->add_column("estimation_technique", DataType::String, false);
  results_table->add_column("value", DataType::Int, false);
  results_table->add_column("actual_count", DataType::Int, false);
  results_table->add_column("estimated_count", DataType::Int, false);

  return results_table;
}

void postgres1_estimation_example(std::shared_ptr<Table> results_table, std::string data_name,
                                              int distinct_values, int granularity) {
  int row_count = 100'000;

  std::cout << "-----------------------------------" << std::endl;
  std::cout << " >> Postgres1 Estimation Example" << std::endl;
  std::cout << "data: " << data_name << std::endl;
  std::cout << "granularity: " << granularity << std::endl;
  std::cout << std::endl;

  std::vector<uint> distribution;
  if (data_name == "normal") {
    double variance = distinct_values / 6.0;
    distribution = generate_normal_distribution(row_count, distinct_values, variance);
  } else if (data_name == "uniform") {
    distribution = generate_uniform_distribution(row_count, distinct_values);
  } else if (data_name == "zipf") {
    distribution = generate_zipfian_distribution(row_count, distinct_values);
  } else {
    throw std::invalid_argument("data: " + data_name);
  }

  auto estimation = generate_postgres1_estimation(distribution, granularity);
  for (uint i = 0; i < distribution.size(); i++) {
    auto actual_count = static_cast<int>(distribution[i]);
    auto estimated_count = static_cast<int>(estimation[i]);
    auto estimation_tec = std::string("postgres1_") + std::to_string(granularity);
    results_table->append({row_count, distinct_values, data_name, estimation_tec, static_cast<int>(i), actual_count, estimated_count});
  }
}

void postgres2_estimation_example(std::shared_ptr<Table> results_table, std::string data_name,
                                  int distinct_values, int granularity) {
  int row_count = 100'000;

  std::cout << "-----------------------------------" << std::endl;
  std::cout << " >> Postgres2 Estimation Example" << std::endl;
  std::cout << "data: " << data_name << std::endl;
  std::cout << "granularity: " << granularity << std::endl;
  std::cout << std::endl;

  std::vector<uint> distribution;
  if (data_name == "normal") {
    double variance = distinct_values / 6.0;
    distribution = generate_normal_distribution(row_count, distinct_values, variance);
  } else if (data_name == "uniform") {
    distribution = generate_uniform_distribution(row_count, distinct_values);
  } else if (data_name == "zipf") {
    distribution = generate_zipfian_distribution(row_count, distinct_values);
  } else {
    throw std::invalid_argument("data: " + data_name);
  }

  auto estimation = generate_postgres2_estimation(distribution, granularity);
  for (int i = 0; i < distinct_values; i++) {
    auto actual_count = static_cast<int>(distribution[i]);
    auto estimated_count = static_cast<int>(estimation[i]);
    auto estimation_tec = std::string("postgres2_") + std::to_string(granularity);
    results_table->append({row_count, distinct_values, data_name, estimation_tec, i, actual_count, estimated_count});
  }
}

void filter_estimation_examples(std::shared_ptr<Table> results_table, std::string data_name, int distinct_values) {
  int row_count = 100'000;
  auto remainder_sizes = {2, 4, 8, 16};
  auto quotient_sizes = {12, 13, 14, 15, 16, 17};

  std::cout << "-----------------------------------" << std::endl;
  std::cout << " >> Filter Estimation Examples" << std::endl;
  std::cout << "data: " << data_name << std::endl;
  std::cout << std::endl;

  std::vector<uint> distribution;
  if (data_name == "normal") {
    double variance = distinct_values / 6.0;
    distribution = generate_normal_distribution(row_count, distinct_values, variance);
  } else if (data_name == "uniform") {
    distribution = generate_uniform_distribution(row_count, distinct_values);
  } else if (data_name == "zipf") {
    distribution = generate_zipfian_distribution(row_count, distinct_values);
  } else {
    throw std::invalid_argument("data: " + data_name);
  }

  //auto postgres_estimation = generate_postgres_estimation(distribution, 10);

  for (auto quotient_size : quotient_sizes) {
    for (auto remainder_size : remainder_sizes) {
      auto filter = CountingQuotientFilter<int>(quotient_size, remainder_size);
      auto filter_too_small = false;
      for (int i = 0; i < distinct_values; i++) {
        //std::cout << filter.load_factor() << std::endl;
        if (filter.is_full()) {
          filter_too_small = true;
        } else {
          filter.insert(i, distribution[i]);
        }
      }
      if (filter_too_small) {
        continue;
      }

      auto over_estimation = 0;
      auto under_estimation = 0;
      for (int i = 0; i < distinct_values; i++) {
        auto actual_count = static_cast<int>(distribution[i]);
        auto filter_count = static_cast<int>(filter.count(i));
        auto estimation_tec = std::string("filter_") + std::to_string(quotient_size) + "_" + std::to_string(remainder_size);
        results_table->append({row_count, distinct_values, data_name, estimation_tec, i, actual_count, filter_count});
        if (filter_count > actual_count) {
          over_estimation += filter_count - actual_count;
        }
        if (filter_count < actual_count) {
          under_estimation += actual_count - filter_count;
        }
      }

      std::cout << "quotient_size: " << quotient_size;
      std::cout << ", remainder_size: " << remainder_size;
      std::cout << ", error: " << over_estimation << std::endl;
      if (under_estimation > 0) {
        std::cout << "UNDERCOUNTS: " << under_estimation << std::endl;
        //throw std::logic_error("filter with undercounts");
      }
    }
  }
}
