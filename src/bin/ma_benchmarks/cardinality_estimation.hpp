#pragma once

#include "utils.hpp"

void filter_misestimation_series(std::string distribution_type, int distinct_values) {
  //int sample_size = 300'000;
  int sample_size = 300'000;
  int value_count = 100'000;
  auto quotient_sizes = {12, 13, 14, 15, 16, 17};
  auto remainder_sizes = {2, 4, 8, 16};
  auto data = distribution_type + std::to_string(distinct_values);

  std::cout << " >> Filter Misestimation Series" << std::endl;
  std::cout << "sample size: " << sample_size << std::endl;
  std::cout << "distinct values: " << distinct_values << std::endl;
  std::cout << "data distribution: " << distribution_type << std::endl;

  auto results_table = std::make_shared<Table>();
  results_table->add_column("sample_size", DataType::Int, false);
  results_table->add_column("value_count", DataType::Int, false);
  results_table->add_column("distinct_values", DataType::Int, false);
  results_table->add_column("data", DataType::String, false);
  results_table->add_column("quotient_size", DataType::Int, false);
  results_table->add_column("remainder_size", DataType::Int, false);
  results_table->add_column("error", DataType::Int, false);
  results_table->add_column("occurrences", DataType::Int, false);

  std::vector<uint> distribution;
  if (distribution_type == "normal") {
    double variance = distinct_values / 6.0;
    distribution = generate_normal_distribution(value_count, distinct_values, variance);
  } else if (distribution_type == "uniform") {
    distribution = generate_uniform_distribution(value_count, distinct_values);
  } else if (distribution_type == "zipf") {
    distribution = generate_zipfian_distribution(value_count, distinct_values);
  } else {
    throw std::invalid_argument("data: " + data);
  }

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

      for (auto pair : over_estimation) {
        results_table->append({sample_size, value_count, distinct_values, data, quotient_size,
                               remainder_size, pair.first, pair.second});
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
  serialize_results_csv("filter_misestimation_" + data, results_table);
}

void postgres_misestimation_series(std::string distribution_type, int distinct_values, uint granularity) {
  int value_count = 100'000;
  auto data = distribution_type + std::to_string(distinct_values);

  std::cout << " >> Postgres Misestimation Series" << std::endl;
  std::cout << "distinct values: " << distinct_values << std::endl;
  std::cout << "data distribution: " << distribution_type << std::endl;

  auto results_table = std::make_shared<Table>();
  results_table->add_column("value_count", DataType::Int, false);
  results_table->add_column("distinct_values", DataType::Int, false);
  results_table->add_column("data", DataType::String, false);
  results_table->add_column("granularity", DataType::Int, false);
  results_table->add_column("error", DataType::Int, false);
  results_table->add_column("occurrences", DataType::Int, false);

  std::vector<uint> distribution;
  if (distribution_type == "normal") {
    double variance = distinct_values / 6.0;
    distribution = generate_normal_distribution(value_count, distinct_values, variance);
  } else if (distribution_type == "uniform") {
    distribution = generate_uniform_distribution(value_count, distinct_values);
  } else if (distribution_type == "zipf") {
    distribution = generate_zipfian_distribution(value_count, distinct_values);
  } else {
    throw std::invalid_argument("data: " + data);
  }

  auto estimation = generate_postgres_estimation(distribution, granularity);
  auto errors = std::map<int, int>();
  auto sum_error = 0;

  for(uint i = 0; i < distribution.size(); i++) {
    auto actual_count = static_cast<int>(distribution[i]);
    auto estimated_count = static_cast<int>(estimation[i]);
    auto error = estimated_count - actual_count;

    errors[error]++;
    sum_error += std::abs(error);
  }

  for (auto pair : errors) {
    results_table->append({value_count, distinct_values, data, static_cast<int>(granularity), pair.first, pair.second});
  }

  auto mean_error = sum_error / static_cast<double>(distribution.size());
  std::cout << "Mean Error: " << std::to_string(mean_error) << std::endl;
  serialize_results_csv("postgres_misestimation_" + data, results_table);
}

void filter_cardinality_estimation_series(std::string distribution_type, int distinct_values) {
  int value_count = 100'000;
  auto remainder_sizes = {2, 4, 8, 16};
  auto quotient_sizes = {12, 13, 14, 15, 16, 17};
  auto data = distribution_type + std::to_string(distinct_values);

  std::cout << " >> Single Filter Test" << std::endl;
  std::cout << "data: " << data << std::endl;

  auto results_table = std::make_shared<Table>();
  results_table->add_column("value_count", DataType::Int, false);
  results_table->add_column("distinct_values", DataType::Int, false);
  results_table->add_column("data", DataType::String, false);
  results_table->add_column("quotient_size", DataType::Int, false);
  results_table->add_column("remainder_size", DataType::Int, false);
  results_table->add_column("value", DataType::Int, false);
  results_table->add_column("actual_count", DataType::Int, false);
  results_table->add_column("filter_count", DataType::Int, false);

  std::vector<uint> distribution;
  if (distribution_type == "normal") {
    double variance = distinct_values / 6.0;
    distribution = generate_normal_distribution(value_count, distinct_values, variance);
  } else if (distribution_type == "uniform") {
    distribution = generate_uniform_distribution(value_count, distinct_values);
  } else if (distribution_type == "zipf") {
    distribution = generate_zipfian_distribution(value_count, distinct_values);
  } else {
    throw std::invalid_argument("data: " + data);
  }

  //auto postgres_estimation = generate_postgres_estimation(distribution, 10);

  for (auto quotient_size : quotient_sizes) {
    for (auto remainder_size : remainder_sizes) {
      auto filter = CountingQuotientFilter<int>(quotient_size, remainder_size);
      auto filter_too_small = false;
      for (int i = 0; i < distinct_values; i++) {
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
        //auto postgres_count = static_cast<int>(postgres_estimation[i]);
        results_table->append({value_count, distinct_values, data, quotient_size, remainder_size,
                               i, actual_count, /*postgres_count,*/ filter_count});
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
      }
    }
  }

  serialize_results_csv("filter_cardinality_estimation_" + data, results_table);
}
