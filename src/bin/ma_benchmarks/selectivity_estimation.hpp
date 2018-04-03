#pragma once

#include "utils.hpp"

std::shared_ptr<Table> create_estimation_results_table() {
  auto column_definitions = TableColumnDefinitions();
  column_definitions.push_back(TableColumnDefinition("sample_size", DataType::Int, false));
  column_definitions.push_back(TableColumnDefinition("row_count", DataType::Int, false));
  column_definitions.push_back(TableColumnDefinition("distinct_values", DataType::Int, false));
  column_definitions.push_back(TableColumnDefinition("data_name", DataType::String, false));
  column_definitions.push_back(TableColumnDefinition("estimation_technique", DataType::String, false));
  column_definitions.push_back(TableColumnDefinition("error", DataType::Int, false));
  column_definitions.push_back(TableColumnDefinition("occurrences", DataType::Int, false));
  return std::make_shared<Table>(column_definitions, TableType::Data);
}

void filter_estimation_series(std::shared_ptr<Table> results_table, std::string data_name, int distinct_values) {
  //int sample_size = 300'000;
  int sample_size = 3'000'000;
  int row_count = 100'000;
  auto quotient_sizes = {12, 13, 14, 15, 16, 17};
  auto remainder_sizes = {2, 4, 8, 16};

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

      auto estimation_tec = std::string("filter_") + std::to_string(quotient_size) + "_" + std::to_string(remainder_size);
      for (auto pair : over_estimation) {
        results_table->append({sample_size, row_count, distinct_values, data_name, estimation_tec, pair.first, pair.second});
      }

      auto mean_error = sum_error / static_cast<double>(sample_size);
      std::cout << "estimation: "<< estimation_tec << ", "
                << "data: " << data_name << ", "
                << "distinct values: " << distinct_values << ", "
                << "mean error: " << mean_error << std::endl;
      if (under_estimation > 0) {
        std::cout << "UNDERCOUNTS: " << under_estimation << std::endl;
      }
    }
  }
}

void uniform_estimation_series(std::shared_ptr<Table> results_table, std::string data_name, int distinct_values) {
  auto row_count = 100'000;
  auto distribution = generate_distribution(data_name, row_count, distinct_values);
  auto estimation = generate_uniform_estimation(distribution);
  auto errors = std::map<int, int>();
  auto sum_error = 0;

  for(uint i = 0; i < distribution.size(); i++) {
    auto actual_count = static_cast<int>(distribution[i]);
    auto estimated_count = static_cast<int>(estimation[i]);
    auto error = estimated_count - actual_count;

    errors[error]++;
    sum_error += std::abs(error);
  }

  auto estimation_tec = std::string("uniform");
  for (auto pair : errors) {
    results_table->append({distinct_values, row_count, distinct_values, data_name, estimation_tec, pair.first, pair.second});
  }

  auto mean_error = sum_error / static_cast<double>(distribution.size());
  std::cout << "estimation: uniform, "
            << "data: " << data_name << ", "
            << "distinct values: " << distinct_values << ", "
            << "mean error: " << mean_error << std::endl;
}


void postgres1_estimation_series(std::shared_ptr<Table> results_table, std::string data_name,
                                   int distinct_values, uint granularity) {
  int row_count = 100'000;
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
  std::cout << "estimation: postgres1, "
            << "data: " << data_name << ", "
            << "distinct values: " << distinct_values << ", "
            << "mean error: " << mean_error << std::endl;
}

void postgres2_estimation_series(std::shared_ptr<Table> results_table, std::string data_name,
                                   int distinct_values, uint granularity) {
  int row_count = 100'000;
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
  std::cout << "estimation: postgres2, "
            << "data: " << data_name << ", "
            << "distinct values: " << distinct_values << ", "
            << "mean error: " << mean_error << std::endl;
}

std::shared_ptr<Table> create_estimation_examples_table() {
  auto column_definitions = TableColumnDefinitions();
  column_definitions.push_back(TableColumnDefinition("row_count", DataType::Int, false));
  column_definitions.push_back(TableColumnDefinition("distinct_values", DataType::Int, false));
  column_definitions.push_back(TableColumnDefinition("data_name", DataType::String, false));
  column_definitions.push_back(TableColumnDefinition("estimation_technique", DataType::String, false));
  column_definitions.push_back(TableColumnDefinition("value", DataType::Int, false));
  column_definitions.push_back(TableColumnDefinition("actual_count", DataType::Int, false));
  column_definitions.push_back(TableColumnDefinition("estimated_count", DataType::Int, false));
  return std::make_shared<Table>(column_definitions, TableType::Data);
}

void postgres1_estimation_example(std::shared_ptr<Table> results_table, std::string data_name,
                                              int distinct_values, int granularity) {
  std::cout << "postgres1 estimation example, "
            << "data: " << data_name << ", "
            << "granularity: " << granularity << std::endl;
  int row_count = 100'000;
  auto distribution = generate_distribution(data_name, row_count, distinct_values);
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
  std::cout << "postgres2 estimation example, "
            << "data: " << data_name << ", "
            << "granularity: " << granularity << std::endl;
  int row_count = 100'000;
  auto distribution = generate_distribution(data_name, row_count, distinct_values);
  auto estimation = generate_postgres2_estimation(distribution, granularity);
  for (int i = 0; i < distinct_values; i++) {
    auto actual_count = static_cast<int>(distribution[i]);
    auto estimated_count = static_cast<int>(estimation[i]);
    auto estimation_tec = std::string("postgres2_") + std::to_string(granularity);
    results_table->append({row_count, distinct_values, data_name, estimation_tec, i, actual_count, estimated_count});
  }
}

void uniform_estimation_example(std::shared_ptr<Table> results_table, std::string data_name, int distinct_values) {
  std::cout << "uniform estimation example, data: " << data_name << std::endl;
  int row_count = 100'000;
  auto distribution = generate_distribution(data_name, row_count, distinct_values);
  auto estimation = generate_uniform_estimation(distribution);
  for (int i = 0; i < distinct_values; i++) {
    auto actual_count = static_cast<int>(distribution[i]);
    auto estimated_count = static_cast<int>(estimation[i]);
    auto estimation_tec = std::string("uniform");
    results_table->append({row_count, distinct_values, data_name, estimation_tec, i, actual_count, estimated_count});
  }
}

void filter_estimation_examples(std::shared_ptr<Table> results_table, std::string data_name, int distinct_values) {
  std::cout << "filter estimation examples, data: " << data_name << std::endl;
  int row_count = 100'000;
  auto remainder_sizes = {2, 4, 8, 16};
  auto quotient_sizes = {12, 13, 14, 15, 16, 17};
  auto distribution = generate_distribution(data_name, row_count, distinct_values);
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

      if (under_estimation > 0) {
        std::cout << "UNDERCOUNTS: " << under_estimation << std::endl;
        //throw std::logic_error("filter with undercounts");
      }
    }
  }
}
