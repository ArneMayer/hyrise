#pragma once

#include "storage/index/counting_quotient_filter/counting_quotient_filter.hpp"
#include "data_generation.hpp"

double inserts_per_second_string() {
  const int n = 100'000;
  std::string values[n];
  uint8_t quotient_size = static_cast<uint8_t>(std::ceil(std::log2(n)));
  uint8_t remainder_size = 8;
  for (int i = 0; i < n; i++) {
    values[i] = random_string(16);
  }
  auto filter = CountingQuotientFilter<std::string>(quotient_size, remainder_size);

  clear_cache();
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < n; i++) {
    filter.insert(values[i]);
  }
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
  auto inserts_per_second = n / static_cast<double>(duration.count()) * 1'000'000;

  std::cout << inserts_per_second << " string inserts per second";
  return inserts_per_second;
}

double inserts_per_second_int() {
  const int n = 100'000;
  int values[n];
  uint8_t quotient_size = static_cast<uint8_t>(std::ceil(std::log2(n)));
  uint8_t remainder_size = 8;
  for (int i = 0; i < n; i++) {
    values[i] = random_int(0, 1'000'000);
  }
  auto filter = CountingQuotientFilter<int>(quotient_size, remainder_size);

  clear_cache();
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < n; i++) {
    filter.insert(values[i]);
  }
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-start);
  auto inserts_per_second = n / static_cast<double>(duration.count()) * 1'000'000;

  std::cout << inserts_per_second << " int inserts per second";
  return inserts_per_second;
}
