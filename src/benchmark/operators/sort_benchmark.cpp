#include <memory>

#include "benchmark/benchmark.h"

#include "../benchmark_basic_fixture.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

BENCHMARK_F(BenchmarkBasicFixture, BM_Sort)(benchmark::State& state) {
  clear_cache();

  auto warm_up = std::make_shared<Sort>(_table_wrapper_a, ColumnID{0} /* "a" */, OrderByMode::Ascending);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto sort = std::make_shared<Sort>(_table_wrapper_a, ColumnID{0} /* "a" */, OrderByMode::Ascending);
    sort->execute();
  }
}

}  // namespace opossum
