#include <algorithm>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"
#include "types.hpp"

#include "storage/dictionary_column.hpp"
#include "storage/index/b_tree/b_tree_index.hpp"

namespace opossum {

class BTreeIndexTest : public BaseTest {
 protected:
  void SetUp() override {
  }
};

TEST_F(BTreeIndexTest, VectorOfRandomInts) {
  /*
  std::vector<int> ints(10001);
  for (auto i = 0u; i < ints.size(); ++i) {
    ints[i] = i * 2;
  }

  std::random_device rd;
  std::mt19937 random_generator(rd());
  std::shuffle(ints.begin(), ints.end(), random_generator);

  auto column = create_dict_column_by_type<int>(DataType::Int, ints);
  auto index = std::make_shared<BTreeIndex<int>>(std::vector<std::shared_ptr<const BaseColumn>>({column}));

  for (auto i : {0, 2, 4, 8, 12, 14, 60, 64, 128, 130, 1024, 1026, 2048, 2050, 4096, 8190, 8192, 8194, 16382, 16384}) {
    EXPECT_EQ(column->get(*index->lower_bound({i})), i);
    EXPECT_EQ(column->get(*index->lower_bound({i + 1})), i + 2);
    EXPECT_EQ(column->get(*index->upper_bound({i})), i + 2);
    EXPECT_EQ(column->get(*index->upper_bound({i + 1})), i + 2);

    auto expected_lower = i;
    for (auto it = index->lower_bound({i}); it < index->lower_bound({i + 20}); ++it) {
      EXPECT_EQ(column->get(*it), expected_lower);
      expected_lower += 2;
    }
  }
  EXPECT_EQ(index->upper_bound({99999}), index->cend());
  */
}

} // namespace opossum