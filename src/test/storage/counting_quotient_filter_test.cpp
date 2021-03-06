#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include <iostream>

#include "../base_test.hpp"
#include "gtest/gtest.h"
#include "types.hpp"

#include "storage/dictionary_column.hpp"
#include "storage/index/counting_quotient_filter/counting_quotient_filter.hpp"

namespace opossum {

class CountingQuotientFilterTest : public BaseTest {
 protected:
  void SetUp () override {
  }
};

TEST_F(CountingQuotientFilterTest, MultipleMembershipInt2) {
  auto filter = CountingQuotientFilter<int>(8, 2);
  filter.insert(10);
  filter.insert(20);
  filter.insert(30);
  filter.insert(40);
  filter.insert(50);
  filter.insert(60);
  filter.insert(70);
  filter.insert(80);
  filter.insert(90);
  filter.insert(100);
  EXPECT_TRUE(filter.count(10) >= 1);
  EXPECT_TRUE(filter.count(20) >= 1);
  EXPECT_TRUE(filter.count(30) >= 1);
  EXPECT_TRUE(filter.count(40) >= 1);
  EXPECT_TRUE(filter.count(50) >= 1);
  EXPECT_TRUE(filter.count(60) >= 1);
  EXPECT_TRUE(filter.count(70) >= 1);
  EXPECT_TRUE(filter.count(80) >= 1);
  EXPECT_TRUE(filter.count(90) >= 1);
  EXPECT_TRUE(filter.count(100) >= 1);
}

TEST_F(CountingQuotientFilterTest, MultipleMembershipInt4) {
  auto filter = CountingQuotientFilter<int>(16, 4);
  filter.insert(10);
  filter.insert(20);
  filter.insert(30);
  filter.insert(40);
  filter.insert(50);
  filter.insert(60);
  filter.insert(70);
  filter.insert(80);
  filter.insert(90);
  filter.insert(100);
  EXPECT_TRUE(filter.count(10) >= 1);
  EXPECT_TRUE(filter.count(20) >= 1);
  EXPECT_TRUE(filter.count(30) >= 1);
  EXPECT_TRUE(filter.count(40) >= 1);
  EXPECT_TRUE(filter.count(50) >= 1);
  EXPECT_TRUE(filter.count(60) >= 1);
  EXPECT_TRUE(filter.count(70) >= 1);
  EXPECT_TRUE(filter.count(80) >= 1);
  EXPECT_TRUE(filter.count(90) >= 1);
  EXPECT_TRUE(filter.count(100) >= 1);
}

TEST_F(CountingQuotientFilterTest, MultipleMembershipInt8) {
  auto filter = CountingQuotientFilter<int>(8, 8);
  filter.insert(10);
  filter.insert(20);
  filter.insert(30);
  filter.insert(40);
  filter.insert(50);
  filter.insert(60);
  filter.insert(70);
  filter.insert(80);
  filter.insert(90);
  filter.insert(100);
  EXPECT_TRUE(filter.count(10) >= 1);
  EXPECT_TRUE(filter.count(20) >= 1);
  EXPECT_TRUE(filter.count(30) >= 1);
  EXPECT_TRUE(filter.count(40) >= 1);
  EXPECT_TRUE(filter.count(50) >= 1);
  EXPECT_TRUE(filter.count(60) >= 1);
  EXPECT_TRUE(filter.count(70) >= 1);
  EXPECT_TRUE(filter.count(80) >= 1);
  EXPECT_TRUE(filter.count(90) >= 1);
  EXPECT_TRUE(filter.count(100) >= 1);
}

TEST_F(CountingQuotientFilterTest, MultipleMembershipInt16) {
  auto filter = CountingQuotientFilter<int>(8, 16);
  filter.insert(10);
  filter.insert(20);
  filter.insert(30);
  filter.insert(40);
  filter.insert(50);
  filter.insert(60);
  filter.insert(70);
  filter.insert(80);
  filter.insert(90);
  filter.insert(100);
  EXPECT_TRUE(filter.count(10) >= 1);
  EXPECT_TRUE(filter.count(20) >= 1);
  EXPECT_TRUE(filter.count(30) >= 1);
  EXPECT_TRUE(filter.count(40) >= 1);
  EXPECT_TRUE(filter.count(50) >= 1);
  EXPECT_TRUE(filter.count(60) >= 1);
  EXPECT_TRUE(filter.count(70) >= 1);
  EXPECT_TRUE(filter.count(80) >= 1);
  EXPECT_TRUE(filter.count(90) >= 1);
  EXPECT_TRUE(filter.count(100) >= 1);
}

TEST_F(CountingQuotientFilterTest, MultipleMembershipInt32) {
  auto filter = CountingQuotientFilter<int>(8, 32);
  filter.insert(10);
  filter.insert(20);
  filter.insert(30);
  filter.insert(40);
  filter.insert(50);
  filter.insert(60);
  filter.insert(70);
  filter.insert(80);
  filter.insert(90);
  filter.insert(100);
  EXPECT_TRUE(filter.count(10) >= 1);
  EXPECT_TRUE(filter.count(20) >= 1);
  EXPECT_TRUE(filter.count(30) >= 1);
  EXPECT_TRUE(filter.count(40) >= 1);
  EXPECT_TRUE(filter.count(50) >= 1);
  EXPECT_TRUE(filter.count(60) >= 1);
  EXPECT_TRUE(filter.count(70) >= 1);
  EXPECT_TRUE(filter.count(80) >= 1);
  EXPECT_TRUE(filter.count(90) >= 1);
  EXPECT_TRUE(filter.count(100) >= 1);
}

TEST_F(CountingQuotientFilterTest, MultipleMembershipString) {
  auto filter = CountingQuotientFilter<std::string>(8, 8);
  filter.insert("10");
  filter.insert("20");
  filter.insert("30");
  filter.insert("40");
  filter.insert("50");
  filter.insert("60");
  filter.insert("70");
  filter.insert("80");
  filter.insert("90");
  filter.insert("100");
  EXPECT_TRUE(filter.count("10") >= 1);
  EXPECT_TRUE(filter.count("20") >= 1);
  EXPECT_TRUE(filter.count("30") >= 1);
  EXPECT_TRUE(filter.count("40") >= 1);
  EXPECT_TRUE(filter.count("50") >= 1);
  EXPECT_TRUE(filter.count("60") >= 1);
  EXPECT_TRUE(filter.count("70") >= 1);
  EXPECT_TRUE(filter.count("80") >= 1);
  EXPECT_TRUE(filter.count("90") >= 1);
  EXPECT_TRUE(filter.count("100") >= 1);
}

} // namespace opossum
