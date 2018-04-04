#pragma once

#include <vector>

#include "base_column_analyzer.hpp"

using namespace opossum;

template <typename ColumnDataType>
class ColumnAnalyzer : public BaseColumnAnalyzer {
 public:
  ColumnAnalyzer(std::shared_ptr<const Table> table, ColumnID column_id)
    : BaseColumnAnalyer(table, column_id);
  ColumnAnalyzer() = delete;
  virtual ~ColumnAnalyzer() = default;

  virtual std::vector<uint> get_chunk_distribution(ChunkID chunk_id) override {
    auto base_column = _table->get_chunk(chunk_id)->get_column(column_id);
    auto base_dict_column = std::dynamic_pointer_cast<const BaseDictionaryColumn>(base_column);
    auto materialization = std::vector<ColumnDataType>();
    materialization.reserve(base_column->size());
    auto distribution = std::vector<uint>(base_dict_column->unique_values_count());

    // Copy values
    resolve_column_type<ColumnDataType>(*base_column, [&](const auto& typed_column) {
      auto iterable_left = create_iterable_from_column<ColumnDataType>(typed_column);
      iterable_left.for_each([&](const auto& value) {
        if (value.is_null()) return;
        materialization.push_back(value.value());
      });
    });

    // Sort values
    std::sort(materialization.begin(), materialization.end());

    // Determine Counts
    uint value_id = 0;
    T current_value = materialization[0];
    uint count = 1;
    for (uint i = 1; i < materialization.size(); i++) {
      if (materialization[i] == current_value) {
        count++;
      } else {
        distribution[value_id] = count;

        value_id++;
        count = 1;
        current_value = materialization[i];
      }
    }

    return distribution;
  }
};
