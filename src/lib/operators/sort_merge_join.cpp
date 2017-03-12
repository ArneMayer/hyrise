#include "sort_merge_join.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace opossum {
// TODO(Fabian): Add Numa-realted comments/information!

SortMergeJoin::SortMergeJoin(const std::shared_ptr<AbstractOperator> left,
                             const std::shared_ptr<AbstractOperator> right,
                             optional<std::pair<const std::string&, const std::string&>> column_names,
                             const std::string& op, const JoinMode mode)
    : AbstractOperator(left, right), _op{op}, _mode{mode} {
  // Check optional column names
  // Per definition either two names are specified or none
  if (column_names) {
    _left_column_name = column_names->first;
    _right_column_name = column_names->second;

    if (left == nullptr) {
      std::string message = "SortMergeJoin::SortMergeJoin: left input operator is null";
      std::cout << message << std::endl;
      throw std::exception(std::runtime_error(message));
    }

    if (right == nullptr) {
      std::string message = "SortMergeJoin::SortMergeJoin: right input operator is null";
      std::cout << message << std::endl;
      throw std::exception(std::runtime_error(message));
    }
    // Check column_type
    auto left_column_id = _input_left->column_id_by_name(_left_column_name);
    auto right_column_id = _input_right->column_id_by_name(_right_column_name);
    auto left_column_type = _input_left->column_type(left_column_id);
    auto right_column_type = _input_right->column_type(right_column_id);

    if (left_column_type != right_column_type) {
      std::string message = "SortMergeJoin::SortMergeJoin: column type \"" + left_column_type + "\" of left column \"" +
                            _left_column_name + "\" does not match colum type \"" + right_column_type +
                            "\" of right column \"" + _right_column_name + "\"!";
      std::cout << message << std::endl;
      throw std::exception(std::runtime_error(message));
    }
    // Create implementation to compute join result
    if (_mode != JoinMode::Cross) {
      _impl = make_unique_by_column_type<AbstractOperatorImpl, SortMergeJoinImpl>(left_column_type, *this);
    } else {
      _product = std::make_shared<Product>(left, right, "left", "right");
    }
  } else {
    // No names specified --> this is only valid if we want to cross-join
    if (_mode != JoinMode::Cross) {
      std::string message = "SortMergeJoin::SortMergeJoin: No columns specified for join operator";
      std::cout << message << std::endl;
      throw std::exception(std::runtime_error(message));
    } else {
      _product = std::make_shared<Product>(left, right, "left", "right");
    }
  }

  /*_pos_list_left = std::make_shared<PosList>();
  _pos_list_right = std::make_shared<PosList>();*/
}

void SortMergeJoin::execute() {
  if (_mode != JoinMode::Cross) {
    _impl->execute();
  } else {
    _product->execute();
  }
}

std::shared_ptr<const Table> SortMergeJoin::get_output() const {
  if (_mode != JoinMode::Cross) {
    return _impl->get_output();
  } else {
    return _product->get_output();
  }
}

const std::string SortMergeJoin::name() const { return "SortMergeJoin"; }

uint8_t SortMergeJoin::num_in_tables() const { return 2u; }

uint8_t SortMergeJoin::num_out_tables() const { return 1u; }

/**
** Start of implementation
**/

template <typename T>
SortMergeJoin::SortMergeJoinImpl<T>::SortMergeJoinImpl(SortMergeJoin& sort_merge_join)
    : _sort_merge_join{sort_merge_join} {
  if (_sort_merge_join._op == "=") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left == value_right; };
  } else if (_sort_merge_join._op == "<") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left < value_right; };
  } else {
    std::string message = "SortMergeJoinImpl::SortMergeJoinImpl: Unknown operator " + _sort_merge_join._op;
    std::cout << message << std::endl;
    throw std::exception(std::runtime_error(message));
  }
  /* right now only equi-joins supported
  * (and test wise "<")
  * but for other join ops it is recommended to use another join, as we do not gain any benfit
  * as the output is in O(n²) for all Non-Equi Joins

  if (_nested_loop_join._op == "=") {
  _compare = [](const T& value_left, const T& value_right) -> bool { return value_left == value_right; };
  } else if (_nested_loop_join._op == "<") {
  _compare = [](const T& value_left, const T& value_right) -> bool { return value_left < value_right; };
  } else if (_nested_loop_join._op == ">") {
  _compare = [](const T& value_left, const T& value_right) -> bool { return value_left < value_right; };
  } else if (_nested_loop_join._op == ">=") {
  _compare = [](const T& value_left, const T& value_right) -> bool { return value_left >= value_right; };
  } else if (_nested_loop_join._op == "<=") {
  _compare = [](const T& value_left, const T& value_right) -> bool { return value_left <= value_right; };
  } else if (_nested_loop_join._op == "!=") {
  _compare = [](const T& value_left, const T& value_right) -> bool { return value_left != value_right; };
  } else {
  std::string message = "SortMergeJoinImpl::SortMergeJoinImpl: Unknown operator " + _nested_loop_join._op;
  std::cout << message << std::endl;
  throw std::exception(std::runtime_error(message));
  }*/
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::sort_partition(const std::vector<ChunkID> chunk_ids,
                                                         std::shared_ptr<const Table> input,
                                                         const std::string& column_name, bool left) {
  for (auto chunk_id : chunk_ids) {
    auto& chunk = input->get_chunk(chunk_id);
    auto column = chunk.get_column(input->column_id_by_name(column_name));
    auto context = std::make_shared<SortContext>(chunk_id, left);
    column->visit(*this, context);
  }
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::sort_table(std::shared_ptr<SortedTable> sort_table,
                                                     std::shared_ptr<const Table> input, const std::string& column_name,
                                                     bool left) {
  // sort_table = std::make_shared<SortedTable>();
  sort_table->_partition.resize(input->chunk_count());
  for (ChunkID chunk_id = 0; chunk_id < input->chunk_count(); ++chunk_id) {
    sort_table->_partition[chunk_id]._values.resize(input->get_chunk(chunk_id).size());
  }

  const uint32_t threshold = 100000;
  std::vector<std::thread> threads;
  uint32_t size = 0;
  std::vector<ChunkID> chunk_ids;
  for (ChunkID chunk_id = 0; chunk_id < input->chunk_count(); ++chunk_id) {
    /*
    auto& chunk = _sort_merge_join._input_right->get_chunk(chunk_id);
    auto column =
        chunk.get_column(_sort_merge_join._input_right->column_id_by_name(_sort_merge_join._right_column_name));
    auto context = std::make_shared<SortContext>(chunk_id, false);
    column->visit(*this, context);
    */
    size += input->chunk_size();
    chunk_ids.push_back(chunk_id);
    if (size > threshold || chunk_id == input->chunk_count() - 1) {
      threads.push_back(std::thread(&SortMergeJoin::SortMergeJoinImpl<T>::sort_partition, *this, chunk_ids, input,
                                    column_name, left));
      size = 0;
      chunk_ids.clear();
    }
    // threads.at(chunk_id) = std::thread(&SortMergeJoin::SortMergeJoinImpl<T>::sort_right_partition, *this, chunk_id);
  }

  for (auto& thread : threads) {
    thread.join();
  }

  if (_partition_count == 1) {
    std::vector<std::pair<T, RowID>> partition_values;
    for (auto& s_chunk : sort_table->_partition) {
      for (auto& entry : s_chunk._values) {
        partition_values.push_back(entry);
      }
    }

    sort_table->_partition.clear();
    sort_table->_partition.resize(1);

    for (auto& entry : partition_values) {
      sort_table->_partition[0]._values.push_back(entry);
    }
  } else {
    // Do radix-partitioning here for _partition_count>1 partitions

    std::vector<std::vector<std::pair<T, RowID>>> partitions;
    partitions.resize(_partition_count);
    // for prefix computation we need to table-wide know how many entries there are for each partition
    for (uint8_t i = 0; i < _partition_count; ++i) {
      sort_table->_histogram.insert(std::pair<uint8_t, uint32_t>(i, 0));
    }

    // Each chunk should prepare additional data to enable partitioning
    for (auto& s_chunk : sort_table->_partition) {
      for (uint8_t i = 0; i < _partition_count; ++i) {
        s_chunk._histogram.insert(std::pair<uint8_t, uint32_t>(i, 0));
        s_chunk._prefix.insert(std::pair<uint8_t, uint32_t>(i, 0));
      }
      // fill histogram
      for (auto& entry : s_chunk._values) {
        auto radix = get_radix<T>(entry.first, _partition_count - 1);
        ++(s_chunk._histogram[radix]);
      }
    }

    // Each chunk need to sequentially fill _prefix map to actually fill partition of tables in parallel
    for (auto& s_chunk : sort_table->_partition) {
      for (size_t radix = 0; radix < _partition_count; ++radix) {
        s_chunk._prefix[radix] = sort_table->_histogram[radix];
        sort_table->_histogram[radix] += s_chunk._histogram[radix];
      }
    }

    for (uint8_t radix = 0; radix < _partition_count; ++radix) {
      partitions[radix].resize(sort_table->_histogram[radix]);
    }

    // Each chunk fills (parallel) partition
    for (auto& s_chunk : sort_table->_partition) {
      for (auto& entry : s_chunk._values) {
        auto radix = get_radix<T>(entry.first, _partition_count - 1);
        partitions[radix].at(s_chunk._prefix[radix]++) = entry;
      }
    }

    // move result to table
    sort_table->_partition.clear();
    sort_table->_partition.resize(partitions.size());
    for (size_t index = 0; index < partitions.size(); ++index) {
      sort_table->_partition[index]._values = partitions[index];
    }
  }
  // Sort partitions (right now std:sort -> but maybe can be replaced with
  // an algorithm more efficient, if subparts are already sorted [InsertionSort?])
  for (auto& partition : sort_table->_partition) {
    std::sort(partition._values.begin(), partition._values.end(),
              [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
  }
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::partition_join(uint32_t partition_number,
                                                         std::vector<PosList>& pos_lists_left,
                                                         std::vector<PosList>& pos_lists_right) {
  uint32_t left_index = 0;
  uint32_t right_index = 0;

  auto& left_current_partition = _sorted_left_table->_partition[partition_number];
  auto& right_current_partition = _sorted_right_table->_partition[partition_number];

  const size_t left_size = left_current_partition._values.size();
  const size_t right_size = right_current_partition._values.size();

  while (left_index < left_size && right_index < right_size) {
    T left_value = left_current_partition._values[left_index].first;
    T right_value = right_current_partition._values[right_index].first;
    uint32_t left_index_offset = 0;
    uint32_t right_index_offset = 0;

    // determine offset up to which all values are the same
    // left side
    for (; left_index_offset < left_size - left_index; ++left_index_offset) {
      if (left_index + left_index_offset + 1 == left_size ||
          left_value != left_current_partition._values[left_index + left_index_offset + 1].first) {
        break;
      }
    }
    // right side
    for (; right_index_offset < right_size - right_index; ++right_index_offset) {
      if (right_index_offset + right_index + 1 == right_size ||
          right_value != right_current_partition._values[right_index + right_index_offset + 1].first) {
        break;
      }
    }

    // search for matching values of both partitions
    if (left_value == right_value) {
      /* match found */
      // find all same values in each table then add product to _output
      uint32_t max_index_left = left_index + left_index_offset;
      uint32_t max_index_right = right_index_offset + right_index;
      RowID left_row_id;
      RowID right_row_id;

      for (uint32_t l_index = left_index; l_index <= max_index_left; ++l_index) {
        left_row_id = left_current_partition._values[l_index].second;

        for (uint32_t r_index = right_index; r_index <= max_index_right; ++r_index) {
          right_row_id = right_current_partition._values[r_index].second;
          // _sort_merge_join._pos_list_left->push_back(left_row_id);
          // _sort_merge_join._pos_list_right->push_back(right_row_id);
          pos_lists_left[partition_number].push_back(left_row_id);
          pos_lists_right[partition_number].push_back(right_row_id);
        }
      }

      // afterwards set index for both tables to next new value
      left_index += left_index_offset + 1u;
      right_index += right_index_offset + 1u;
    } else {
      if (left_value < right_value) {
        left_index += left_index_offset + 1u;
      } else {
        right_index += right_index_offset + 1u;
      }
    }
  }
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::perform_join() {
  _sort_merge_join._pos_list_left = std::make_shared<PosList>();
  _sort_merge_join._pos_list_right = std::make_shared<PosList>();
  // For now only equi-join is implemented
  // That means we only have to join partitions who are the same from both sides
  std::vector<PosList> pos_lists_left(_sorted_left_table->_partition.size());
  std::vector<PosList> pos_lists_right(_sorted_left_table->_partition.size());

  std::vector<std::thread> threads;

  // parallel join for each partition (Equi-Join Case)
  for (uint32_t partition_number = 0; partition_number < _sorted_left_table->_partition.size(); ++partition_number) {
    threads.push_back(std::thread(&SortMergeJoin::SortMergeJoinImpl<T>::partition_join, *this, partition_number,
                                  std::ref(pos_lists_left), std::ref(pos_lists_right)));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // Merge pos_lists_left of partitions together
  for (auto& p_list : pos_lists_left) {
    _sort_merge_join._pos_list_left->reserve(_sort_merge_join._pos_list_left->size() + p_list.size());
    _sort_merge_join._pos_list_left->insert(_sort_merge_join._pos_list_left->end(), p_list.begin(), p_list.end());
  }

  // Merge pos_lists_right of partitions together
  for (auto& p_list : pos_lists_right) {
    _sort_merge_join._pos_list_right->reserve(_sort_merge_join._pos_list_right->size() + p_list.size());
    _sort_merge_join._pos_list_right->insert(_sort_merge_join._pos_list_right->end(), p_list.begin(), p_list.end());
  }
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::build_output() {
  _sort_merge_join._output = std::make_shared<Table>(0, false);
  // left output
  for (size_t column_id = 0; column_id < _sort_merge_join._input_left->col_count(); column_id++) {
    // Add the column meta data
    _sort_merge_join._output->add_column(_sort_merge_join._input_left->column_name(column_id),
                                         _sort_merge_join._input_left->column_type(column_id), false);

    // Check whether the column consists of reference columns
    const auto r_column =
        std::dynamic_pointer_cast<ReferenceColumn>(_sort_merge_join._input_left->get_chunk(0).get_column(column_id));
    if (r_column) {
      // Create a pos_list referencing the original column
      auto new_pos_list =
          dereference_pos_list(_sort_merge_join._input_left, column_id, _sort_merge_join._pos_list_left);
      auto ref_column = std::make_shared<ReferenceColumn>(r_column->referenced_table(),
                                                          r_column->referenced_column_id(), new_pos_list);
      _sort_merge_join._output->get_chunk(0).add_column(ref_column);
    } else {
      auto ref_column =
          std::make_shared<ReferenceColumn>(_sort_merge_join._input_left, column_id, _sort_merge_join._pos_list_left);
      _sort_merge_join._output->get_chunk(0).add_column(ref_column);
    }
  }
  // right_output
  for (size_t column_id = 0; column_id < _sort_merge_join._input_right->col_count(); column_id++) {
    // Add the column meta data
    _sort_merge_join._output->add_column(_sort_merge_join._input_right->column_name(column_id),
                                         _sort_merge_join._input_right->column_type(column_id), false);

    // Check whether the column consists of reference columns
    const auto r_column =
        std::dynamic_pointer_cast<ReferenceColumn>(_sort_merge_join._input_right->get_chunk(0).get_column(column_id));
    if (r_column) {
      // Create a pos_list referencing the original column
      auto new_pos_list =
          dereference_pos_list(_sort_merge_join._input_right, column_id, _sort_merge_join._pos_list_right);
      auto ref_column = std::make_shared<ReferenceColumn>(r_column->referenced_table(),
                                                          r_column->referenced_column_id(), new_pos_list);
      _sort_merge_join._output->get_chunk(0).add_column(ref_column);
    } else {
      auto ref_column =
          std::make_shared<ReferenceColumn>(_sort_merge_join._input_right, column_id, _sort_merge_join._pos_list_right);
      _sort_merge_join._output->get_chunk(0).add_column(ref_column);
    }
  }
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::execute() {
  // sort left table
  _sorted_left_table = std::make_shared<SortedTable>();
  sort_table(_sorted_left_table, _sort_merge_join._input_left, _sort_merge_join._left_column_name, true);
  // sort right table
  _sorted_right_table = std::make_shared<SortedTable>();
  sort_table(_sorted_right_table, _sort_merge_join._input_right, _sort_merge_join._right_column_name, false);

  perform_join();
  build_output();
}

template <typename T>
std::shared_ptr<Table> SortMergeJoin::SortMergeJoinImpl<T>::get_output() const {
  return _sort_merge_join._output;
}

template <typename T>
std::shared_ptr<PosList> SortMergeJoin::SortMergeJoinImpl<T>::dereference_pos_list(
    std::shared_ptr<const Table> input_table, size_t column_id, std::shared_ptr<const PosList> pos_list) {
  // Get all the input pos lists so that we only have to pointer cast the columns once
  auto input_pos_lists = std::vector<std::shared_ptr<const PosList>>();
  for (ChunkID chunk_id = 0; chunk_id < input_table->chunk_count(); chunk_id++) {
    auto b_column = input_table->get_chunk(chunk_id).get_column(column_id);
    auto r_column = std::dynamic_pointer_cast<ReferenceColumn>(b_column);
    input_pos_lists.push_back(r_column->pos_list());
  }

  // Get the row ids that are referenced
  auto new_pos_list = std::make_shared<PosList>();
  for (const auto& row : *pos_list) {
    new_pos_list->push_back(input_pos_lists.at(row.chunk_id)->at(row.chunk_offset));
  }

  return new_pos_list;
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::handle_value_column(BaseColumn& column,
                                                              std::shared_ptr<ColumnVisitableContext> context) {
  auto& value_column = dynamic_cast<ValueColumn<T>&>(column);
  auto sort_context = std::static_pointer_cast<SortContext>(context);
  auto& sorted_table = sort_context->_write_to_sorted_left_table ? _sorted_left_table : _sorted_right_table;
  // SortedChunk chunk;

  for (ChunkOffset chunk_offset = 0; chunk_offset < value_column.values().size(); chunk_offset++) {
    RowID row_id{sort_context->_chunk_id, chunk_offset};
    sorted_table->_partition[sort_context->_chunk_id]._values[chunk_offset] =
        std::pair<T, RowID>(value_column.values()[chunk_offset], row_id);
  }

  std::sort(sorted_table->_partition[sort_context->_chunk_id]._values.begin(),
            sorted_table->_partition[sort_context->_chunk_id]._values.end(),
            [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
  // sorted_table->_partition[sort_context->_chunk_id] = std::move(chunk);
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::handle_dictionary_column(BaseColumn& column,
                                                                   std::shared_ptr<ColumnVisitableContext> context) {
  auto& dictionary_column = dynamic_cast<DictionaryColumn<T>&>(column);
  auto sort_context = std::static_pointer_cast<SortContext>(context);
  auto& sorted_table = sort_context->_write_to_sorted_left_table ? _sorted_left_table : _sorted_right_table;
  SortedChunk chunk;

  auto value_ids = dictionary_column.attribute_vector();
  auto dict = dictionary_column.dictionary();

  std::vector<std::vector<RowID>> value_count = std::vector<std::vector<RowID>>(dict->size());

  // Collect the rows for each value id
  for (ChunkOffset chunk_offset = 0; chunk_offset < value_ids->size(); chunk_offset++) {
    value_count[value_ids->get(chunk_offset)].push_back(RowID{sort_context->_chunk_id, chunk_offset});
  }

  // Append the rows to the sorted chunk
  for (ValueID value_id = 0; value_id < dict->size(); value_id++) {
    for (auto& row_id : value_count[value_id]) {
      chunk._values.push_back(std::pair<T, RowID>(dict->at(value_id), row_id));
    }
  }

  // Chunk is already sorted now because the dictionary is sorted
  sorted_table->_partition[sort_context->_chunk_id] = std::move(chunk);
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::handle_reference_column(ReferenceColumn& ref_column,
                                                                  std::shared_ptr<ColumnVisitableContext> context) {
  auto referenced_table = ref_column.referenced_table();
  auto referenced_column_id = ref_column.referenced_column_id();
  auto sort_context = std::static_pointer_cast<SortContext>(context);
  auto sorted_table = sort_context->_write_to_sorted_left_table ? _sorted_left_table : _sorted_right_table;
  auto pos_list = ref_column.pos_list();
  SortedChunk chunk;

  // Retrieve the columns from the referenced table so they only have to be casted once
  auto v_columns = std::vector<std::shared_ptr<ValueColumn<T>>>(referenced_table->chunk_count());
  auto d_columns = std::vector<std::shared_ptr<DictionaryColumn<T>>>(referenced_table->chunk_count());
  std::cout << referenced_table->chunk_count() << std::endl;
  for (ChunkID chunk_id = 0; chunk_id < referenced_table->chunk_count(); chunk_id++) {
    v_columns[chunk_id] = std::dynamic_pointer_cast<ValueColumn<T>>(
        referenced_table->get_chunk(chunk_id).get_column(referenced_column_id));
    d_columns[chunk_id] = std::dynamic_pointer_cast<DictionaryColumn<T>>(
        referenced_table->get_chunk(chunk_id).get_column(referenced_column_id));
  }

  // Retrieve the values from the referenced columns
  for (ChunkOffset chunk_offset = 0; chunk_offset < pos_list->size(); chunk_offset++) {
    const auto& row_id = pos_list->at(chunk_offset);

    // Dereference the value
    T value;
    if (v_columns[row_id.chunk_id]) {
      value = v_columns[row_id.chunk_id]->values()[row_id.chunk_offset];
    } else if (d_columns[row_id.chunk_id]) {
      ValueID value_id = d_columns[row_id.chunk_id]->attribute_vector()->get(row_id.chunk_offset);
      value = d_columns[row_id.chunk_id]->dictionary()->at(value_id);
    } else {
      throw std::runtime_error(
          "SortMergeJoinImpl::handle_reference_column: referenced column is neither value nor dictionary column!");
    }

    chunk._values.push_back(std::pair<T, RowID>(value, RowID{sort_context->_chunk_id, chunk_offset}));
  }

  // Sort the values and append the chunk to the sorted table
  std::sort(chunk._values.begin(), chunk._values.end(),
            [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
  sorted_table->_partition[sort_context->_chunk_id] = std::move(chunk);
}

}  // namespace opossum
