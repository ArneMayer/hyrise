#include "sort_merge_join.hpp"

#include <memory>
#include <string>
#include <utility>

namespace opossum {
// TODO(Fabian): Comment everything!!

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
      std::string message = "SortMergeJoin::execute: column type \"" + left_column_type + "\" of left column \"" +
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
      std::string message = "NestedLoopJoin::NestedLoopJoin: No columns specified for join operator";
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
SortMergeJoin::SortMergeJoinImpl<T>::SortMergeJoinImpl(SortMergeJoin& nested_loop_join)
    : _nested_loop_join{nested_loop_join} {
  if (_nested_loop_join._op == "=") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left == value_right; };
  } else {
    std::string message = "SortMergeJoinImpl::SortMergeJoinImpl: Unknown operator " + _nested_loop_join._op;
    std::cout << message << std::endl;
    throw std::exception(std::runtime_error(message));
  }
  /* right now only equi-joins supported
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
typename SortMergeJoin::SortMergeJoinImpl<T>::SortedTable SortMergeJoin::SortMergeJoinImpl<T>::sort_left_table() {
  auto sorted_left_table = SortMergeJoin::SortMergeJoinImpl<T>::SortedTable();
  return sorted_left_table;
}

template <typename T>
typename SortMergeJoin::SortMergeJoinImpl<T>::SortedTable SortMergeJoin::SortMergeJoinImpl<T>::sort_right_table() {
  auto sorted_right_table = SortMergeJoin::SortMergeJoinImpl<T>::SortedTable();
  return sorted_right_table;
}

template <typename T>
void perform_join(typename SortMergeJoin::SortMergeJoinImpl<T>::SortedTable& left,
                  typename SortMergeJoin::SortMergeJoinImpl<T>::SortedTable& right) {
  // do join
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::execute() {
  auto sorted_left_table = sort_left_table();
  auto sorted_right_table = sort_right_table();
  perform_join(sorted_left_table, sorted_right_table);
}

/*
template <typename T>
std::shared_ptr<Table> SortMergeJoin::SortMergeJoinImpl<T>::get_output() const {
  std::string message = "SortMergeJoinImpl::get_output() not implemented";
  std::cout << message << std::endl;
  throw std::exception(std::runtime_error(message));
  return nullptr;
}*/

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::join_value_value(ValueColumn<T>& left, ValueColumn<T>& right,
                                                           std::shared_ptr<JoinContext> context, bool reverse_order) {}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::join_value_dictionary(ValueColumn<T>& left, DictionaryColumn<T>& right,
                                                                std::shared_ptr<JoinContext> context,
                                                                bool reverse_order) {}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::join_value_reference(ValueColumn<T>& left, ReferenceColumn& right,
                                                               std::shared_ptr<JoinContext> context,
                                                               bool reverse_order) {}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::join_dictionary_dictionary(DictionaryColumn<T>& left,
                                                                     DictionaryColumn<T>& right,
                                                                     std::shared_ptr<JoinContext> context,
                                                                     bool reverse_order) {}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::join_dictionary_reference(DictionaryColumn<T>& left, ReferenceColumn& right,
                                                                    std::shared_ptr<JoinContext> context,
                                                                    bool reverse_order) {}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::join_reference_reference(ReferenceColumn& left, ReferenceColumn& right,
                                                                   std::shared_ptr<JoinContext> context,
                                                                   bool reverse_order) {}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::handle_value_column(BaseColumn& column,
                                                              std::shared_ptr<ColumnVisitableContext> context) {
  /*
auto join_context = std::static_pointer_cast<JoinContext>(context);
auto& value_column_left = dynamic_cast<ValueColumn<T>&>(column);
auto value_column_right = std::dynamic_pointer_cast<ValueColumn<T>>(join_context->_column_right);
if (value_column_right) {
join_value_value(value_column_left, *value_column_right, join_context);
return;
}
auto dictionary_column_right = std::dynamic_pointer_cast<DictionaryColumn<T>>(join_context->_column_right);
if (dictionary_column_right) {
join_value_dictionary(value_column_left, *dictionary_column_right, join_context);
return;
}
auto reference_column_right = std::dynamic_pointer_cast<ReferenceColumn>(join_context->_column_right);
if (reference_column_right) {
join_value_reference(value_column_left, *reference_column_right, join_context);
return;
}
*/
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::handle_dictionary_column(BaseColumn& column,
                                                                   std::shared_ptr<ColumnVisitableContext> context) {
  /*
auto join_context = std::static_pointer_cast<JoinContext>(context);
auto& dictionary_column_left = dynamic_cast<DictionaryColumn<T>&>(column);

auto value_column_right = std::dynamic_pointer_cast<ValueColumn<T>>(join_context->_column_right);
if (value_column_right) {
join_value_dictionary(*value_column_right, dictionary_column_left, join_context, true);
return;
}
auto dictionary_column_right = std::dynamic_pointer_cast<DictionaryColumn<T>>(join_context->_column_right);
if (dictionary_column_right) {
join_dictionary_dictionary(dictionary_column_left, *dictionary_column_right, join_context);
return;
}
auto reference_column_right = std::dynamic_pointer_cast<ReferenceColumn>(join_context->_column_right);
if (reference_column_right) {
join_dictionary_reference(dictionary_column_left, *reference_column_right, join_context);
return;
}
*/
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::handle_reference_column(ReferenceColumn& reference_column_left,
                                                                  std::shared_ptr<ColumnVisitableContext> context) {
  /*
auto join_context = std::static_pointer_cast<JoinContext>(context);

auto value_column_right = std::dynamic_pointer_cast<ValueColumn<T>>(join_context->_column_right);
if (value_column_right) {
join_value_reference(*value_column_right, reference_column_left, join_context, true);
return;
}
auto dictionary_column_right = std::dynamic_pointer_cast<DictionaryColumn<T>>(join_context->_column_right);
if (dictionary_column_right) {
join_dictionary_reference(*dictionary_column_right, reference_column_left, join_context, true);
return;
}
auto reference_column_right = std::dynamic_pointer_cast<ReferenceColumn>(join_context->_column_right);
if (reference_column_right) {
join_reference_reference(reference_column_left, *reference_column_right, join_context);
return;
} */
}

}  // namespace opossum