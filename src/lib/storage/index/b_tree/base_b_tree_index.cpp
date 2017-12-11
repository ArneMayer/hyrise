#include "base_b_tree_index.hpp"

#include "storage/index/column_index_type.hpp"

namespace opossum {

BaseBTreeIndex::BaseBTreeIndex(const std::vector<std::shared_ptr<const BaseColumn>>& index_columns)
    : BaseIndex{get_index_type_of<BaseBTreeIndex>()}, _index_column(index_columns.front()) { }

} // namespace opossum