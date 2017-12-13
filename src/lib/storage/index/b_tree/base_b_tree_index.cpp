#include "base_b_tree_index.hpp"

#include "storage/index/column_index_type.hpp"

namespace opossum {

BaseBTreeIndex::BaseBTreeIndex(std::shared_ptr<const Table> table, const ColumnID column_id)
    : _table{table}, _column_id(column_id) { }

} // namespace opossum