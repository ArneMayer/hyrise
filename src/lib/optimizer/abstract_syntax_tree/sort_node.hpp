#pragma once

#include <string>
#include <vector>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Struct to specify Order By items.
 * Order By items are defined by the column_name they operate on and their sort order.
 */
struct OrderByDefinition {
  OrderByDefinition(const ColumnID column_id, const OrderByMode order_by_mode);

  ColumnID column_id;
  OrderByMode order_by_mode;
};

/**
 * This node type represents sorting operations as defined in ORDER BY clauses.
 */
class SortNode : public AbstractASTNode {
 public:
  explicit SortNode(const std::vector<OrderByDefinition>& order_by_definitions);

  std::string description() const override;

  const std::vector<OrderByDefinition>& order_by_definitions() const;

  void reorder_columns(const ColumnIDMapping &column_id_mapping,
                                              const std::optional<ASTChildSide> &caller_child_side = std::nullopt) override;

 private:
  std::vector<OrderByDefinition> _order_by_definitions;
};

}  // namespace opossum
