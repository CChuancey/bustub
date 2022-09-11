//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_plan.h
//
// Identification: src/include/execution/plans/seq_scan_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>

#include "catalog/catalog.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * The FilterPlanNode represents a filter operation.
 * It retains any tuple that satisfies the predicate in the child.
 */
class FilterPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new FilterPlanNode instance.
   * @param output The output schema of this filter plan node
   * @param predicate The predicate applied during the scan operation
   * @param child The child plan node
   */
  FilterPlanNode(const Schema *output, const AbstractExpression *predicate, const AbstractPlanNode *child)
      : AbstractPlanNode(output, {child}), predicate_{predicate} {}

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::Filter; }

  /** @return The predicate to test tuples against; tuples should only be returned if they evaluate to true */
  auto GetPredicate() const -> const AbstractExpression * { return predicate_; }

  /** @return The child plan node */
  auto GetChildPlan() const -> const AbstractPlanNode * {
    BUSTUB_ASSERT(GetChildren().size() == 1, "Filter should have exactly one child plan.");
    return GetChildAt(0);
  }

 protected:
  auto PlanNodeToString() const -> std::string override {
    return fmt::format("Filter {{ predicate={} }}", *predicate_);
  }

 private:
  /** The predicate that all returned tuples must satisfy */
  const AbstractExpression *predicate_;
};

}  // namespace bustub
