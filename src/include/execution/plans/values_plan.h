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
 * The ValuesPlanNode represents rows of values. For example,
 * `INSERT INTO table VALUES ((0, 1), (1, 2))`, where we will have
 * `(0, 1)` and `(1, 2)` as the output of this executor.
 */
class ValuesPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new ValuesPlanNode instance.
   * @param output The output schema of this values plan node
   * @param child The child plan node
   */
  ValuesPlanNode(const Schema *output, const AbstractPlanNode *child) : AbstractPlanNode(output, {child}) {}

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::Values; }

 protected:
  auto PlanNodeToString() const -> std::string override { return fmt::format("Values {{ }}"); }

 private:
};

}  // namespace bustub
