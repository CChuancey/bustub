//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// abstract_plan.h
//
// Identification: src/include/execution/plans/abstract_plan.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "fmt/format.h"

namespace bustub {

/** PlanType represents the types of plans that we have in our system. */
enum class PlanType {
  SeqScan,
  IndexScan,
  Insert,
  Update,
  Delete,
  Aggregation,
  Limit,
  Distinct,
  NestedLoopJoin,
  NestedIndexJoin,
  HashJoin,
  Filter,
  Values,
  MockScan
};

/**
 * AbstractPlanNode represents all the possible types of plan nodes in our system.
 * Plan nodes are modeled as trees, so each plan node can have a variable number of children.
 * Per the Volcano model, the plan node receives the tuples of its children.
 * The ordering of the children may matter.
 */
class AbstractPlanNode {
 public:
  /**
   * Create a new AbstractPlanNode with the specified output schema and children.
   * @param output_schema the schema for the output of this plan node
   * @param children the children of this plan node
   */
  AbstractPlanNode(const Schema *output_schema, std::vector<const AbstractPlanNode *> &&children)
      : output_schema_(output_schema), children_(std::move(children)) {}

  /** Virtual destructor. */
  virtual ~AbstractPlanNode() = default;

  /** @return the schema for the output of this plan node */
  auto OutputSchema() const -> const Schema * { return output_schema_; }

  /** @return the child of this plan node at index child_idx */
  auto GetChildAt(uint32_t child_idx) const -> const AbstractPlanNode * { return children_[child_idx]; }

  /** @return the children of this plan node */
  auto GetChildren() const -> const std::vector<const AbstractPlanNode *> & { return children_; }

  /** @return the type of this plan node */
  virtual auto GetType() const -> PlanType = 0;

  /** @return the string representation of the plan node and its children */
  auto ToString() const -> std::string {
    return fmt::format("{} | {}{}", PlanNodeToString(), output_schema_->ToString(), ChildrenToString(2));
  }

 protected:
  /** @return the string representation of the plan node itself */
  virtual auto PlanNodeToString() const -> std::string { return "<unknown>"; }

  /** @return the string representation of the plan node's children */
  auto ChildrenToString(int indent) const -> std::string;

 private:
  /**
   * The schema for the output of this plan node. In the volcano model, every plan node will spit out tuples,
   * and this tells you what schema this plan node's tuples will have.
   */
  const Schema *output_schema_;
  /** The children of this plan node. */
  std::vector<const AbstractPlanNode *> children_;
};
}  // namespace bustub

template <typename T>
struct fmt::formatter<T, std::enable_if_t<std::is_base_of<bustub::AbstractPlanNode, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const bustub::AbstractPlanNode &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x.ToString(), ctx);
  }
};

template <typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<bustub::AbstractPlanNode, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const std::unique_ptr<bustub::AbstractPlanNode> &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x->ToString(), ctx);
  }
};
