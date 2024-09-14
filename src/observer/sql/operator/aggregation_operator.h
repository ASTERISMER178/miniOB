#pragma once

#include "sql/operator/operator.h"

class SelectStmt;

class AggregationOperator : public Operator {
public:
  AggregationOperator(SelectStmt *select_stmt) : select_stmt_(select_stmt)
  {}
  virtual ~AggregationOperator() = default;

  RC open() override;
  RC next() override;
  RC close() override;
  Tuple * current_tuple() override { return nullptr; }

private:
  SelectStmt *select_stmt_ = nullptr;
};