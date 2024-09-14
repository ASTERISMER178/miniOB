#pragma once

#include "sql/operator/aggregation_operator.h"
#include "sql/stmt/select_stmt.h"

RC AggregationOperator::open()
{
  if (children_.size() < 1) {
    LOG_WARN("Aggregation operator must have least 1 child.");
    return RC::INTERNAL;
  }

  auto &child = children_[0];
  RC rc = child->open();
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to open child operator: %s", strrc(rc));
    return rc;
  }

  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("Failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    for (auto &aggregate : select_stmt_->aggregates()) {
      aggregate->do_record(row_tuple->record());
    }
  }
  return RC::SUCCESS;
}

RC AggregationOperator::close()
{
  return children_[0]->close();
}

RC AggregationOperator::next()
{
  return RC::RECORD_EOF;
}