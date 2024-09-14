#pragma once

#include "sql/operator/descartes_operator.h"
#include "sql/operator/table_scan_operator.h"
#include "sql/operator/predicate_operator.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/common/record.h"
#include "common/lang/defer.h"
#include "rc.h"

RC DescartesOperator::open()
{
  if (children_.size() < 1) {
    LOG_WARN("Decartes operator must have least 1 child.");
    return RC::INTERNAL;
  }

  // collects all tuple filted by inner table condition
  std::vector<std::vector<Tuple *>> tuple_sets;
  for (auto child : children_) {
    std::vector<Tuple *> tuple_set;
    RC rc = child->open();
    if (rc != RC::SUCCESS) {
      LOG_WARN("Failed to open child operator: %s", strrc(rc));
      return rc;
    }

    while (RC::SUCCESS == (rc = child->next())) {
      Tuple *current_tuple = child->current_tuple();
      RowTuple *tuple = new RowTuple();
      tuple->set_schema(((RowTuple *)current_tuple)->table(), 
          ((RowTuple *)current_tuple)->table()->table_meta().field_metas());

      //TODO: may cause memery leak.
      auto &current_record = ((PredicateOperator*)child)->current_record();
      Record *record = new Record(current_record);
      tuple->set_record(record);
      if (nullptr == tuple) {
        LOG_WARN("Faild to get current record");
        return RC::INTERNAL;
      }

      tuple_set.emplace_back(tuple);
    }
    tuple_sets.emplace_back(tuple_set);
  }

  CompositeTuple com_tuple;
  std::vector<const FilterUnit *> filter_units;
  if (filter_stmt_ != nullptr && !filter_stmt_->filter_units().empty()) {
    for (const FilterUnit *filter_unit : filter_stmt_->filter_units()) {
      Expression *left_expr = filter_unit->left();
      Expression *right_expr = filter_unit->right();
      if(left_expr->type() == ExprType::FIELD && right_expr->type() == ExprType::FIELD) {
        if(strcmp(((FieldExpr *)left_expr)->table_name(), ((FieldExpr *)right_expr)->table_name()) != 0) {
          filter_units.push_back(filter_unit);
        }
      }
    }
  }
  descartes(tuple_sets, 0, com_tuple, filter_units);
  return RC::SUCCESS;
}

void DescartesOperator::descartes(std::vector<std::vector<Tuple *>> &tuple_sets, int pos, CompositeTuple &com_tuple, std::vector<const FilterUnit *> &filter_units)
{
  auto &tuple_set = tuple_sets[pos];
  for (auto tuple : tuple_set) {
    com_tuple.add_tuple(tuple);
    if (pos == tuple_sets.size() - 1) {
      if(do_predicate(com_tuple, filter_units)) {
        com_tuple_sets_.push_back(com_tuple);
      }
      com_tuple.remove_tuple();
      continue;
    }

    descartes(tuple_sets, pos+1, com_tuple, filter_units);
    com_tuple.remove_tuple();
  }

}

bool DescartesOperator::do_predicate(CompositeTuple &tuple, std::vector<const FilterUnit *> &filter_units) {
  if (filter_units.empty()) {
    return true;
  }
  for (const FilterUnit *filter_unit : filter_units) {
    Expression *left_expr = filter_unit->left();
    Expression *right_expr = filter_unit->right();
    CompOp comp = filter_unit->comp();
    TupleCell left_cell;
    TupleCell right_cell;
    RC rc = left_expr->get_value(tuple, left_cell);

    // 只过滤表间条件
    if (rc != RC::SUCCESS) {
      continue;
    }
    rc = right_expr->get_value(tuple, right_cell);
    if (rc != RC::SUCCESS) {
      continue;
    }

    const int compare = left_cell.compare(right_cell);
    bool filter_result = false;
    switch (comp) {
    case EQUAL_TO: {
      filter_result = (0 == compare); 
    } break;
    case LESS_EQUAL: {
      filter_result = (compare <= 0); 
    } break;
    case NOT_EQUAL: {
      filter_result = (compare != 0);
    } break;
    case LESS_THAN: {
      filter_result = (compare < 0);
    } break;
    case GREAT_EQUAL: {
      filter_result = (compare >= 0);
    } break;
    case GREAT_THAN: {
      filter_result = (compare > 0);
    } break;
    default: {
      LOG_WARN("invalid compare type: %d", comp);
    } break;
    }
    if (!filter_result) {
      return false;
    }
  }
  return true;
}
RC DescartesOperator::close()
{
  for (auto child : children_) {
    child->close();
    // DEFER([&] () {delete child;});
  }
  return RC::SUCCESS;
}

RC DescartesOperator::next() {
  return RC::RECORD_EOF;
}
