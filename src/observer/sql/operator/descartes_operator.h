#pragma once

#include "sql/operator/operator.h"
#include "rc.h"

class FilterStmt;
class FilterUnit;

class DescartesOperator : public Operator {
public:
  DescartesOperator(FilterStmt *filter_stmt)
    : filter_stmt_(filter_stmt) {}

  virtual ~DescartesOperator() = default;

  RC open() override;
  RC next() override;
  RC close() override;
  std::vector<CompositeTuple> &com_tuple_sets() {
    return com_tuple_sets_;
  }

  Tuple * current_tuple() override { return nullptr; }
private:
  bool do_predicate(CompositeTuple &tuple, std::vector<const FilterUnit *> &filter_units);
  void descartes(std::vector<std::vector<Tuple *>> &tuple_sets, int pos, CompositeTuple &com_tuple, std::vector<const FilterUnit *> &filter_units);

private:
  FilterStmt *filter_stmt_ = nullptr;
  std::vector<CompositeTuple> com_tuple_sets_;
};
