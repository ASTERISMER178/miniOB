#pragma once

#include "sql/operator/operator.h"
#include "sql/stmt/update_stmt.h"
#include "rc.h"

class UpdateStmt;

class UpdateOperator : public Operator
{
public:
  UpdateOperator(UpdateStmt *update_stmt)
    : update_stmt_(update_stmt) {}

    virtual ~UpdateOperator() = default;

    RC open() override;
    RC next() override;
    RC close() override;

    Tuple * current_tuple() override;


private:
  UpdateStmt *update_stmt_ = nullptr;
};
