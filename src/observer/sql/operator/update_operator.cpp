#include "storage/common/record.h"
#include "storage/common/table.h"
#include "sql/operator/update_operator.h"
#include "rc.h"

RC UpdateOperator::open()
{
  if (children_.size() != 1) {
    LOG_WARN("udpate operator must has 1 child");
    return RC::INTERNAL;
  }

  Operator *child = children_[0];
  RC rc = child->open();
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to open child operator: %s", strrc(rc));
    return rc;
  }

  Table *table = update_stmt_->table();
  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("Failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record &record = row_tuple->record();
    char *data = record.data();
    switch (update_stmt_->value().type) {
      case INTS:
      case DATES: {
        *(int *)(data + update_stmt_->field_meta()->offset()) 
          = *(int *)update_stmt_->value().data;
        break;
      }
      case FLOATS:{
        *(float *)(data + update_stmt_->field_meta()->offset())
          = *(float *)update_stmt_->value().data;
      }
      case CHARS: {
        int len = strlen((char *)update_stmt_->value().data);
        memcpy((char *)(data + update_stmt_->field_meta()->offset()), (char *)(update_stmt_->value().data), len+1);
        // for(int i = 0; i < len; ++i) {
        //   *(char *)(data + update_stmt_->field_meta()->offset() + i) = *(char *)(update_stmt_->value().data + i);
        // }
      }
      record.set_data(data);
    }
    rc = table->update_record(nullptr, &record);
  }

  return RC::SUCCESS;
}

RC UpdateOperator::close()
{
  children_[0]->close();
  return RC::SUCCESS;
}

RC UpdateOperator::next() {
  return RC::RECORD_EOF;
}

Tuple *UpdateOperator::current_tuple() {
  return nullptr;
}