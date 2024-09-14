/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "storage/common/db.h"
#include "storage/common/table.h"
#include "storage/common/field.h"
#include "storage/common/record.h"
#include "util/comparator.h"

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

static void wildcard_fields(Table *table, std::vector<Field> &field_metas)
{
  const TableMeta &table_meta = table->table_meta();
  const int field_num = table_meta.field_num();
  for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    field_metas.push_back(Field(table, table_meta.field(i)));
  }
}

RC SelectStmt::create(Db *db, const Selects &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  // collect tables in `from` statement
  std::vector<Table *> tables;
  std::unordered_map<std::string, Table *> table_map;
  for (int i = select_sql.relation_num - 1; i >= 0; --i) {
    const char *table_name = select_sql.relations[i];
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    tables.push_back(table);
    table_map.insert(std::pair<std::string, Table *>(table_name, table));
  }

  // collect query fields in `select` statement
  std::vector<Field> query_fields;
  for (int i = select_sql.attr_num - 1; i >= 0; i--) {
    const RelAttr &relation_attr = select_sql.attributes[i];

    if (common::is_blank(relation_attr.relation_name) && 0 == strcmp(relation_attr.attribute_name, "*")) {
      for (Table *table : tables) {
        wildcard_fields(table, query_fields);
      }

    } else if (!common::is_blank(relation_attr.relation_name)) {  // TODO
      const char *table_name = relation_attr.relation_name;
      const char *field_name = relation_attr.attribute_name;

      if (0 == strcmp(table_name, "*")) {
        if (0 != strcmp(field_name, "*")) {
          LOG_WARN("invalid field name while table is *. attr=%s", field_name);
          return RC::SCHEMA_FIELD_MISSING;
        }
        for (Table *table : tables) {
          wildcard_fields(table, query_fields);
        }
      } else {
        auto iter = table_map.find(table_name);
        if (iter == table_map.end()) {
          LOG_WARN("no such table in from list: %s", table_name);
          return RC::SCHEMA_FIELD_MISSING;
        }

        Table *table = iter->second;
        if (0 == strcmp(field_name, "*")) {
          wildcard_fields(table, query_fields);
        } else {
          const FieldMeta *field_meta = table->table_meta().field(field_name);
          if (nullptr == field_meta) {
            LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), field_name);
            return RC::SCHEMA_FIELD_MISSING;
          }

          query_fields.push_back(Field(table, field_meta));
        }
      }
    } else {
      if (tables.size() != 1) {
        LOG_WARN("invalid. I do not know the attr's table. attr=%s", relation_attr.attribute_name);
        return RC::SCHEMA_FIELD_MISSING;
      }

      Table *table = tables[0];
      const FieldMeta *field_meta = table->table_meta().field(relation_attr.attribute_name);
      if (nullptr == field_meta) {
        LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), relation_attr.attribute_name);
        return RC::SCHEMA_FIELD_MISSING;
      }

      query_fields.push_back(Field(table, field_meta));
    }
  }

  LOG_INFO("got %d tables in from stmt and %d fields in query stmt", tables.size(), query_fields.size());

  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  RC rc =
      FilterStmt::create(db, default_table, &table_map, select_sql.conditions, select_sql.condition_num, filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // collect aggregation
  // single table
  // TODO mutil tables
  std::vector<Aggregate *> aggregates;
  for (int i = 0; i < select_sql.aggregation_num; ++i) {

    auto &aggr_type = select_sql.aggr_type[i];
    auto &aggregation = select_sql.aggregations[i];

    auto table = tables[0];
    if (aggr_type == COUNT && strcmp(aggregation, "*") == 0) {
      const FieldMeta *field_meta = table->table_meta().field(0);
      if (nullptr == field_meta) {
        LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), aggregation);
        return RC::SCHEMA_FIELD_MISSING;
      }
      aggregates.push_back(new CountAggregate(aggr_type, aggregation, Field(table, field_meta)));
      continue;
    }
    const FieldMeta *field_meta = table->table_meta().field(aggregation);
    if (nullptr == field_meta) {
      LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), aggregation);
      return RC::SCHEMA_FIELD_MISSING;
    }

    switch (aggr_type) {
      case MAX:
        aggregates.push_back(new MaxAggregate(aggr_type, aggregation, Field(table, field_meta)));
        break;
      case MIN:
        aggregates.push_back(new MinAggregate(aggr_type, aggregation, Field(table, field_meta)));
        break;
      case AVG:
        aggregates.push_back(new AvgAggregate(aggr_type, aggregation, Field(table, field_meta)));
        break;
      case COUNT:
        aggregates.push_back(new CountAggregate(aggr_type, aggregation, Field(table, field_meta)));
        break;
      default:
        LOG_WARN("Unimplement aggregate function");
        return RC::UNIMPLENMENT;
    }
  }
  // everything alright
  SelectStmt *select_stmt = new SelectStmt();
  select_stmt->tables_.swap(tables);
  select_stmt->query_fields_.swap(query_fields);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->aggregates_.swap(aggregates);
  stmt = select_stmt;
  return RC::SUCCESS;
}

void MaxAggregate::do_record(const Record &rec)
{
  if (value_data_ == nullptr) {
    value_data_ = rec.data();
    return;
  }
  auto field_meta = field_.meta();
  auto attr_type = field_.attr_type();
  const char *rec_data = rec.data() + field_meta->offset();
  const char *max_value = value_data_ + field_meta->offset();
  int res = 0;

  switch (attr_type) {
    case INTS:
    case DATES:
      res = compare_int(const_cast<char *>(max_value), const_cast<char *>(rec_data));
      break;
    case FLOATS:
      res = compare_float(const_cast<char *>(max_value), const_cast<char *>(rec_data));
      break;
    case CHARS:
      res = compare_string(
          const_cast<char *>(max_value), strlen(max_value), const_cast<char *>(rec_data), strlen(rec_data));
      break;
    default:
      break;
  }

  if (res <= 0) value_data_ = rec.data();
}

void MinAggregate::do_record(const Record &rec)
{
  if (value_data_ == nullptr) {
    value_data_ = rec.data();
    return;
  }
  auto field_meta = field_.meta();
  auto attr_type = field_.attr_type();
  const char *rec_data = rec.data() + field_meta->offset();
  const char *max_value = value_data_ + field_meta->offset();
  int res = 0;

  switch (attr_type) {
    case INTS:
    case DATES:
      res = compare_int(const_cast<char *>(max_value), const_cast<char *>(rec_data));
      break;
    case FLOATS:
      res = compare_float(const_cast<char *>(max_value), const_cast<char *>(rec_data));
      break;
    case CHARS:
      res = compare_string(
          const_cast<char *>(max_value), strlen(max_value), const_cast<char *>(rec_data), strlen(rec_data));
      break;
    default:
      break;
  }
  if (res >= 0) value_data_ = rec.data();
}

void CountAggregate::do_record(const Record &rec)
{
  count_++;
}

void AvgAggregate::do_record(const Record &rec)
{
  auto field_meta = field_.meta();
  auto attr_type = field_.attr_type();
  switch (attr_type)
  {
  case INTS:case DATES:
    sum_ += (float)(*(int*)(rec.data() + field_meta->offset()));
    break;
  case FLOATS:
    sum_ += *(float*)(rec.data() + field_meta->offset());
  default:
    break;
  }
  count_++;
}