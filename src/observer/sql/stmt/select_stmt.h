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
// Created by Wangyunlai on 2022/6/5.
//

#pragma once

#include <vector>

#include "rc.h"
#include "sql/stmt/stmt.h"
#include "storage/common/field.h"
#include <iostream>

class FieldMeta;
class FilterStmt;
class Db;
class Table;
class Aggregate;

class SelectStmt : public Stmt
{
public:

  SelectStmt() = default;
  ~SelectStmt() override;

  StmtType type() const override { return StmtType::SELECT; }
public:
  static RC create(Db *db, const Selects &select_sql, Stmt *&stmt);

public:
  const std::vector<Table *> &tables() const { return tables_; }
  const std::vector<Field> &query_fields() const { return query_fields_; }
  FilterStmt *filter_stmt() const { return filter_stmt_; }
  std::vector<Aggregate*> &aggregates() { return aggregates_; }
private:
  std::vector<Field> query_fields_;
  std::vector<Table *> tables_;
  FilterStmt *filter_stmt_ = nullptr;
  std::vector<Aggregate*> aggregates_;
};

class Aggregate
{
public:
  Aggregate(AggrType aggr_type, const char *attr_name, Field field) 
    : aggr_type_(aggr_type), attr_name_(attr_name), field_(field) {}
  virtual void do_record(const Record &rec) = 0;
  virtual std::string to_string() = 0;
  virtual void value_to_string(std::ostream &os) const = 0;

protected:
  AggrType aggr_type_;
  const char *attr_name_ = nullptr;
  Field field_;
};

class CountAggregate : public Aggregate
{
public:
  CountAggregate(AggrType aggr_type, const char *attr_name, Field field)
    : Aggregate(aggr_type, attr_name, field), count_(0) {}
  void do_record(const Record &rec) override;
  std::string to_string() override { return "COUNT(" + std::string(attr_name_) + ')'; }
  void value_to_string(std::ostream &os) const override { os << count_; }
private:
  int count_;
};

class MaxAggregate : public Aggregate
{
public:
  MaxAggregate(AggrType aggr_type, const char *attr_name, Field field)
    : Aggregate(aggr_type, attr_name, field), value_data_(nullptr) {}
  void do_record(const Record &rec) override;
  std::string to_string() override { return "MAX(" + std::string(attr_name_) + ')'; }
  void value_to_string(std::ostream &os) const override { 
    auto attr_type = field_.attr_type();
    switch (attr_type)
    {
    case DATES:case INTS:
      os << (*(int*)(value_data_ + field_.meta()->offset()));
      break;
    case FLOATS:
      os << (*(float*)(value_data_ + field_.meta()->offset()));
      break;
    case CHARS:
      {
        auto data = (value_data_ + field_.meta()->offset());
        for (int i = 0; i < field_.meta()->len(); i++) {
          if (data[i] == '\0') {
            break;
          }
          os << data[i];
        }
        break;
      }
    default:
      break;
    }
  }
private:
  const char *value_data_;
};

class MinAggregate : public Aggregate
{
public:
  MinAggregate(AggrType aggr_type, const char *attr_name, Field field)
    : Aggregate(aggr_type, attr_name, field), value_data_(nullptr) {}
  void do_record(const Record &rec) override;
  std::string to_string() override { return "MIN(" + std::string(attr_name_) + ')'; }
  void value_to_string(std::ostream &os) const override { 
    auto attr_type = field_.attr_type();
    switch (attr_type)
    {
    case DATES:case INTS:
      os << *(int*)(value_data_ + field_.meta()->offset());
      break;
    case FLOATS:
      os << *(float*)(value_data_ + field_.meta()->offset());
      break;
    case CHARS:
      {
        auto data = (value_data_ + field_.meta()->offset());
        for (int i = 0; i < field_.meta()->len(); i++) {
          if (data[i] == '\0') {
            break;
          }
          os << data[i];
        }
        break;
      }
    default:
      break;
    }
  }
private:
  const char *value_data_;
};

class AvgAggregate : public Aggregate
{
public:
  AvgAggregate(AggrType aggr_type, const char *attr_name, Field field)
    : Aggregate(aggr_type, attr_name, field), sum_(0.0), count_(0) {}
  void do_record(const Record &rec) override;
  std::string to_string() override { return "AVG(" + std::string(attr_name_) + ')'; }
  void value_to_string(std::ostream &os) const override { os << (sum_ / (float)count_); }
private:
  float sum_;
  int count_;
};