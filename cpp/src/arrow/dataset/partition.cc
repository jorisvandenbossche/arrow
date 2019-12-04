// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/dataset/partition.h"

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/scalar.h"
#include "arrow/util/iterator.h"

namespace arrow {
namespace dataset {

Result<ExpressionPtr> PartitionScheme::Parse(const std::string& path) const {
  ExpressionVector expressions;
  int i = 0;

  for (auto segment : fs::internal::SplitAbstractPath(path)) {
    ARROW_ASSIGN_OR_RAISE(auto expr, Parse(segment, i++));
    if (expr->Equals(true)) {
      continue;
    }

    expressions.push_back(std::move(expr));
  }

  return and_(std::move(expressions));
}

class DefaultPartitionScheme : public PartitionScheme {
 public:
  DefaultPartitionScheme() : PartitionScheme(::arrow::schema({})) {}

  std::string name() const override { return "default_partition_scheme"; }

  Result<ExpressionPtr> Parse(const std::string& segment, int i) const override {
    return scalar(true);
  }
};

PartitionSchemePtr PartitionScheme::Default() {
  return std::make_shared<DefaultPartitionScheme>();
}

PartitionSchemeDiscovery::PartitionSchemeDiscovery() : schema_(::arrow::schema({})) {}

Result<ExpressionPtr> SegmentDictionaryPartitionScheme::Parse(const std::string& segment,
                                                              int i) const {
  if (static_cast<size_t>(i) < dictionaries_.size()) {
    auto it = dictionaries_[i].find(segment);
    if (it != dictionaries_[i].end()) {
      return it->second;
    }
  }

  return scalar(true);
}

Result<ExpressionPtr> PartitionKeysScheme::ConvertKey(const Key& key,
                                                      const Schema& schema) {
  auto field = schema.GetFieldByName(key.name);
  if (field == nullptr) {
    return scalar(true);
  }

  std::shared_ptr<Scalar> converted;
  RETURN_NOT_OK(Scalar::Parse(field->type(), key.value, &converted));
  return equal(field_ref(field->name()), scalar(converted));
}

Result<ExpressionPtr> PartitionKeysScheme::Parse(const std::string& segment,
                                                 int i) const {
  if (auto key = ParseKey(segment, i)) {
    return ConvertKey(*key, *schema_);
  }

  return scalar(true);
}

util::optional<PartitionKeysScheme::Key> SchemaPartitionScheme::ParseKey(
    const std::string& segment, int i) const {
  if (i >= schema_->num_fields()) {
    return util::nullopt;
  }

  return Key{schema_->field(i)->name(), segment};
}

inline bool AllIntegral(const std::vector<std::string>& reprs) {
  return std::all_of(reprs.begin(), reprs.end(), [](util::string_view repr) {
    // TODO(bkietz) use ParseUnsigned or so
    return repr.find_first_not_of("0123456789") == util::string_view::npos;
  });
}

inline std::shared_ptr<Schema> InferSchema(
    const std::unordered_map<std::string, std::vector<std::string>>& name_to_values) {
  std::vector<std::shared_ptr<Field>> fields(name_to_values.size());

  size_t field_index = 0;
  for (const auto& name_values : name_to_values) {
    auto type = AllIntegral(name_values.second) ? int32() : utf8();
    fields[field_index++] = field(name_values.first, type);
  }
  return ::arrow::schema(std::move(fields));
}

class SchemaPartitionSchemeDiscovery : public PartitionSchemeDiscovery {
 public:
  SchemaPartitionSchemeDiscovery(std::string partition_base_dir,
                                 std::vector<std::string> field_names)
      : partition_base_dir_(std::move(partition_base_dir)),
        field_names_(std::move(field_names)) {}

  Result<std::shared_ptr<Schema>> Inspect(
      const fs::FileStatsVector& stats) const override {
    std::unordered_map<std::string, std::vector<std::string>> name_to_values;

    for (const auto& stats : stats) {
      if (!fs::internal::IsAncestorOf(partition_base_dir_, stats.path())) {
        continue;
      }

      auto segments = fs::internal::SplitAbstractPath(
          stats.path().substr(partition_base_dir_.size()));

      size_t field_index = 0;
      for (auto&& segment : segments) {
        if (field_index == field_names_.size()) break;

        name_to_values[field_names_[field_index++]].push_back(std::move(segment));
      }
    }

    return InferSchema(name_to_values);
  }

  Result<PartitionSchemePtr> Finish() const override {
    return PartitionSchemePtr(new SchemaPartitionScheme(schema_));
  }

 private:
  std::string partition_base_dir_;
  std::vector<std::string> field_names_;
};

PartitionSchemeDiscoveryPtr SchemaPartitionScheme::MakeDiscovery(
    std::string partition_base_dir, std::vector<std::string> field_names) {
  return PartitionSchemeDiscoveryPtr(new SchemaPartitionSchemeDiscovery(
      std::move(partition_base_dir), std::move(field_names)));
}

util::optional<PartitionKeysScheme::Key> HivePartitionScheme::ParseKey(
    const std::string& segment) {
  static std::regex hive_style("^([^=]+)=(.*)$");

  std::smatch matches;
  if (!std::regex_match(segment, matches, hive_style) || matches.size() != 3) {
    return util::nullopt;
  }

  return Key{matches[1].str(), matches[2].str()};
}

class HivePartitionSchemeDiscovery : public PartitionSchemeDiscovery {
 public:
  HivePartitionSchemeDiscovery(std::string partition_base_dir)
      : partition_base_dir_(std::move(partition_base_dir)) {}

  Result<std::shared_ptr<Schema>> Inspect(
      const fs::FileStatsVector& stats) const override {
    std::unordered_map<std::string, std::vector<std::string>> name_to_values;

    for (const auto& stats : stats) {
      if (!fs::internal::IsAncestorOf(partition_base_dir_, stats.path())) {
        continue;
      }

      auto segments = fs::internal::SplitAbstractPath(
          stats.path().substr(partition_base_dir_.size()));

      for (const auto& segment : segments) {
        if (auto key = HivePartitionScheme::ParseKey(segment)) {
          name_to_values[key->name].push_back(std::move(key->value));
        }
      }
    }

    return InferSchema(name_to_values);
  }

  Result<PartitionSchemePtr> Finish() const override {
    return PartitionSchemePtr(new SchemaPartitionScheme(schema_));
  }

 private:
  std::string partition_base_dir_;
};

PartitionSchemeDiscoveryPtr HivePartitionScheme::MakeDiscovery(
    std::string partition_base_dir) {
  return PartitionSchemeDiscoveryPtr(
      new HivePartitionSchemeDiscovery(std::move(partition_base_dir)));
}

}  // namespace dataset
}  // namespace arrow
