/*
 * Copyright 2023 Google LLC All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.zetasql.toolkit.catalog.bigquery;

import com.google.common.collect.ImmutableList;
import com.google.zetasql.Function;
import com.google.zetasql.FunctionArgumentType;
import com.google.zetasql.FunctionArgumentType.FunctionArgumentTypeOptions;
import com.google.zetasql.FunctionProtos.FunctionOptionsProto;
import com.google.zetasql.FunctionSignature;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.StructType.StructField;
import com.google.zetasql.TVFRelation;
import com.google.zetasql.TVFRelation.Column;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.ZetaSQLFunctions.FunctionEnums.ArgumentCardinality;
import com.google.zetasql.ZetaSQLFunctions.FunctionEnums.Mode;
import com.google.zetasql.ZetaSQLFunctions.FunctionEnums.NamedArgumentKind;
import com.google.zetasql.ZetaSQLFunctions.SignatureArgumentKind;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.CreateMode;
import com.google.zetasql.toolkit.catalog.CatalogOperations;
import com.google.zetasql.toolkit.catalog.ProcedureInfo;
import com.google.zetasql.toolkit.catalog.TVFInfo;
import com.google.zetasql.toolkit.catalog.TVFInfo.TVFType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Defines types, functions and procedures which are available in BigQuery but are not built into
 * ZetaSQL.
 *
 * @see #addToCatalog(SimpleCatalog) to add these resources to a {@link SimpleCatalog}.
 */
class BigQueryBuiltIns {

  public static final Map<String, Type> TYPE_ALIASES = new HashMap<>();

  static {
    TYPE_ALIASES.put("INT", simpleType(TypeKind.TYPE_INT64));
    TYPE_ALIASES.put("SMALLINT", simpleType(TypeKind.TYPE_INT64));
    TYPE_ALIASES.put("INTEGER", simpleType(TypeKind.TYPE_INT64));
    TYPE_ALIASES.put("BIGINT", simpleType(TypeKind.TYPE_INT64));
    TYPE_ALIASES.put("TINYINT", simpleType(TypeKind.TYPE_INT64));
    TYPE_ALIASES.put("BYTEINT", simpleType(TypeKind.TYPE_INT64));
    TYPE_ALIASES.put("DECIMAL", simpleType(TypeKind.TYPE_NUMERIC));
    TYPE_ALIASES.put("BIGDECIMAL", simpleType(TypeKind.TYPE_BIGNUMERIC));
  }

  private static final String BIGQUERY_FUNCTION_GROUP = "BigQuery";

  public static final List<Function> FUNCTIONS =
      ImmutableList.of(
          // CONTAINS_SUBSTR(STRING, ANY, [STRING]) -> BOOL
          new Function(
              "CONTAINS_SUBSTR",
              BIGQUERY_FUNCTION_GROUP,
              Mode.SCALAR,
              ImmutableList.of(
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_BOOL),
                      ImmutableList.of(
                          requiredArgument(TypeKind.TYPE_STRING, "expression"),
                          requiredArgument(SignatureArgumentKind.ARG_TYPE_ANY_1, "search_value_literal"),
                          optionalArgument(TypeKind.TYPE_STRING, "json_scope")),
                      -1))),
          // SEARCH(ANY, STRING, [STRING], [STRING]) -> BOOL
          new Function(
              "SEARCH",
              BIGQUERY_FUNCTION_GROUP,
              Mode.SCALAR,
              ImmutableList.of(
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_BOOL),
                      ImmutableList.of(
                          requiredArgument(SignatureArgumentKind.ARG_TYPE_ANY_1, "search_data"),
                          requiredArgument(TypeKind.TYPE_STRING, "search_query"),
                          optionalArgument(TypeKind.TYPE_STRING, "json_scope"),
                          optionalArgument(TypeKind.TYPE_STRING, "analyzer")),
                      -1))),
          // ML.IMPUTER(numerical | STRING, STRING) -> (FLOAT64 | STRING)
          new Function(
              "ML.IMPUTER",
              BIGQUERY_FUNCTION_GROUP,
              Mode.ANALYTIC,
              ImmutableList.of(
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_DOUBLE),
                      ImmutableList.of(
                          requiredArgument(TypeKind.TYPE_DOUBLE, "expression"),
                          requiredArgument(TypeKind.TYPE_STRING, "strategy")),
                      -1),
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_STRING),
                      ImmutableList.of(
                          requiredArgument(TypeKind.TYPE_STRING, "expression"),
                          requiredArgument(TypeKind.TYPE_STRING, "strategy")),
                      -1)),
              FunctionOptionsProto.newBuilder()
                  .setSupportsOverClause(true)
                  .build()),
          // ML.BUCKETIZE(numerical, ARRAY<INT64>, [BOOL]) -> STRING
          new Function(
              "ML.BUCKETIZE",
              BIGQUERY_FUNCTION_GROUP,
              Mode.SCALAR,
              ImmutableList.of(
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_STRING),
                      ImmutableList.of(
                          requiredArgument(TypeKind.TYPE_DOUBLE, "numerical_expression"),
                          requiredArgument(arrayType(TypeKind.TYPE_INT64), "array_split_points"),
                          optionalArgument(TypeKind.TYPE_BOOL, "exclude_boundaries")),
                      -1))),
          // ML.QUANTILE_BUCKETIZE(numerical, INT64) -> STRING
          new Function(
              "ML.QUANTILE_BUCKETIZE",
              BIGQUERY_FUNCTION_GROUP,
              Mode.ANALYTIC,
              ImmutableList.of(
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_STRING),
                      ImmutableList.of(
                          requiredArgument(TypeKind.TYPE_DOUBLE, "numerical_expression"),
                          requiredArgument(TypeKind.TYPE_INT64, "num_buckets")),
                      -1)),
              FunctionOptionsProto.newBuilder()
                  .setSupportsOverClause(true)
                  .build()),
          // ML.MAX_ABS_SCALER(numerical) -> FLOAT64
          new Function(
              "ML.MAX_ABS_SCALER",
              BIGQUERY_FUNCTION_GROUP,
              Mode.ANALYTIC,
              ImmutableList.of(
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_DOUBLE),
                      ImmutableList.of(
                          requiredArgument(TypeKind.TYPE_DOUBLE, "numerical_expression")),
                      -1)),
              FunctionOptionsProto.newBuilder()
                  .setSupportsOverClause(true)
                  .build()),
          // ML.MIN_MAX_SCALER(numerical) -> FLOAT64
          new Function(
              "ML.MIN_MAX_SCALER",
              BIGQUERY_FUNCTION_GROUP,
              Mode.ANALYTIC,
              ImmutableList.of(
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_DOUBLE),
                      ImmutableList.of(
                          requiredArgument(TypeKind.TYPE_DOUBLE, "numerical_expression")),
                      -1)),
              FunctionOptionsProto.newBuilder()
                  .setSupportsOverClause(true)
                  .build()),
          // ML.NORMALIZER
          new Function(
              "ML.NORMALIZER",
              BIGQUERY_FUNCTION_GROUP,
              Mode.SCALAR,
              ImmutableList.of(
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_DOUBLE),
                      ImmutableList.of(
                          requiredArgument(arrayType(TypeKind.TYPE_FLOAT), "array_expression"),
                          optionalArgument(TypeKind.TYPE_DOUBLE, "p")),
                      -1))),
          // ML.ROBUST_SCALER(numerical, [ARRAY<INT64>], [BOOL], [BOOL]) -> FLOAT64
          new Function(
              "ML.ROBUST_SCALER",
              BIGQUERY_FUNCTION_GROUP,
              Mode.ANALYTIC,
              ImmutableList.of(
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_DOUBLE),
                      ImmutableList.of(
                          requiredArgument(TypeKind.TYPE_DOUBLE, "numerical_expression"),
                          optionalArgument(arrayType(TypeKind.TYPE_INT64), "quantile_range"),
                          optionalArgument(TypeKind.TYPE_BOOL, "with_median"),
                          optionalArgument(TypeKind.TYPE_BOOL, "with_quantile_range")),
                      -1)),
              FunctionOptionsProto.newBuilder()
                  .setSupportsOverClause(true)
                  .build()),
          // ML.STANDARD_SCALER(numerical) -> FLOAT64
          new Function(
              "ML.STANDARD_SCALER",
              BIGQUERY_FUNCTION_GROUP,
              Mode.ANALYTIC,
              ImmutableList.of(
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_DOUBLE),
                      ImmutableList.of(
                          requiredArgument(TypeKind.TYPE_DOUBLE, "numerical_expression")),
                      -1)),
              FunctionOptionsProto.newBuilder()
                  .setSupportsOverClause(true)
                  .build()),
          // ML.HASH_BUCKETIZE(STRING, INT64) -> INT64
          new Function(
              "ML.HASH_BUCKETIZE",
              BIGQUERY_FUNCTION_GROUP,
              Mode.SCALAR,
              ImmutableList.of(
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_INT64),
                      ImmutableList.of(
                          requiredArgument(TypeKind.TYPE_STRING, "string_expression"),
                          requiredArgument(TypeKind.TYPE_INT64, "hash_bucket_size")),
                      -1))),
          // ML.LABEL_ENCODER(STRING, [INT64], [INT64]) -> INT64
          new Function(
              "ML.LABEL_ENCODER",
              BIGQUERY_FUNCTION_GROUP,
              Mode.ANALYTIC,
              ImmutableList.of(
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_INT64),
                      ImmutableList.of(
                          requiredArgument(TypeKind.TYPE_STRING, "string_expression"),
                          optionalArgument(TypeKind.TYPE_INT64, "top_k"),
                          optionalArgument(TypeKind.TYPE_INT64, "frequency_threshold")),
                      -1)),
              FunctionOptionsProto.newBuilder()
                  .setSupportsOverClause(true)
                  .build()),
          // ML.MULTI_HOT_ENCODER(ARRAY<STRING>, [INT64], [INT64]) -> ARRAY<STRUCT<INT64, FLOAT64>>
          new Function(
              "ML.MULTI_HOT_ENCODER",
              BIGQUERY_FUNCTION_GROUP,
              Mode.ANALYTIC,
              ImmutableList.of(
                  new FunctionSignature(
                      new FunctionArgumentType(
                          TypeFactory.createArrayType(
                              TypeFactory.createStructType(
                                  ImmutableList.of(
                                      new StructField("index", simpleType(TypeKind.TYPE_INT64)),
                                      new StructField("value", simpleType(TypeKind.TYPE_DOUBLE)))))),
                      ImmutableList.of(
                          requiredArgument(arrayType(TypeKind.TYPE_STRING), "array_expression"),
                          optionalArgument(TypeKind.TYPE_INT64, "top_k"),
                          optionalArgument(TypeKind.TYPE_INT64, "frequency_threshold")),
                      -1)),
              FunctionOptionsProto.newBuilder()
                  .setSupportsOverClause(true)
                  .build()),
          // ML.ONE_HOT_ENCODER(ARRAY<STRING>, [STRING], [INT64], [INT64])
          //    -> ARRAY<STRUCT<INT64, FLOAT64>>
          new Function(
              "ML.ONE_HOT_ENCODER",
              BIGQUERY_FUNCTION_GROUP,
              Mode.ANALYTIC,
              ImmutableList.of(
                  new FunctionSignature(
                      new FunctionArgumentType(
                          TypeFactory.createArrayType(
                              TypeFactory.createStructType(
                                  ImmutableList.of(
                                      new StructField("index", simpleType(TypeKind.TYPE_INT64)),
                                      new StructField("value", simpleType(TypeKind.TYPE_DOUBLE)))))),
                      ImmutableList.of(
                          requiredArgument(arrayType(TypeKind.TYPE_STRING), "array_expression"),
                          optionalArgument(TypeKind.TYPE_STRING, "drop"),
                          optionalArgument(TypeKind.TYPE_INT64, "top_k"),
                          optionalArgument(TypeKind.TYPE_INT64, "frequency_threshold")),
                      -1)),
              FunctionOptionsProto.newBuilder()
                  .setSupportsOverClause(true)
                  .build()),
          // ML.NGRAMS(ARRAY<STRING>, ARRAY<INT64>, [STRING]) -> ARRAY<STRING>
          new Function(
              "ML.NGRAMS",
              BIGQUERY_FUNCTION_GROUP,
              Mode.SCALAR,
              ImmutableList.of(
                  new FunctionSignature(
                      returnType(arrayType(TypeKind.TYPE_STRING)),
                      ImmutableList.of(
                          requiredArgument(arrayType(TypeKind.TYPE_STRING), "array_input"),
                          requiredArgument(arrayType(TypeKind.TYPE_INT64), "range"),
                          optionalArgument(TypeKind.TYPE_STRING, "separator")),
                      -1))),
          // ML.BAG_OF_WORDS(ARRAY<STRING>, [INT64], [INT64]) -> ARRAY<STRUCT<INT64, FLOAT64>>
          new Function(
              "ML.BAG_OF_WORDS",
              BIGQUERY_FUNCTION_GROUP,
              Mode.ANALYTIC,
              ImmutableList.of(
                  new FunctionSignature(
                      new FunctionArgumentType(
                          TypeFactory.createArrayType(
                              TypeFactory.createStructType(
                                  ImmutableList.of(
                                      new StructField("index", simpleType(TypeKind.TYPE_INT64)),
                                      new StructField("value", simpleType(TypeKind.TYPE_DOUBLE)))))),
                      ImmutableList.of(
                          requiredArgument(arrayType(TypeKind.TYPE_STRING), "tokenized_document"),
                          optionalArgument(TypeKind.TYPE_INT64, "top_k"),
                          optionalArgument(TypeKind.TYPE_INT64, "frequency_threshold")),
                      -1)),
              FunctionOptionsProto.newBuilder()
                  .setSupportsOverClause(true)
                  .build()),
          // ML.TF_IDF(ARRAY<STRING>, [INT64], [INT64]) -> ARRAY<STRUCT<INT64, FLOAT64>>
          new Function(
              "ML.TF_IDF",
              BIGQUERY_FUNCTION_GROUP,
              Mode.ANALYTIC,
              ImmutableList.of(
                  new FunctionSignature(
                      new FunctionArgumentType(
                          TypeFactory.createArrayType(
                              TypeFactory.createStructType(
                                  ImmutableList.of(
                                      new StructField("index", simpleType(TypeKind.TYPE_INT64)),
                                      new StructField("value", simpleType(TypeKind.TYPE_DOUBLE)))))),
                      ImmutableList.of(
                          requiredArgument(arrayType(TypeKind.TYPE_STRING), "tokenized_document"),
                          optionalArgument(TypeKind.TYPE_INT64, "top_k"),
                          optionalArgument(TypeKind.TYPE_INT64, "frequency_threshold")),
                      -1)),
              FunctionOptionsProto.newBuilder()
                  .setSupportsOverClause(true)
                  .build()),
          // ML.LP_NORM(ARRAY<numerical>, FLOAT64) -> FLOAT64
          new Function(
              "ML.LP_NORM",
              BIGQUERY_FUNCTION_GROUP,
              Mode.SCALAR,
              ImmutableList.of(
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_DOUBLE),
                      ImmutableList.of(
                          requiredArgument(arrayType(TypeKind.TYPE_INT64), "vector"),
                          requiredArgument(TypeKind.TYPE_DOUBLE, "degree")),
                      -1),
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_DOUBLE),
                      ImmutableList.of(
                          requiredArgument(arrayType(TypeKind.TYPE_NUMERIC), "vector"),
                          requiredArgument(TypeKind.TYPE_DOUBLE, "degree")),
                      -1),
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_DOUBLE),
                      ImmutableList.of(
                          requiredArgument(arrayType(TypeKind.TYPE_BIGNUMERIC), "vector"),
                          requiredArgument(TypeKind.TYPE_DOUBLE, "degree")),
                      -1),
                  new FunctionSignature(
                      returnType(TypeKind.TYPE_DOUBLE),
                      ImmutableList.of(
                          requiredArgument(arrayType(TypeKind.TYPE_DOUBLE), "vector"),
                          requiredArgument(TypeKind.TYPE_DOUBLE, "degree")),
                      -1))));

  public static final List<TVFInfo> TABLE_VALUED_FUNCTIONS = ImmutableList.of(
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.FEATURE_INFO"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
                  ImmutableList.of(
                      Column.create("input", simpleType(TypeKind.TYPE_STRING)),
                      Column.create("min", simpleType(TypeKind.TYPE_DOUBLE)),
                      Column.create("max", simpleType(TypeKind.TYPE_DOUBLE)),
                      Column.create("mean", simpleType(TypeKind.TYPE_DOUBLE)),
                      Column.create("median", simpleType(TypeKind.TYPE_DOUBLE)),
                      Column.create("stddev", simpleType(TypeKind.TYPE_DOUBLE)),
                      Column.create("category_count", simpleType(TypeKind.TYPE_INT64)),
                      Column.create("null_count", simpleType(TypeKind.TYPE_INT64)),
                      Column.create("dimension", simpleType(TypeKind.TYPE_INT64)))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.FEATURES_AT_TIME"))
          .setTVFType(TVFType.FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_RELATION, "feature_table"),
                  optionalArgument(TypeKind.TYPE_TIMESTAMP, "time"),
                  optionalArgument(TypeKind.TYPE_INT64, "num_rows"),
                  optionalArgument(TypeKind.TYPE_BOOL, "ignore_feature_nulls")),
              -1))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.ENTITY_FEATURES_AT_TIME"))
          .setTVFType(TVFType.FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_RELATION, "feature_table"),
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_RELATION, "entity_time_table"),
                  optionalArgument(TypeKind.TYPE_INT64, "num_rows"),
                  optionalArgument(TypeKind.TYPE_BOOL, "ignore_feature_nulls")),
              -1))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.TRIAL_INFO"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model")),
              -1))
          .setOutputSchema(
              TVFRelation.createColumnBased(
                      ImmutableList.of(
                          Column.create("trial_id", simpleType(TypeKind.TYPE_INT64)),
                          Column.create("hyperparameters", TypeFactory.createStructType(ImmutableList.of())),
                          Column.create("hparam_tuning_evaluation_metrics", TypeFactory.createStructType(ImmutableList.of())),
                          Column.create("training_loss", simpleType(TypeKind.TYPE_DOUBLE)),
                          Column.create("eval_loss", simpleType(TypeKind.TYPE_DOUBLE)),
                          Column.create("status", simpleType(TypeKind.TYPE_STRING)),
                          Column.create("error_message", simpleType(TypeKind.TYPE_STRING)),
                          Column.create("is_optimal", simpleType(TypeKind.TYPE_BOOL)))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.ROC_CURVE"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model"),
                  optionalArgument(SignatureArgumentKind.ARG_TYPE_RELATION, "table"),
                  optionalArgument(arrayType(TypeKind.TYPE_DOUBLE), "thresholds"),
                  optionalArgument(SignatureArgumentKind.ARG_TYPE_ARBITRARY, "options")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
                ImmutableList.of(
                  Column.create("threshold", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("recall", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("true_positives", simpleType(TypeKind.TYPE_INT64)),
                  Column.create("false_positives", simpleType(TypeKind.TYPE_INT64)),
                  Column.create("true_negatives", simpleType(TypeKind.TYPE_INT64)),
                  Column.create("false_negatives", simpleType(TypeKind.TYPE_INT64)))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.TRAINING_INFO"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("training_run", simpleType(TypeKind.TYPE_INT64)),
                  Column.create("iteration", simpleType(TypeKind.TYPE_INT64)),
                  Column.create("loss", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("eval_loss", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("learning_rate", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("duration_ms", simpleType(TypeKind.TYPE_INT64)),
                  Column.create("cluster_info",
                      TypeFactory.createArrayType(
                          TypeFactory.createStructType(
                              ImmutableList.of(
                                  new StructField("centroid_id", simpleType(TypeKind.TYPE_INT64)),
                                  new StructField("cluster_radius", simpleType(TypeKind.TYPE_DOUBLE)),
                                  new StructField("cluster_size", simpleType(TypeKind.TYPE_INT64)))))))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.RECONSTRUCTION_LOSS"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model"),
                  optionalArgument(SignatureArgumentKind.ARG_TYPE_RELATION, "table")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("mean_absolute_error", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("mean_squared_error", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("mean_squared_log_error", simpleType(TypeKind.TYPE_DOUBLE)))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.HOLIDAY_INFO"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("region", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("holiday_name", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("primary_date", simpleType(TypeKind.TYPE_DATE)),
                  Column.create("preholiday_days", simpleType(TypeKind.TYPE_INT64)),
                  Column.create("postholiday_days", simpleType(TypeKind.TYPE_INT64)))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.FORECAST"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model"),
                  optionalArgument(SignatureArgumentKind.ARG_TYPE_RELATION, "table"),
                  optionalArgument(SignatureArgumentKind.ARG_TYPE_ARBITRARY, "options")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("forecast_timestamp", simpleType(TypeKind.TYPE_TIMESTAMP)),
                  Column.create("forecast_value", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("standard_error", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("confidence_level", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("prediction_interval_lower_bound", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("prediction_interval_upper_bound", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("confidence_interval_lower_bound", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("confidence_interval_upper_bound", simpleType(TypeKind.TYPE_DOUBLE)))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.GENERATE_TEXT"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model"),
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_RELATION, "table"),
                  optionalArgument(SignatureArgumentKind.ARG_TYPE_ARBITRARY, "options")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("prompt", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("ml_generate_text_result", simpleType(TypeKind.TYPE_JSON)),
                  Column.create("ml_generate_text_llm_result", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("ml_generate_text_rai_result", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("ml_generate_text_status", simpleType(TypeKind.TYPE_STRING)))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.GENERATE_TEXT_EMBEDDING"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model"),
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_RELATION, "table"),
                  optionalArgument(SignatureArgumentKind.ARG_TYPE_ARBITRARY, "options")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("content", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("ml_embed_text_result", simpleType(TypeKind.TYPE_JSON)),
                  Column.create("text_embedding", arrayType(TypeKind.TYPE_DOUBLE)),
                  Column.create("statistics", simpleType(TypeKind.TYPE_JSON)),
                  Column.create("ml_embed_text_status", simpleType(TypeKind.TYPE_STRING)))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.UNDERSTAND_TEXT"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model"),
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_RELATION, "table"),
                  optionalArgument(SignatureArgumentKind.ARG_TYPE_ARBITRARY, "options")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("text_content", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("ml_understand_text_result", simpleType(TypeKind.TYPE_JSON)),
                  Column.create("ml_understand_text_status", simpleType(TypeKind.TYPE_STRING)))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.TRANSLATE"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model"),
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_RELATION, "table"),
                  optionalArgument(SignatureArgumentKind.ARG_TYPE_ARBITRARY, "options")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("text_content", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("ml_translate_result", simpleType(TypeKind.TYPE_JSON)),
                  Column.create("ml_translate_status", simpleType(TypeKind.TYPE_STRING)))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.ANNOTATE_IMAGE"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model"),
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_RELATION, "table"),
                  optionalArgument(SignatureArgumentKind.ARG_TYPE_ARBITRARY, "options")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("ml_annotate_image_result", simpleType(TypeKind.TYPE_JSON)),
                  Column.create("ml_annotate_image_status", simpleType(TypeKind.TYPE_STRING)))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.FEATURE_IMPORTANCE"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("feature", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("importance_weight", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("importance_gain", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("importance_cover", simpleType(TypeKind.TYPE_DOUBLE)))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.ADVANCED_WEIGHTS"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model"),
                  optionalArgument(SignatureArgumentKind.ARG_TYPE_ARBITRARY, "options")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("processed_input", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("category", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("weight", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("standard_error", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("p_value", simpleType(TypeKind.TYPE_DOUBLE)))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.CENTROIDS"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model"),
                  optionalArgument(SignatureArgumentKind.ARG_TYPE_ARBITRARY, "options")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("trial_id", simpleType(TypeKind.TYPE_INT64)),
                  Column.create("centroid_id", simpleType(TypeKind.TYPE_INT64)),
                  Column.create("feature", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("numerical_value", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("geography_value", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("categorical_value",
                      TypeFactory.createArrayType(
                          TypeFactory.createStructType(
                              ImmutableList.of(
                                  new StructField("category", simpleType(TypeKind.TYPE_STRING)),
                                  new StructField("value", simpleType(TypeKind.TYPE_STRING)))))))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.PRINCIPAL_COMPONENTS"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("principal_component_id", simpleType(TypeKind.TYPE_INT64)),
                  Column.create("feature", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("numerical_value", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("categorical_value",
                      TypeFactory.createArrayType(
                          TypeFactory.createStructType(
                              ImmutableList.of(
                                  new StructField("category", simpleType(TypeKind.TYPE_STRING)),
                                  new StructField("value", simpleType(TypeKind.TYPE_STRING)))))))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.PRINCIPAL_COMPONENT_INFO"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("principal_component_id", simpleType(TypeKind.TYPE_INT64)),
                  Column.create("eigenvalue", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("explained_variance_ratio", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("cumulative_explained_variance_ratio", simpleType(TypeKind.TYPE_DOUBLE)))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.ARIMA_COEFFICIENTS"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("ar_coefficients", arrayType(TypeKind.TYPE_DOUBLE)),
                  Column.create("ma_coefficients", arrayType(TypeKind.TYPE_DOUBLE)),
                  Column.create("intercept_or_drift", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("processed_input", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("weight", simpleType(TypeKind.TYPE_DOUBLE)),
                  // TODO: double check category_weights is an ARRAY<STRUCT<...>>
                  Column.create("category_weights",
                      TypeFactory.createArrayType(
                          TypeFactory.createStructType(
                              ImmutableList.of(
                                  new StructField("category", simpleType(TypeKind.TYPE_STRING)),
                                  new StructField("weight", simpleType(TypeKind.TYPE_DOUBLE)))))))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.GLOBAL_EXPLAIN"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model"),
                  optionalArgument(SignatureArgumentKind.ARG_TYPE_ARBITRARY, "options")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("feature", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("attribution", simpleType(TypeKind.TYPE_DOUBLE)))))
          .build(),
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of("ML.WEIGHTS"))
          .setTVFType(TVFType.FIXED_OUTPUT_SCHEMA)
          .setSignature(new FunctionSignature(
              returnType(SignatureArgumentKind.ARG_TYPE_RELATION),
              ImmutableList.of(
                  requiredArgument(SignatureArgumentKind.ARG_TYPE_MODEL, "model"),
                  optionalArgument(SignatureArgumentKind.ARG_TYPE_ARBITRARY, "options")),
              -1))
          .setOutputSchema(TVFRelation.createColumnBased(
              ImmutableList.of(
                  Column.create("trial_id", simpleType(TypeKind.TYPE_INT64)),
                  Column.create("processed_input", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("feature", simpleType(TypeKind.TYPE_STRING)),
                  Column.create("weight", simpleType(TypeKind.TYPE_DOUBLE)),
                  Column.create("attribution", simpleType(TypeKind.TYPE_DOUBLE)),
                  // TODO: double check category_weights is an ARRAY<STRUCT<...>>
                  Column.create("category_weights",
                      TypeFactory.createArrayType(
                          TypeFactory.createStructType(
                              ImmutableList.of(
                                  new StructField("category", simpleType(TypeKind.TYPE_STRING)),
                                  new StructField("weight", simpleType(TypeKind.TYPE_DOUBLE)))))),
                  // TODO: double check category_weights is an ARRAY<STRUCT<...>>
                  Column.create("factor_weights",
                      TypeFactory.createArrayType(
                          TypeFactory.createStructType(
                              ImmutableList.of(
                                  new StructField("factor", simpleType(TypeKind.TYPE_INT64)),
                                  new StructField("weight", simpleType(TypeKind.TYPE_DOUBLE)))))),
                  Column.create("intercept", simpleType(TypeKind.TYPE_DOUBLE)))))
          .build());

  public static final List<ProcedureInfo> PROCEDURES =
      ImmutableList.of(
          // BQ.ABORT_SESSION([STRING])
          new ProcedureInfo(
              ImmutableList.of("BQ.ABORT_SESSION"),
              new FunctionSignature(
                  new FunctionArgumentType(simpleType(TypeKind.TYPE_STRING)),
                  ImmutableList.of(
                      new FunctionArgumentType(
                          simpleType(TypeKind.TYPE_STRING),
                          FunctionArgumentTypeOptions.builder()
                              .setArgumentName("session_id", NamedArgumentKind.POSITIONAL_ONLY)
                              .setCardinality(ArgumentCardinality.OPTIONAL)
                              .build(),
                          1)),
                  -1)),
          // BQ.JOBS.CANCEL(STRING)
          new ProcedureInfo(
              ImmutableList.of("BQ.JOBS.CANCEL"),
              new FunctionSignature(
                  new FunctionArgumentType(simpleType(TypeKind.TYPE_STRING)),
                  ImmutableList.of(
                      new FunctionArgumentType(
                          simpleType(TypeKind.TYPE_STRING),
                          FunctionArgumentTypeOptions.builder()
                              .setArgumentName("job", NamedArgumentKind.POSITIONAL_ONLY)
                              .setCardinality(ArgumentCardinality.REQUIRED)
                              .build(),
                          1)),
                  -1)),
          // BQ.REFRESH_EXTERNAL_METADATA_CACHE(STRING)
          new ProcedureInfo(
              ImmutableList.of("BQ.REFRESH_EXTERNAL_METADATA_CACHE"),
              new FunctionSignature(
                  new FunctionArgumentType(simpleType(TypeKind.TYPE_STRING)),
                  ImmutableList.of(
                      new FunctionArgumentType(
                          simpleType(TypeKind.TYPE_STRING),
                          FunctionArgumentTypeOptions.builder()
                              .setArgumentName("table_name", NamedArgumentKind.POSITIONAL_ONLY)
                              .setCardinality(ArgumentCardinality.REQUIRED)
                              .build(),
                          1)),
                  -1)),
          // BQ.REFRESH_MATERIALIZED_VIEW(STRING)
          new ProcedureInfo(
              ImmutableList.of("BQ.REFRESH_MATERIALIZED_VIEW"),
              new FunctionSignature(
                  new FunctionArgumentType(simpleType(TypeKind.TYPE_STRING)),
                  ImmutableList.of(
                      new FunctionArgumentType(
                          simpleType(TypeKind.TYPE_STRING),
                          FunctionArgumentTypeOptions.builder()
                              .setArgumentName("view_name", NamedArgumentKind.POSITIONAL_ONLY)
                              .setCardinality(ArgumentCardinality.REQUIRED)
                              .build(),
                          1)),
                  -1)));

  /**
   * Adds the defined BigQuery-specific types, functions and procedures to the provided {@link
   * SimpleCatalog}
   *
   * @param catalog The SimpleCatalog to which resources should be added
   */
  public static void addToCatalog(SimpleCatalog catalog) {
    TYPE_ALIASES.forEach(catalog::addType);
    FUNCTIONS.forEach(catalog::addFunction);
    TABLE_VALUED_FUNCTIONS.forEach(tvfInfo -> catalog.addTableValuedFunction(tvfInfo.toTVF()));
    PROCEDURES.forEach(procedure ->
      CatalogOperations.createProcedureInCatalog(
          catalog, procedure.getFullName(), procedure, CreateMode.CREATE_DEFAULT));
  }

  private static Type simpleType(TypeKind kind) {
    return TypeFactory.createSimpleType(kind);
  }

  private static Type arrayType(TypeKind kind) {
    return TypeFactory.createArrayType(simpleType(kind));
  }

  private static FunctionArgumentType returnType(TypeKind kind) {
    return returnType(simpleType(kind));
  }

  private static FunctionArgumentType returnType(Type type) {
    return new FunctionArgumentType(type);
  }

  private static FunctionArgumentType returnType(SignatureArgumentKind kind) {
    return new FunctionArgumentType(kind);
  }

  private static FunctionArgumentType requiredArgument(TypeKind kind, String name) {
    return requiredArgument(simpleType(kind), name);
  }

  private static FunctionArgumentType requiredArgument(Type type, String name) {
    return new FunctionArgumentType(
        type,
        FunctionArgumentTypeOptions.builder()
            .setArgumentName(name, NamedArgumentKind.POSITIONAL_OR_NAMED)
            .setCardinality(ArgumentCardinality.REQUIRED)
            .build(),
        1);
  }

  private static FunctionArgumentType requiredArgument(SignatureArgumentKind kind, String name) {
    return new FunctionArgumentType(
        kind,
        FunctionArgumentTypeOptions.builder()
            .setArgumentName(name, NamedArgumentKind.POSITIONAL_OR_NAMED)
            .setCardinality(ArgumentCardinality.REQUIRED)
            .build(),
        1);
  }

  private static FunctionArgumentType optionalArgument(TypeKind kind, String name) {
    return optionalArgument(simpleType(kind), name);
  }

  private static FunctionArgumentType optionalArgument(Type type, String name) {
    return new FunctionArgumentType(
        type,
        FunctionArgumentTypeOptions.builder()
            .setArgumentName(name, NamedArgumentKind.POSITIONAL_OR_NAMED)
            .setCardinality(ArgumentCardinality.OPTIONAL)
            .build(),
        1);
  }

  private static FunctionArgumentType optionalArgument(SignatureArgumentKind kind, String name) {
    return new FunctionArgumentType(
        kind,
        FunctionArgumentTypeOptions.builder()
            .setArgumentName(name, NamedArgumentKind.POSITIONAL_OR_NAMED)
            .setCardinality(ArgumentCardinality.OPTIONAL)
            .build(),
        1);
  }

  private BigQueryBuiltIns() {}
}
