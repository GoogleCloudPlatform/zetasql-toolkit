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

package com.google.zetasql.toolkit.options;

import com.google.common.collect.ImmutableSet;
import com.google.zetasql.LanguageOptions;
import com.google.zetasql.ZetaSQLOptions.LanguageFeature;
import com.google.zetasql.ZetaSQLOptions.NameResolutionMode;
import com.google.zetasql.ZetaSQLOptions.ProductMode;
import com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind;

public class BigQueryLanguageOptions {

    private static final LanguageOptions languageOptions = new LanguageOptions();

    static {
        languageOptions.setNameResolutionMode(NameResolutionMode.NAME_RESOLUTION_DEFAULT);
        languageOptions.setProductMode(ProductMode.PRODUCT_EXTERNAL);

        languageOptions.setEnabledLanguageFeatures(
                ImmutableSet.of(
                        LanguageFeature.FEATURE_ALLOW_MISSING_PATH_EXPRESSION_IN_ALTER_DDL,
                        LanguageFeature.FEATURE_ALTER_COLUMN_SET_DATA_TYPE,
                        LanguageFeature.FEATURE_ALTER_TABLE_RENAME_COLUMN,
                        LanguageFeature.FEATURE_ANALYTIC_FUNCTIONS,
                        LanguageFeature.FEATURE_ANONYMIZATION,
                        LanguageFeature.FEATURE_BIGNUMERIC_TYPE,
                        LanguageFeature.FEATURE_CBRT_FUNCTIONS,
                        LanguageFeature.FEATURE_CREATE_EXTERNAL_TABLE_WITH_CONNECTION,
                        LanguageFeature.FEATURE_CREATE_EXTERNAL_TABLE_WITH_PARTITION_COLUMNS,
                        LanguageFeature.FEATURE_CREATE_EXTERNAL_TABLE_WITH_TABLE_ELEMENT_LIST,
                        LanguageFeature.FEATURE_CREATE_MATERIALIZED_VIEW_CLUSTER_BY,
                        LanguageFeature.FEATURE_CREATE_MATERIALIZED_VIEW_PARTITION_BY,
                        LanguageFeature.FEATURE_CREATE_SNAPSHOT_TABLE,
                        LanguageFeature.FEATURE_CREATE_TABLE_AS_SELECT_COLUMN_LIST,
                        LanguageFeature.FEATURE_CREATE_TABLE_CLONE,
                        LanguageFeature.FEATURE_CREATE_TABLE_CLUSTER_BY,
                        LanguageFeature.FEATURE_CREATE_TABLE_COPY,
                        LanguageFeature.FEATURE_CREATE_TABLE_FIELD_ANNOTATIONS,
                        LanguageFeature.FEATURE_CREATE_TABLE_FUNCTION,
                        LanguageFeature.FEATURE_CREATE_TABLE_LIKE,
                        LanguageFeature.FEATURE_CREATE_TABLE_NOT_NULL,
                        LanguageFeature.FEATURE_CREATE_TABLE_PARTITION_BY,
                        LanguageFeature.FEATURE_CREATE_VIEW_WITH_COLUMN_LIST,
                        LanguageFeature.FEATURE_DML_UPDATE_WITH_JOIN,
                        LanguageFeature.FEATURE_ENCRYPTION,
                        LanguageFeature.FEATURE_FOREIGN_KEYS,
                        LanguageFeature.FEATURE_GEOGRAPHY,
                        LanguageFeature.FEATURE_GROUP_BY_ROLLUP,
                        LanguageFeature.FEATURE_INTERVAL_TYPE,
                        LanguageFeature.FEATURE_INVERSE_TRIG_FUNCTIONS,
                        LanguageFeature.FEATURE_JSON_ARRAY_FUNCTIONS,
                        LanguageFeature.FEATURE_JSON_TYPE,
                        LanguageFeature.FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS,
                        LanguageFeature.FEATURE_NAMED_ARGUMENTS,
                        LanguageFeature.FEATURE_NUMERIC_TYPE,
                        LanguageFeature.FEATURE_PARAMETERIZED_TYPES,
                        LanguageFeature.FEATURE_PARAMETERS_IN_GRANTEE_LIST,
                        LanguageFeature.FEATURE_ROUND_WITH_ROUNDING_MODE,
                        LanguageFeature.FEATURE_TABLESAMPLE,
                        LanguageFeature.FEATURE_TABLE_VALUED_FUNCTIONS,
                        LanguageFeature.FEATURE_TEMPLATE_FUNCTIONS,
                        LanguageFeature.FEATURE_TIMESTAMP_NANOS,
                        LanguageFeature.FEATURE_TIME_BUCKET_FUNCTIONS,
                        LanguageFeature.FEATURE_UNENFORCED_PRIMARY_KEYS,
                        LanguageFeature.FEATURE_V_1_1_HAVING_IN_AGGREGATE,
                        LanguageFeature.FEATURE_V_1_1_LIMIT_IN_AGGREGATE,
                        LanguageFeature.FEATURE_V_1_1_NULL_HANDLING_MODIFIER_IN_AGGREGATE,
                        LanguageFeature.FEATURE_V_1_1_NULL_HANDLING_MODIFIER_IN_ANALYTIC,
                        LanguageFeature.FEATURE_V_1_1_ORDER_BY_IN_AGGREGATE,
                        LanguageFeature.FEATURE_V_1_1_SELECT_STAR_EXCEPT_REPLACE,
                        LanguageFeature.FEATURE_V_1_1_WITH_ON_SUBQUERY,
                        LanguageFeature.FEATURE_V_1_2_CIVIL_TIME,
                        LanguageFeature.FEATURE_V_1_2_SAFE_FUNCTION_CALL,
                        LanguageFeature.FEATURE_V_1_2_WEEK_WITH_WEEKDAY,
                        LanguageFeature.FEATURE_V_1_3_ADDITIONAL_STRING_FUNCTIONS,
                        LanguageFeature.FEATURE_V_1_3_ALLOW_DASHES_IN_TABLE_NAME,
                        LanguageFeature.FEATURE_V_1_3_ALLOW_REGEXP_EXTRACT_OPTIONALS,
                        LanguageFeature.FEATURE_V_1_3_ANNOTATION_FRAMEWORK,
                        LanguageFeature.FEATURE_V_1_3_CASE_STMT,
                        LanguageFeature.FEATURE_V_1_3_COLLATION_SUPPORT,
                        LanguageFeature.FEATURE_V_1_3_COLUMN_DEFAULT_VALUE,
                        LanguageFeature.FEATURE_V_1_3_CONCAT_MIXED_TYPES,
                        LanguageFeature.FEATURE_V_1_3_DATE_ARITHMETICS,
                        LanguageFeature.FEATURE_V_1_3_DATE_TIME_CONSTRUCTORS,
                        LanguageFeature.FEATURE_V_1_3_DECIMAL_ALIAS,
                        LanguageFeature.FEATURE_V_1_3_EXTENDED_DATE_TIME_SIGNATURES,
                        LanguageFeature.FEATURE_V_1_3_EXTENDED_GEOGRAPHY_PARSERS,
                        LanguageFeature.FEATURE_V_1_3_FORMAT_IN_CAST,
                        LanguageFeature.FEATURE_V_1_3_FOR_IN,
                        LanguageFeature.FEATURE_V_1_3_IS_DISTINCT,
                        LanguageFeature.FEATURE_V_1_3_LIKE_ANY_SOME_ALL,
                        LanguageFeature.FEATURE_V_1_3_NULLS_FIRST_LAST_IN_ORDER_BY,
                        LanguageFeature.FEATURE_V_1_3_OMIT_INSERT_COLUMN_LIST,
                        LanguageFeature.FEATURE_V_1_3_PIVOT,
                        LanguageFeature.FEATURE_V_1_3_QUALIFY,
                        LanguageFeature.FEATURE_V_1_3_REMOTE_FUNCTION,
                        LanguageFeature.FEATURE_V_1_3_REPEAT,
                        LanguageFeature.FEATURE_V_1_3_SCRIPT_LABEL,
                        LanguageFeature.FEATURE_V_1_3_UNNEST_AND_FLATTEN_ARRAYS,
                        LanguageFeature.FEATURE_V_1_3_UNPIVOT,
                        LanguageFeature.FEATURE_V_1_3_WITH_GROUP_ROWS,
                        LanguageFeature.FEATURE_V_1_3_WITH_RECURSIVE,
                        LanguageFeature.FEATURE_V_1_4_BARE_ARRAY_ACCESS,
                        LanguageFeature.FEATURE_V_1_4_ARRAY_AGGREGATION_FUNCTIONS,
                        LanguageFeature.FEATURE_V_1_4_ARRAY_FIND_FUNCTIONS,
                        LanguageFeature.FEATURE_V_1_4_SAFE_FUNCTION_CALL_WITH_LAMBDA_ARGS,
                        LanguageFeature.FEATURE_V_1_4_REMOTE_MODEL,
                        LanguageFeature.FEATURE_V_1_4_STRUCT_POSITIONAL_ACCESSOR,
                        LanguageFeature.FEATURE_V_1_4_SINGLE_TABLE_NAME_ARRAY_PATH,
                        LanguageFeature.FEATURE_V_1_4_SQL_MACROS,
                        LanguageFeature.FEATURE_V_1_4_COLLATION_IN_WITH_RECURSIVE,
                        LanguageFeature.FEATURE_V_1_4_COLLATION_IN_EXPLICIT_CAST,
                        LanguageFeature.FEATURE_V_1_4_LOAD_DATA_PARTITIONS,
                        LanguageFeature.FEATURE_V_1_4_CREATE_MODEL_WITH_ALIASED_QUERY_LIST,
                        LanguageFeature.FEATURE_V_1_4_LOAD_DATA_TEMP_TABLE,
                        LanguageFeature.FEATURE_V_1_4_CORRESPONDING,
                        LanguageFeature.FEATURE_V_1_4_SEQUENCE_ARG,
                        LanguageFeature.FEATURE_V_1_4_GROUPING_BUILTIN,
                        LanguageFeature.FEATURE_V_1_4_GROUPING_SETS,
                        LanguageFeature.FEATURE_V_1_4_PRESERVE_ANNOTATION_IN_IMPLICIT_CAST_IN_SCAN,
                        LanguageFeature.FEATURE_V_1_4_CORRESPONDING_FULL,
                        LanguageFeature.FEATURE_V_1_4_LIKE_ANY_SOME_ALL_ARRAY,
                        LanguageFeature.FEATURE_V_1_4_LIKE_ANY_SOME_ALL_SUBQUERY,
                        LanguageFeature.FEATURE_V_1_4_FIRST_AND_LAST_N,
                        LanguageFeature.FEATURE_V_1_4_NULLIFZERO_ZEROIFNULL,
                        LanguageFeature.FEATURE_V_1_4_PI_FUNCTIONS,
                        LanguageFeature.FEATURE_V_1_4_CREATE_FUNCTION_LANGUAGE_WITH_CONNECTION,
                        LanguageFeature.FEATURE_V_1_4_SINGLETON_UNNEST_INFERS_ALIAS,
                        LanguageFeature.FEATURE_V_1_4_ARRAY_ZIP,
                        LanguageFeature.FEATURE_V_1_4_MULTIWAY_UNNEST,
                        LanguageFeature.FEATURE_V_1_4_USE_OPERATION_COLLATION_FOR_NULLIF,
                        LanguageFeature.FEATURE_V_1_4_ENABLE_EDIT_DISTANCE_BYTES,
                        LanguageFeature.FEATURE_V_1_4_ENABLE_FLOAT_DISTANCE_FUNCTIONS,
                        LanguageFeature.FEATURE_V_1_4_ENABLE_MEASURES,
                        LanguageFeature.FEATURE_V_1_4_GROUP_BY_ALL,
                        LanguageFeature.FEATURE_V_1_4_ENFORCE_STRICT_MACROS,
                        LanguageFeature.FEATURE_V_1_4_LIMIT_OFFSET_EXPRESSIONS,
                        LanguageFeature.FEATURE_V_1_4_MAP_TYPE,
                        LanguageFeature.FEATURE_V_1_4_DISABLE_FLOAT32,
                        LanguageFeature.FEATURE_V_1_4_LITERAL_CONCATENATION,
                        LanguageFeature.FEATURE_V_1_4_DOT_PRODUCT,
                        LanguageFeature.FEATURE_V_1_4_MANHATTAN_DISTANCE,
                        LanguageFeature.FEATURE_V_1_4_L1_NORM,
                        LanguageFeature.FEATURE_V_1_4_L2_NORM,
                        LanguageFeature.FEATURE_V_1_4_STRUCT_BRACED_CONSTRUCTORS,
                        LanguageFeature.FEATURE_V_1_4_WITH_RECURSIVE_DEPTH_MODIFIER,
                        LanguageFeature.FEATURE_V_1_4_JSON_ARRAY_VALUE_EXTRACTION_FUNCTIONS,
                        LanguageFeature.FEATURE_V_1_4_ENFORCE_CONDITIONAL_EVALUATION,
                        LanguageFeature.FEATURE_V_1_4_JSON_MORE_VALUE_EXTRACTION_FUNCTIONS));

        languageOptions.setSupportedStatementKinds(
                ImmutableSet.of(
                        ResolvedNodeKind.RESOLVED_ADD_CONSTRAINT_ACTION,
                        ResolvedNodeKind.RESOLVED_ALTER_ENTITY_STMT,
                        ResolvedNodeKind.RESOLVED_ALTER_MATERIALIZED_VIEW_STMT,
                        ResolvedNodeKind.RESOLVED_ALTER_MODEL_STMT,
                        ResolvedNodeKind.RESOLVED_ALTER_SCHEMA_STMT,
                        ResolvedNodeKind.RESOLVED_ALTER_TABLE_STMT,
                        ResolvedNodeKind.RESOLVED_ALTER_VIEW_STMT,
                        ResolvedNodeKind.RESOLVED_ASSERT_STMT,
                        ResolvedNodeKind.RESOLVED_ASSIGNMENT_STMT,
                        ResolvedNodeKind.RESOLVED_AUX_LOAD_DATA_STMT,
                        ResolvedNodeKind.RESOLVED_BEGIN_STMT,
                        ResolvedNodeKind.RESOLVED_CALL_STMT,
                        ResolvedNodeKind.RESOLVED_COMMIT_STMT,
                        ResolvedNodeKind.RESOLVED_CREATE_ENTITY_STMT,
                        ResolvedNodeKind.RESOLVED_CREATE_EXTERNAL_TABLE_STMT,
                        ResolvedNodeKind.RESOLVED_CREATE_FUNCTION_STMT,
                        ResolvedNodeKind.RESOLVED_CREATE_INDEX_STMT,
                        ResolvedNodeKind.RESOLVED_CREATE_MATERIALIZED_VIEW_STMT,
                        ResolvedNodeKind.RESOLVED_CREATE_MODEL_STMT,
                        ResolvedNodeKind.RESOLVED_CREATE_PROCEDURE_STMT,
                        ResolvedNodeKind.RESOLVED_CREATE_ROW_ACCESS_POLICY_STMT,
                        ResolvedNodeKind.RESOLVED_CREATE_SCHEMA_STMT,
                        ResolvedNodeKind.RESOLVED_CREATE_SNAPSHOT_TABLE_STMT,
                        ResolvedNodeKind.RESOLVED_CREATE_TABLE_AS_SELECT_STMT,
                        ResolvedNodeKind.RESOLVED_CREATE_TABLE_FUNCTION_STMT,
                        ResolvedNodeKind.RESOLVED_CREATE_TABLE_STMT,
                        ResolvedNodeKind.RESOLVED_CREATE_VIEW_STMT,
                        ResolvedNodeKind.RESOLVED_DELETE_STMT,
                        ResolvedNodeKind.RESOLVED_DROP_CONSTRAINT_ACTION,
                        ResolvedNodeKind.RESOLVED_DROP_FUNCTION_STMT,
                        ResolvedNodeKind.RESOLVED_DROP_MATERIALIZED_VIEW_STMT,
                        ResolvedNodeKind.RESOLVED_DROP_ROW_ACCESS_POLICY_STMT,
                        ResolvedNodeKind.RESOLVED_DROP_SNAPSHOT_TABLE_STMT,
                        ResolvedNodeKind.RESOLVED_DROP_STMT,
                        ResolvedNodeKind.RESOLVED_DROP_TABLE_FUNCTION_STMT,
                        ResolvedNodeKind.RESOLVED_EXECUTE_IMMEDIATE_STMT,
                        ResolvedNodeKind.RESOLVED_EXPORT_DATA_STMT,
                        ResolvedNodeKind.RESOLVED_EXPORT_MODEL_STMT,
                        ResolvedNodeKind.RESOLVED_GRANT_STMT,
                        ResolvedNodeKind.RESOLVED_INSERT_STMT,
                        ResolvedNodeKind.RESOLVED_MERGE_STMT,
                        ResolvedNodeKind.RESOLVED_QUERY_STMT,
                        ResolvedNodeKind.RESOLVED_REVOKE_STMT,
                        ResolvedNodeKind.RESOLVED_ROLLBACK_STMT,
                        ResolvedNodeKind.RESOLVED_TRUNCATE_STMT,
                        ResolvedNodeKind.RESOLVED_UPDATE_STMT));

        languageOptions.enableReservableKeyword("QUALIFY");
    }

    public static LanguageOptions get() {
        return languageOptions;
    }
}
