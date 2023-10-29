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

package com.google.zetasql.toolkit;

import com.google.common.collect.ImmutableList;
import com.google.zetasql.ArrayType;
import com.google.zetasql.LanguageOptions;
import com.google.zetasql.StructType;
import com.google.zetasql.Type;
import com.google.zetasql.ZetaSQLOptions.LanguageFeature;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedLiteral;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedParameter;
import java.util.Optional;

/**
 * Basic (and not complete) implementation of a type coercer for ZetaSQL, based on the
 * <a href="https://github.com/google/zetasql/blob/master/zetasql/public/coercer.cc">C++ Coercer</a>
 *
 * <p> This is not a complete implementation for ZetaSQL; since it doesn't properly implement
 * coercion for protos, enums and other complex types. However, it should suffice for the
 * SQL engines the ZetaSQL toolkit is targeting (primarily BigQuery and Cloud Spanner).
 */
public class Coercer {

  private final LanguageOptions languageOptions;

  public Coercer(LanguageOptions languageOptions) {
    this.languageOptions = languageOptions;
  }

  /**
   * List of supported type coercions in ZetaSQL.
   *
   * <p> Each {@link TypeCoercion} specifies a supported type coercion from one type
   * (the "from" type) to another (the "to" type). Each coercion has a {@link CoercionMode}
   * associated to it.
   *
   * <p> For example:
   *
   * <ul>
   *   <li> BOOL implicitly coerces to BOOL (any type coerces implicitly to itself)
   *   <li> INT32 implicitly coerces to INT64 and DOUBLE
   *   <li> INT32 explicitly coerces to STRING (it needs to be casted)
   * </ul>
   */
  private static final ImmutableList<TypeCoercion> supportedTypeCoercions = ImmutableList.of(
      new TypeCoercion(/*from=*/TypeKind.TYPE_BOOL, /*to=*/TypeKind.TYPE_BOOL, CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BOOL, /*to=*/TypeKind.TYPE_INT32, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BOOL, /*to=*/TypeKind.TYPE_INT64, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BOOL, /*to=*/TypeKind.TYPE_UINT32, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BOOL, /*to=*/TypeKind.TYPE_UINT64, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BOOL, /*to=*/TypeKind.TYPE_STRING, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT32, /*to=*/TypeKind.TYPE_BOOL, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT32, /*to=*/TypeKind.TYPE_INT32, CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT32, /*to=*/TypeKind.TYPE_INT64, CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT32, /*to=*/TypeKind.TYPE_UINT32,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT32, /*to=*/TypeKind.TYPE_UINT64,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT32, /*to=*/TypeKind.TYPE_FLOAT,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT32, /*to=*/TypeKind.TYPE_DOUBLE,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT32, /*to=*/TypeKind.TYPE_STRING,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT32, /*to=*/TypeKind.TYPE_ENUM,
          CoercionMode.EXPLICIT_OR_LITERAL_OR_PARAMETER),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT32, /*to=*/TypeKind.TYPE_NUMERIC,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT32, /*to=*/TypeKind.TYPE_BIGNUMERIC,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT64, /*to=*/TypeKind.TYPE_BOOL, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT64, /*to=*/TypeKind.TYPE_INT32,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT64, /*to=*/TypeKind.TYPE_INT64, CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT64, /*to=*/TypeKind.TYPE_UINT32,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT64, /*to=*/TypeKind.TYPE_UINT64,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT64, /*to=*/TypeKind.TYPE_FLOAT,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT64, /*to=*/TypeKind.TYPE_DOUBLE,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT64, /*to=*/TypeKind.TYPE_STRING,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT64, /*to=*/TypeKind.TYPE_ENUM,
          CoercionMode.EXPLICIT_OR_LITERAL_OR_PARAMETER),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT64, /*to=*/TypeKind.TYPE_NUMERIC,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INT64, /*to=*/TypeKind.TYPE_BIGNUMERIC,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT32, /*to=*/TypeKind.TYPE_BOOL, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT32, /*to=*/TypeKind.TYPE_INT32,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT32, /*to=*/TypeKind.TYPE_INT64,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT32, /*to=*/TypeKind.TYPE_UINT32,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT32, /*to=*/TypeKind.TYPE_UINT64,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT32, /*to=*/TypeKind.TYPE_FLOAT,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT32, /*to=*/TypeKind.TYPE_DOUBLE,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT32, /*to=*/TypeKind.TYPE_STRING,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT32, /*to=*/TypeKind.TYPE_ENUM,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT32, /*to=*/TypeKind.TYPE_NUMERIC,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT32, /*to=*/TypeKind.TYPE_BIGNUMERIC,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT64, /*to=*/TypeKind.TYPE_BOOL, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT64, /*to=*/TypeKind.TYPE_INT32,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT64, /*to=*/TypeKind.TYPE_INT64,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT64, /*to=*/TypeKind.TYPE_UINT32,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT64, /*to=*/TypeKind.TYPE_UINT64,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT64, /*to=*/TypeKind.TYPE_FLOAT,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT64, /*to=*/TypeKind.TYPE_DOUBLE,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT64, /*to=*/TypeKind.TYPE_STRING,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT64, /*to=*/TypeKind.TYPE_ENUM,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT64, /*to=*/TypeKind.TYPE_NUMERIC,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_UINT64, /*to=*/TypeKind.TYPE_BIGNUMERIC,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_NUMERIC, /*to=*/TypeKind.TYPE_INT32,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_NUMERIC, /*to=*/TypeKind.TYPE_INT64,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_NUMERIC, /*to=*/TypeKind.TYPE_UINT32,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_NUMERIC, /*to=*/TypeKind.TYPE_UINT64,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_NUMERIC, /*to=*/TypeKind.TYPE_FLOAT,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_NUMERIC, /*to=*/TypeKind.TYPE_DOUBLE,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_NUMERIC, /*to=*/TypeKind.TYPE_STRING,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_NUMERIC, /*to=*/TypeKind.TYPE_NUMERIC,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_NUMERIC, /*to=*/TypeKind.TYPE_BIGNUMERIC,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BIGNUMERIC, /*to=*/TypeKind.TYPE_INT32,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BIGNUMERIC, /*to=*/TypeKind.TYPE_INT64,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BIGNUMERIC, /*to=*/TypeKind.TYPE_UINT32,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BIGNUMERIC, /*to=*/TypeKind.TYPE_UINT64,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BIGNUMERIC, /*to=*/TypeKind.TYPE_FLOAT,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BIGNUMERIC, /*to=*/TypeKind.TYPE_DOUBLE,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BIGNUMERIC, /*to=*/TypeKind.TYPE_STRING,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BIGNUMERIC, /*to=*/TypeKind.TYPE_NUMERIC,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BIGNUMERIC, /*to=*/TypeKind.TYPE_BIGNUMERIC,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_FLOAT, /*to=*/TypeKind.TYPE_INT32, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_FLOAT, /*to=*/TypeKind.TYPE_INT64, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_FLOAT, /*to=*/TypeKind.TYPE_UINT32,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_FLOAT, /*to=*/TypeKind.TYPE_UINT64,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_FLOAT, /*to=*/TypeKind.TYPE_FLOAT, CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_FLOAT, /*to=*/TypeKind.TYPE_DOUBLE,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_FLOAT, /*to=*/TypeKind.TYPE_STRING,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_FLOAT, /*to=*/TypeKind.TYPE_NUMERIC,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_FLOAT, /*to=*/TypeKind.TYPE_BIGNUMERIC,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DOUBLE, /*to=*/TypeKind.TYPE_INT32,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DOUBLE, /*to=*/TypeKind.TYPE_INT64,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DOUBLE, /*to=*/TypeKind.TYPE_UINT32,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DOUBLE, /*to=*/TypeKind.TYPE_UINT64,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DOUBLE, /*to=*/TypeKind.TYPE_FLOAT,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DOUBLE, /*to=*/TypeKind.TYPE_DOUBLE,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DOUBLE, /*to=*/TypeKind.TYPE_STRING,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DOUBLE, /*to=*/TypeKind.TYPE_NUMERIC,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DOUBLE, /*to=*/TypeKind.TYPE_BIGNUMERIC,
          CoercionMode.EXPLICIT_OR_LITERAL),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_INT32,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_INT64,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_UINT32,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_UINT64,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_FLOAT,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_DOUBLE,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_STRING,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_BYTES,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_DATE,
          CoercionMode.EXPLICIT_OR_LITERAL_OR_PARAMETER),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_TIMESTAMP,
          CoercionMode.EXPLICIT_OR_LITERAL_OR_PARAMETER),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_TIME,
          CoercionMode.EXPLICIT_OR_LITERAL_OR_PARAMETER),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_DATETIME,
          CoercionMode.EXPLICIT_OR_LITERAL_OR_PARAMETER),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_INTERVAL,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_ENUM,
          CoercionMode.EXPLICIT_OR_LITERAL_OR_PARAMETER),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_PROTO,
          CoercionMode.EXPLICIT_OR_LITERAL_OR_PARAMETER),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_BOOL, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_NUMERIC,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_BIGNUMERIC,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_STRING, /*to=*/TypeKind.TYPE_RANGE,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BYTES, /*to=*/TypeKind.TYPE_BYTES, CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BYTES, /*to=*/TypeKind.TYPE_STRING,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_BYTES, /*to=*/TypeKind.TYPE_PROTO,
          CoercionMode.EXPLICIT_OR_LITERAL_OR_PARAMETER),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DATE, /*to=*/TypeKind.TYPE_DATE, CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DATE, /*to=*/TypeKind.TYPE_DATETIME,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DATE, /*to=*/TypeKind.TYPE_TIMESTAMP,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DATE, /*to=*/TypeKind.TYPE_STRING, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_TIMESTAMP, /*to=*/TypeKind.TYPE_DATE,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_TIMESTAMP, /*to=*/TypeKind.TYPE_DATETIME,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_TIMESTAMP, /*to=*/TypeKind.TYPE_TIME,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_TIMESTAMP, /*to=*/TypeKind.TYPE_TIMESTAMP,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_TIMESTAMP, /*to=*/TypeKind.TYPE_STRING,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_TIME, /*to=*/TypeKind.TYPE_TIME, CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_TIME, /*to=*/TypeKind.TYPE_STRING, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DATETIME, /*to=*/TypeKind.TYPE_DATE,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DATETIME, /*to=*/TypeKind.TYPE_DATETIME,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DATETIME, /*to=*/TypeKind.TYPE_STRING,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DATETIME, /*to=*/TypeKind.TYPE_TIME,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_DATETIME, /*to=*/TypeKind.TYPE_TIMESTAMP,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INTERVAL, /*to=*/TypeKind.TYPE_INTERVAL,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_INTERVAL, /*to=*/TypeKind.TYPE_STRING,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_GEOGRAPHY, /*to=*/TypeKind.TYPE_GEOGRAPHY,
          CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_JSON, /*to=*/TypeKind.TYPE_JSON, CoercionMode.IMPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_ENUM, /*to=*/TypeKind.TYPE_STRING, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_ENUM, /*to=*/TypeKind.TYPE_INT32, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_ENUM, /*to=*/TypeKind.TYPE_INT64, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_ENUM, /*to=*/TypeKind.TYPE_UINT32, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_ENUM, /*to=*/TypeKind.TYPE_UINT64, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_PROTO, /*to=*/TypeKind.TYPE_STRING,
          CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_PROTO, /*to=*/TypeKind.TYPE_BYTES, CoercionMode.EXPLICIT),
      new TypeCoercion(/*from=*/TypeKind.TYPE_RANGE, /*to=*/TypeKind.TYPE_STRING, CoercionMode.EXPLICIT)
  );

  private enum CoercionMode {
    IMPLICIT,
    EXPLICIT,
    EXPLICIT_OR_LITERAL,
    EXPLICIT_OR_LITERAL_OR_PARAMETER;
  }

  private static class TypeCoercion {

    public final TypeKind fromKind;
    public final TypeKind toKind;
    public final CoercionMode coercionMode;

    public TypeCoercion(TypeKind fromKind, TypeKind toKind, CoercionMode coercionMode) {
      this.fromKind = fromKind;
      this.toKind = toKind;
      this.coercionMode = coercionMode;
    }
  }

  /**
   * Returns whether fromType can coerce to toType
   * 
   * @param fromType The type to coerce from
   * @param toType The type to coerce to
   * @param isLiteral Whether the expression that's being coerced is a literal
   * @param isParameter Whether the expression that's being coerced is a parameter
   *
   * @return Whether fromType can coerce to toType
   */
  public boolean coercesTo(
      Type fromType, Type toType,
      boolean isLiteral, boolean isParameter
  ) {
    if (fromType.isStruct()) {
      return structCoercesTo((StructType) fromType, toType, isLiteral, isParameter);
    }

    if (fromType.isArray()) {
      return arrayCoercesTo((ArrayType) fromType, toType, isLiteral, isParameter);
    }

    if (!fromType.isSimpleType() && !toType.isSimpleType()) {
      return fromType.equivalent(toType);
    }

    TypeKind fromKind = fromType.getKind();
    TypeKind toKind = toType.getKind();

    Optional<CoercionMode> maybeCoercionMode = supportedTypeCoercions.stream()
        .filter(typeCoercion ->
            typeCoercion.fromKind.equals(fromKind) && typeCoercion.toKind.equals(toKind))
        .map(typeCoercion -> typeCoercion.coercionMode)
        .findFirst();

    if(!maybeCoercionMode.isPresent()) {
      return false;
    }

    CoercionMode coercionMode = maybeCoercionMode.get();

    if (isLiteral && supportsLiteralCoercion(coercionMode)) {
      return true;
    }

    if (isParameter && supportsParameterCoercion(coercionMode)) {
      return true;
    }

    return supportsImplicitCoercion(coercionMode);
  }

  /**
   * @see Coercer#coercesTo(Type, Type, boolean, boolean)
   */
  public boolean structCoercesTo(
      StructType fromType, Type toType,
      boolean isLiteral, boolean isParameter
  ) {
    if (!toType.isStruct() || toType.asStruct().getFieldCount() != fromType.getFieldCount()) {
      return false;
    }

    StructType toTypeAsStruct = toType.asStruct();

    for (int i = 0; i < fromType.getFieldCount(); i++) {
      Type fromTypeFieldType = fromType.getField(i).getType();
      Type toTypeFieldType = toTypeAsStruct.getField(i).getType();
      if (!coercesTo(fromTypeFieldType, toTypeFieldType, isLiteral, isParameter)) {
        return false;
      }
    }

    return true;
  }

  /**
   * @see Coercer#coercesTo(Type, Type, boolean, boolean)
   */
  public boolean arrayCoercesTo(
      ArrayType fromType, Type toType,
      boolean isLiteral, boolean isParameter
  ) {
    if (fromType.equivalent(toType)) {
      return true;
    }

    if (!languageOptions.languageFeatureEnabled(
        LanguageFeature.FEATURE_V_1_1_CAST_DIFFERENT_ARRAY_TYPES)) {
      return false;
    }

    if (!toType.isArray()) {
      return false;
    }

    if (isLiteral || isParameter) {
      return coercesTo(
          fromType.getElementType(),
          toType.asArray().getElementType(),
          isLiteral,
          isParameter);
    }

    return false;
  }

  public boolean expressionCoercesTo(ResolvedExpr resolvedExpr, Type type) {
    Type expressionType = resolvedExpr.getType();
    boolean isLiteral = resolvedExpr instanceof ResolvedLiteral;
    boolean isParameter = resolvedExpr instanceof ResolvedParameter;

    return coercesTo(expressionType, type, isLiteral, isParameter);
  }

  public boolean supportsImplicitCoercion(CoercionMode mode) {
    return mode.equals(CoercionMode.IMPLICIT);
  }

  public boolean supportsLiteralCoercion(CoercionMode mode) {
    return mode.equals(CoercionMode.IMPLICIT)
        || mode.equals(CoercionMode.EXPLICIT_OR_LITERAL)
        || mode.equals(CoercionMode.EXPLICIT_OR_LITERAL_OR_PARAMETER);
  }

  public boolean supportsParameterCoercion(CoercionMode mode) {
    return mode.equals(CoercionMode.IMPLICIT)
        || mode.equals(CoercionMode.EXPLICIT_OR_LITERAL_OR_PARAMETER);
  }

}
