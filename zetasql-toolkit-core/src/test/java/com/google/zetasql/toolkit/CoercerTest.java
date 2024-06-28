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

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.ImmutableList;
import com.google.zetasql.LanguageOptions;
import com.google.zetasql.StructType.StructField;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.ZetaSQLOptions.LanguageFeature;
import com.google.zetasql.ZetaSQLType.TypeKind;
import org.junit.jupiter.api.Test;

public class CoercerTest {

  @Test
  void testBasicCoercions() {
    Type int32Type = TypeFactory.createSimpleType(TypeKind.TYPE_INT32);
    Type int64Type = TypeFactory.createSimpleType(TypeKind.TYPE_INT64);
    Type doubleType = TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE);
    Type stringType = TypeFactory.createSimpleType(TypeKind.TYPE_STRING);
    Type timestampType = TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP);

    LanguageOptions languageOptions = new LanguageOptions();
    languageOptions.enableMaximumLanguageFeatures();

    Coercer coercer = new Coercer(languageOptions);

    assertTrue(
        coercer.coercesTo(int32Type, int64Type, false, false), "Expect INT32 to coerce to INT64");
    assertTrue(
        coercer.coercesTo(int32Type, doubleType, false, false), "Expect INT32 to coerce to DOUBLE");
    assertFalse(
        coercer.coercesTo(int32Type, stringType, false, false),
        "Expect INT32 to not coerce to STRING");
    assertFalse(
        coercer.coercesTo(int64Type, int32Type, false, false),
        "Expect INT64 to not coerce to INT32");
    assertTrue(
        coercer.coercesTo(int64Type, int32Type, true, false),
        "Expect INT64 literals to coerce to INT32");
    assertFalse(
        coercer.coercesTo(stringType, timestampType, false, false),
        "Expect STRING to not coerce to TIMESTAMP");
    assertTrue(
        coercer.coercesTo(stringType, timestampType, true, false),
        "Expect STRING literals to coerce to TIMESTAMP");
    assertTrue(
        coercer.coercesTo(stringType, timestampType, false, true),
        "Expect STRING parameters to coerce to TIMESTAMP");
  }

  @Test
  void testArrayCoercions() {
    Type int32Array =
        TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32));
    Type int64Array =
        TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT64));

    LanguageOptions languageOptions = new LanguageOptions();

    Coercer coercer = new Coercer(languageOptions);

    assertFalse(
        coercer.coercesTo(int32Array, int64Array, true, false),
        "Expect ARRAY<INT32> literal to not coerce to ARRAY<INT64> when "
            + "FEATURE_V_1_1_CAST_DIFFERENT_ARRAY_TYPES is not enabled");

    languageOptions.enableLanguageFeature(LanguageFeature.FEATURE_V_1_1_CAST_DIFFERENT_ARRAY_TYPES);

    assertTrue(
        coercer.coercesTo(int32Array, int64Array, true, false),
        "Expect ARRAY<INT32> literal to coerce to ARRAY<INT64> when "
            + "FEATURE_V_1_1_CAST_DIFFERENT_ARRAY_TYPES is enabled");

    assertFalse(
        coercer.coercesTo(int32Array, int64Array, false, false),
        "Expect non-literal ARRAY<INT32> to not coerce to ARRAY<INT64>");
  }

  @Test
  void testStructCoercions() {
    Type structWithInt32Field =
        TypeFactory.createStructType(
            ImmutableList.of(
                new StructField("field", TypeFactory.createSimpleType(TypeKind.TYPE_INT32))));
    Type structWithInt64Field =
        TypeFactory.createStructType(
            ImmutableList.of(
                new StructField("field", TypeFactory.createSimpleType(TypeKind.TYPE_INT64))));

    LanguageOptions languageOptions = new LanguageOptions();
    languageOptions.enableMaximumLanguageFeatures();

    Coercer coercer = new Coercer(languageOptions);

    assertTrue(
        coercer.coercesTo(structWithInt32Field, structWithInt64Field, false, false),
        "Expect compatible STRUCTs to coerce to one another");
  }

  @Test
  void testComplexCoercions() {
    Type int32StructArray =
        TypeFactory.createArrayType(
            TypeFactory.createStructType(
                ImmutableList.of(
                    new StructField("field", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)))));
    Type int64StructArray =
        TypeFactory.createArrayType(
            TypeFactory.createStructType(
                ImmutableList.of(
                    new StructField("field", TypeFactory.createSimpleType(TypeKind.TYPE_INT64)))));

    LanguageOptions languageOptions = new LanguageOptions();
    languageOptions.enableMaximumLanguageFeatures();

    Coercer coercer = new Coercer(languageOptions);

    assertTrue(
        coercer.coercesTo(int32StructArray, int64StructArray, true, false),
        "Expect compatible ARRAY<STRUCT<...>> literals to coerce to one another");
  }
}
