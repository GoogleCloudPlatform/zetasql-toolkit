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

package com.google.zetasql.toolkit.tools.patch;

import static org.junit.jupiter.api.Assertions.*;

import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.Parser;
import com.google.zetasql.toolkit.ZetaSQLToolkitAnalyzer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ZetaSQLPatcherTest {

  private ZetaSQLToolkitAnalyzer analyzer;

  @BeforeEach
  void init() {
    AnalyzerOptions analyzerOptions = new AnalyzerOptions();
    analyzerOptions.getLanguageOptions().enableMaximumLanguageFeatures();
    analyzerOptions.getLanguageOptions().setSupportsAllStatementKinds();

    this.analyzer = new ZetaSQLToolkitAnalyzer(analyzerOptions);
  }

  public static String generateNestedSelectStatement(int times) {
    if (times == 1) {
      return "SELECT 1";
    }

    return String.format("SELECT 1 FROM (%s)", generateNestedSelectStatement(times - 1));
  }
  @Test
  void testMaxNestingDepthPatch() {
    // The query is a SELECT statement nested 100 times. Parsing or analyzing
    // such a statement normally fails due to reaching the default max nesting
    // depth in the ZetaSQL local service.
    // After patching the max nesting depth, it should not fail.
    String query = generateNestedSelectStatement(100);

    try {
      ZetaSQLPatcher.patchMaxProtobufNestingDepth(1000);
    } catch (IllegalAccessException err) {
      Assumptions.abort("Aborting test because the patch was not applied successfully due to "
          + "disallowed reflective access.");
    }

    assertDoesNotThrow(() -> {
      this.analyzer.analyzeStatements(query).next();
    });
  }

}
