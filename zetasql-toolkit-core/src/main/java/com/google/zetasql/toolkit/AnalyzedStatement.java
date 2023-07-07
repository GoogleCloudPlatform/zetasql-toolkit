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

import com.google.zetasql.parser.ASTNodes.ASTStatement;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import java.util.Optional;

public class AnalyzedStatement {

  private final ASTStatement parsedStatement;
  private final Optional<ResolvedStatement> resolvedStatement;

  public AnalyzedStatement(
      ASTStatement parsedStatement, Optional<ResolvedStatement> resolvedStatement) {
    this.parsedStatement = parsedStatement;
    this.resolvedStatement = resolvedStatement;
  }

  public ASTStatement getParsedStatement() {
    return parsedStatement;
  }

  public Optional<ResolvedStatement> getResolvedStatement() {
    return resolvedStatement;
  }

}
