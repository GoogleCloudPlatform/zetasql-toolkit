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

package com.google.zetasql.toolkit.tools.lineage;

import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedColumnRef;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.Visitor;
import java.util.ArrayList;
import java.util.List;

class ColumnReferenceExtractor extends Visitor {
  // TODO: Handle different types of expressions. E.g. special cases for functions,
  //  subqueries, etc.

  private final ArrayList<ResolvedColumn> result = new ArrayList<>();

  public static List<ResolvedColumn> extractFromExpression(ResolvedExpr expression) {
    ColumnReferenceExtractor extractor = new ColumnReferenceExtractor();
    expression.accept(extractor);
    return extractor.result;
  }

  public void visit(ResolvedColumnRef columnRef) {
    result.add(columnRef.getColumn());
  }

  private ColumnReferenceExtractor() {}

}
