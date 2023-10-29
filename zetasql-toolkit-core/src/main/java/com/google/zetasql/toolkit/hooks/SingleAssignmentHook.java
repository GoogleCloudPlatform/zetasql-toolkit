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

package com.google.zetasql.toolkit.hooks;

import com.google.common.collect.ImmutableList;
import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.Constant;
import com.google.zetasql.NotFoundException;
import com.google.zetasql.parser.ASTNodes.ASTExpression;
import com.google.zetasql.parser.ASTNodes.ASTSingleAssignment;
import com.google.zetasql.parser.ASTNodes.ASTStatement;
import com.google.zetasql.parser.ASTNodes.ASTVariableDeclaration;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.toolkit.AnalysisException;
import com.google.zetasql.toolkit.AnalyzerExtensions;
import com.google.zetasql.toolkit.Coercer;
import com.google.zetasql.toolkit.catalog.CatalogWrapper;

/**
 * The SingleAssignmentHook is a {@link PreAnalysisHook} which validates single assignments
 * (i.e. SET statements) are valid.
 *
 * <p> This hook will fail if the variable assigned to does not exist or if the expression type
 * is not compatible with the variable type.
 */
public class SingleAssignmentHook implements PreAnalysisHook {

  @Override
  public void run(
      String query,
      ASTStatement statement,
      CatalogWrapper catalog,
      AnalyzerOptions analyzerOptions) throws AnalysisException {

    if (!(statement instanceof ASTSingleAssignment)){
      return;
    }

    ASTSingleAssignment singleAssignment = (ASTSingleAssignment) statement;
    Coercer coercer = new Coercer(analyzerOptions.getLanguageOptions());

    String assignmentTarget = singleAssignment.getVariable().getIdString();
    ASTExpression expression = singleAssignment.getExpression();

    ResolvedExpr analyzedExpression = AnalyzerExtensions.analyzeExpression(
        query, expression, analyzerOptions, catalog.getZetaSQLCatalog());

    try {
      Constant constant = catalog.getZetaSQLCatalog()
          .findConstant(ImmutableList.of(assignmentTarget));
      boolean coerces = coercer.expressionCoercesTo(analyzedExpression, constant.getType());

      if (!coerces) {
        String message = String.format(
            "Cannot coerce expression of type %s to type %s",
            analyzedExpression.getType(), constant.getType());
        throw new AnalysisException(message);
      }

    } catch (NotFoundException e) {
      throw new AnalysisException("Undeclared variable: " + assignmentTarget);
    }
  }
}
