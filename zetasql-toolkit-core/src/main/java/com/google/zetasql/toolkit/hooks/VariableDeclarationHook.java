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
import com.google.zetasql.SimpleConstantProtos.SimpleConstantProto;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.parser.ASTNodes.ASTIdentifier;
import com.google.zetasql.parser.ASTNodes.ASTStatement;
import com.google.zetasql.parser.ASTNodes.ASTVariableDeclaration;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.toolkit.AnalysisException;
import com.google.zetasql.toolkit.AnalyzerExtensions;
import com.google.zetasql.toolkit.Coercer;
import com.google.zetasql.toolkit.catalog.CatalogWrapper;
import com.google.zetasql.toolkit.catalog.typeparser.ZetaSQLTypeParser;
import java.util.Optional;

/**
 * The VariableDeclarationHook is a {@link PreAnalysisHook} which creates variables, as defined by
 * an {@link ASTVariableDeclaration} statement, in the catalog.
 *
 * <p> This hook will fail if the variable declaration has a default value which is incompatible
 * with the variable type.
 */
public class VariableDeclarationHook implements PreAnalysisHook {

  private Optional<Type> getDeclaredType(String query, ASTVariableDeclaration declaration) {
    return Optional.ofNullable(declaration.getType())
        .map(astType -> query.substring(
            astType.getParseLocationRange().start(),
            astType.getParseLocationRange().end()
        ))
        .map(ZetaSQLTypeParser::parse);
  }

  private Optional<ResolvedExpr> getDefaultValueExpr(
      String query,
      ASTVariableDeclaration declaration,
      CatalogWrapper catalog,
      AnalyzerOptions analyzerOptions)
  {
    return Optional.ofNullable(declaration.getDefaultValue())
        .map(expression -> AnalyzerExtensions.analyzeExpression(
            query, expression, analyzerOptions, catalog.getZetaSQLCatalog()));
  }

  private Constant buildConstant(String name, Type type) {
    SimpleConstantProto proto = SimpleConstantProto.newBuilder()
        .addNamePath(name)
        .setType(type.serialize())
        .build();

    return Constant.deserialize(
        proto,
        ImmutableList.of(),
        TypeFactory.nonUniqueNames());
  }

  private void addVariableToCatalog(
      ASTVariableDeclaration declaration,
      Type variableType,
      CatalogWrapper catalog) {

    declaration.getVariableList()
        .getIdentifierList()
        .stream()
        .map(ASTIdentifier::getIdString)
        .map(variableName -> buildConstant(variableName, variableType))
        .forEach(catalog::register);
  }

  @Override
  public void run(
      String query,
      ASTStatement statement,
      CatalogWrapper catalog,
      AnalyzerOptions analyzerOptions) throws AnalysisException {

    if (!(statement instanceof ASTVariableDeclaration)) {
      return;
    }

    ASTVariableDeclaration declaration = (ASTVariableDeclaration) statement;

    Optional<Type> explicitType = getDeclaredType(query, declaration);
    Optional<ResolvedExpr> defaultValueExpr = getDefaultValueExpr(
        query, declaration, catalog, analyzerOptions);

    Coercer coercer = new Coercer(analyzerOptions.getLanguageOptions());

    if (!explicitType.isPresent() && !defaultValueExpr.isPresent()) {
      // Should not happen, since this is enforced by the parser
      throw new AnalysisException(
          "Either the type or the default value must be present for variable declarations");
    }

    if (explicitType.isPresent()
        && defaultValueExpr.isPresent()
        && !coercer.expressionCoercesTo(defaultValueExpr.get(), explicitType.get())) {

      String message = String.format(
          "Cannot coerce expression of type %s to type %s",
          defaultValueExpr.get().getType(), explicitType.get());
      throw new AnalysisException(message);
    }

    Type variableType = explicitType.orElseGet(() -> defaultValueExpr.get().getType());

    addVariableToCatalog(declaration, variableType, catalog);
  }
}
