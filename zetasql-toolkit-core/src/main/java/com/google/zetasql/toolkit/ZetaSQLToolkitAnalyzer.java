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
import com.google.zetasql.Analyzer;
import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.Constant;
import com.google.zetasql.LanguageOptions;
import com.google.zetasql.NotFoundException;
import com.google.zetasql.ParseResumeLocation;
import com.google.zetasql.Parser;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.SimpleConstantProtos.SimpleConstantProto;
import com.google.zetasql.SqlException;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.ZetaSQLType.TypeProto;
import com.google.zetasql.parser.ASTNodes.ASTExpression;
import com.google.zetasql.parser.ASTNodes.ASTIdentifier;
import com.google.zetasql.parser.ASTNodes.ASTScriptStatement;
import com.google.zetasql.parser.ASTNodes.ASTSingleAssignment;
import com.google.zetasql.parser.ASTNodes.ASTStatement;
import com.google.zetasql.parser.ASTNodes.ASTType;
import com.google.zetasql.parser.ASTNodes.ASTVariableDeclaration;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import com.google.zetasql.toolkit.catalog.CatalogWrapper;
import com.google.zetasql.toolkit.catalog.basic.BasicCatalogWrapper;
import com.google.zetasql.toolkit.catalog.typeparser.ZetaSQLTypeParser;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Primary class exposed by the ZetaSQL Toolkit to perform SQL analysis.
 *
 * <p>It exposes methods to analyze statements using an empty catalog, an existing {@link
 * SimpleCatalog} or a {@link CatalogWrapper} implementation (such as the BigQueryCatalog or
 * the SpannerCatalog).
 *
 * <p>When analyzing statements that create resources (e.g. a CREATE TEMP TABLE statement), this
 * class will also persist those resources to the catalog. This allows it to transparently support
 * SQL scripts that, for example, create a temp table and later query said temp table. This feature
 * supports Tables, Views, Functions, Table Valued Functions and Procedures.
 */
public class ZetaSQLToolkitAnalyzer {

  private final AnalyzerOptions analyzerOptions;

  /**
   * Constructs a ZetaSQLToolkitAnalyzer using the provided {@link AnalyzerOptions}
   *
   * @param analyzerOptions The AnalyzerOptions that should be used when performing analysis
   */
  public ZetaSQLToolkitAnalyzer(AnalyzerOptions analyzerOptions) {
    this.analyzerOptions = analyzerOptions;
  }

  /**
   * Analyze a SQL query or script, starting with an empty catalog.
   *
   * <p>This method uses the {@link BasicCatalogWrapper} for maintaining the catalog. To follow the
   * semantics of a particular SQL engine (e.g. BigQuery or Spanner),
   *
   * @param query The SQL query or script to analyze
   * @return An iterator of the resulting {@link ResolvedStatement}s
   */
  public Iterator<AnalyzedStatement> analyzeStatements(String query) {
    return this.analyzeStatements(query, new BasicCatalogWrapper());
  }

  /**
   * Analyze a SQL query or script, using the provided {@link CatalogWrapper} to manage the catalog.
   * Creates a copy of the catalog before analyzing to avoid mutating the provided catalog.
   *
   * @param query The SQL query or script to analyze
   * @param catalog The CatalogWrapper implementation to use when managing the catalog
   * @return An iterator of the resulting {@link ResolvedStatement}s
   */
  public Iterator<AnalyzedStatement> analyzeStatements(String query, CatalogWrapper catalog) {
    return this.analyzeStatements(query, catalog, false);
  }

  /**
   * Analyze a SQL query or script, using the provided {@link CatalogWrapper} to manage the catalog.
   *
   * <p>This toolkit includes two implementations, the BigQueryCatalog and the SpannerCatalog;
   * which can be used to run the analyzer following BigQuery or Spanner catalog
   * semantics respectively. For other use-cases, you can provide your own CatalogWrapper
   * implementation.
   *
   * @param query The SQL query or script to analyze
   * @param catalog The CatalogWrapper implementation to use when managing the catalog
   * @param inPlace Whether to apply catalog mutations in place. If true, catalog mutations from
   *     CREATE or DROP statements are applied to the provided catalog. If false, the provided
   *     catalog is copied and the copy is used.
   * @return An iterator of the resulting {@link ResolvedStatement}s
   */
  public Iterator<AnalyzedStatement> analyzeStatements(
      String query, CatalogWrapper catalog, boolean inPlace) {

    CatalogWrapper catalogForAnalysis = inPlace ? catalog : catalog.copy();

    ParseResumeLocation parseResumeLocation = new ParseResumeLocation(query);
    CatalogUpdaterVisitor catalogUpdaterVisitor = new CatalogUpdaterVisitor(catalogForAnalysis);

    return new Iterator<>() {

      private boolean isAnalyzableStatement(ASTStatement parsedStatement) {
        return !(parsedStatement instanceof ASTScriptStatement);
      }

      private ResolvedExpr analyzeExpression(ASTExpression expression) {
        String expressionSource = query.substring(
            expression.getParseLocationRange().start(),
            expression.getParseLocationRange().end()
        );
        return Analyzer.analyzeExpression(
            expressionSource, analyzerOptions, catalogForAnalysis.getZetaSQLCatalog());
      }

      private TypeProto astTypeToTypeProto(ASTType astType) {
        String typeString = query.substring(
            astType.getParseLocationRange().start(),
            astType.getParseLocationRange().end()
        );
        Type type = ZetaSQLTypeParser.parse(typeString);
        return type.serialize();
      }

      private void applyVariableDeclaration(ASTVariableDeclaration declaration) {
        TypeProto constantType;

        if (declaration.getType() != null) {
          constantType = this.astTypeToTypeProto(declaration.getType());
        } else {
          ASTExpression expression = declaration.getDefaultValue();
          ResolvedExpr analyzedExpression = this.analyzeExpression(expression);
          constantType = analyzedExpression.getType().serialize();
        }

        List<Constant> constants = declaration.getVariableList()
            .getIdentifierList()
            .stream()
            .map(ASTIdentifier::getIdString)
            .map(variableName -> SimpleConstantProto.newBuilder()
                .addNamePath(variableName)
                .setType(constantType)
                .build())
            .map(constantProto -> Constant.deserialize(
                constantProto, ImmutableList.of(), TypeFactory.nonUniqueNames()))
            .collect(Collectors.toList());

        // TODO: Add constants to the catalog properly
        constants.forEach(catalogForAnalysis.getZetaSQLCatalog()::addConstant);
      }

      private void applyCatalogMutation(ResolvedStatement statement) {
        statement.accept(catalogUpdaterVisitor);
      }

      @Override
      public boolean hasNext() {
        int inputLength = parseResumeLocation.getInput().getBytes().length;
        int currentPosition = parseResumeLocation.getBytePosition();
        return inputLength > currentPosition;
      }

      @Override
      public AnalyzedStatement next() {
        int startLocation = parseResumeLocation.getBytePosition();
        LanguageOptions languageOptions = analyzerOptions.getLanguageOptions();

        ASTStatement parsedStatement =
            Parser.parseNextScriptStatement(parseResumeLocation, languageOptions);

        if (parsedStatement instanceof ASTVariableDeclaration) {
          this.applyVariableDeclaration((ASTVariableDeclaration) parsedStatement);
        }

        if(!this.isAnalyzableStatement(parsedStatement)) {
          return new AnalyzedStatement(parsedStatement);
        }

        parseResumeLocation.setBytePosition(startLocation);
        ResolvedStatement resolvedStatement =
            Analyzer.analyzeNextStatement(
                parseResumeLocation, analyzerOptions, catalogForAnalysis.getZetaSQLCatalog());

        this.applyCatalogMutation(resolvedStatement);

        return new AnalyzedStatement(parsedStatement, resolvedStatement);
      }
    };
  }
}
