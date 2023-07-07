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
import com.google.zetasql.NotFoundException;
import com.google.zetasql.ParseResumeLocation;
import com.google.zetasql.Parser;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.SimpleConstantProtos.SimpleConstantProto;
import com.google.zetasql.SqlException;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.parser.ASTNodes.ASTExpression;
import com.google.zetasql.parser.ASTNodes.ASTIdentifier;
import com.google.zetasql.parser.ASTNodes.ASTScriptStatement;
import com.google.zetasql.parser.ASTNodes.ASTSingleAssignment;
import com.google.zetasql.parser.ASTNodes.ASTStatement;
import com.google.zetasql.parser.ASTNodes.ASTType;
import com.google.zetasql.parser.ASTNodes.ASTVariableDeclaration;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedLiteral;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedParameter;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import com.google.zetasql.toolkit.catalog.CatalogWrapper;
import com.google.zetasql.toolkit.catalog.basic.BasicCatalogWrapper;
import com.google.zetasql.toolkit.catalog.typeparser.ZetaSQLTypeParser;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Primary class exposed by the ZetaSQL Toolkit to perform SQL analysis.
 *
 * <p>It exposes methods to analyze statements using an empty catalog, an existing {@link
 * SimpleCatalog} or a {@link CatalogWrapper} implementation (such as the BigQueryCatalog or the
 * SpannerCatalog).
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
   * @return An iterator of the resulting {@link AnalyzedStatement}s. Consuming the iterator can
   *     throw an {@link AnalysisException} if analysis fails.
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
   * @return An iterator of the resulting {@link AnalyzedStatement}s. Consuming the iterator can
   *     throw an {@link AnalysisException} if analysis fails.
   */
  public Iterator<AnalyzedStatement> analyzeStatements(String query, CatalogWrapper catalog) {
    return this.analyzeStatements(query, catalog, false);
  }

  /**
   * Analyze a SQL query or script, using the provided {@link CatalogWrapper} to manage the
   * catalog.
   *
   * <p>This toolkit includes two implementations, the BigQueryCatalog and the SpannerCatalog;
   * which can be used to run the analyzer following BigQuery or Spanner catalog semantics
   * respectively. For other use-cases, you can provide your own CatalogWrapper implementation.
   *
   * @param query The SQL query or script to analyze
   * @param catalog The CatalogWrapper implementation to use when managing the catalog
   * @param inPlace Whether to apply catalog mutations in place. If true, catalog mutations from
   *     CREATE or DROP statements are applied to the provided catalog. If false, the provided
   *     catalog is copied and the copy is used.
   * @return An iterator of the resulting {@link AnalyzedStatement}s. Consuming the iterator can
   *     throw an {@link AnalysisException} if analysis fails.
   */
  public Iterator<AnalyzedStatement> analyzeStatements(
      String query, CatalogWrapper catalog, boolean inPlace) {
    CatalogWrapper catalogForAnalysis = inPlace ? catalog : catalog.copy();
    return new StatementAnalyzer(query, catalogForAnalysis, analyzerOptions);
  }

  /**
   * The {@link StatementAnalyzer} is the class that actually implements analysis for SQL in the
   * ZetaSQL Toolkit. It implements Iterator&lt;{@link AnalyzedStatement}&gt;, and consuming the
   * iterator will lazily perform SQL analysis statement by statement.
   *
   * <p> For each statement in the query, it will:
   * <ol>
   *   <li> Parse the statement using the {@link Parser}
   *   <li> If it is a variable declaration or an assignment, validate it and update the catalog
   *   <li> Resolve the statement if possible, using the {@link Analyzer}
   *   <li> Return the corresponding {@link AnalyzedStatement} object; containing the parsed
   *      {@link ASTStatement} and, optionally, the resolved {@link ResolvedStatement}
   * </ol>
   *
   * <p> Resolution will only happen for a statement if it is supported by the Analyzer (i.e.
   * it is not a script statement). If a complex script statement is encountered (e.g. IFs, LOOPs),
   * statement resolution will stop altogether and not be performed for the rest of this query.
   *
   * <p> If parsing, resolution or validations fail while analyzing a statement,
   * an {@link AnalysisException} will be thrown.
   */
  private static class StatementAnalyzer implements Iterator<AnalyzedStatement> {

    private final String query;
    private final CatalogWrapper catalog;
    private final AnalyzerOptions analyzerOptions;
    private final Coercer coercer;
    private final CatalogUpdaterVisitor catalogUpdaterVisitor;
    private final ParseResumeLocation parseResumeLocation;
    private boolean reachedComplexScriptStatement = false;

    public StatementAnalyzer(String query, CatalogWrapper catalog,
        AnalyzerOptions analyzerOptions) {
      this.query = query;
      this.catalog = catalog;
      this.analyzerOptions = analyzerOptions;
      this.coercer = new Coercer(analyzerOptions.getLanguageOptions());
      this.catalogUpdaterVisitor = new CatalogUpdaterVisitor(catalog);
      this.parseResumeLocation = new ParseResumeLocation(query);
    }

    @Override
    public boolean hasNext() {
      int inputLength = parseResumeLocation.getInput().getBytes().length;
      int currentPosition = parseResumeLocation.getBytePosition();
      return inputLength > currentPosition;
    }

    /**
     * Analyze the next statement in the query, following the steps outlined in this class's
     * javadoc.
     *
     * @return An {@link AnalyzedStatement} for the next statement of the query. It will include
     * the parsed statement and, optionally, the resolved statement. See this class's javadoc
     * to know when a statement is resolved.
     */
    @Override
    public AnalyzedStatement next() {
      int startLocation = parseResumeLocation.getBytePosition();

      ASTStatement parsedStatement = parseNextStatement(parseResumeLocation);

      this.reachedComplexScriptStatement =
          this.reachedComplexScriptStatement || isComplexScriptStatement(parsedStatement);

      // If the statement is a variable declaration, we need to validate it and create a Constant
      // in the catalog
      if (parsedStatement instanceof ASTVariableDeclaration) {
        this.applyVariableDeclaration((ASTVariableDeclaration) parsedStatement);
      }

      // If the statement is an assignment (SET statement), we need to validate it
      if (parsedStatement instanceof ASTSingleAssignment) {
        this.validateSingleAssignment((ASTSingleAssignment) parsedStatement);
      }

      if (this.reachedComplexScriptStatement || this.isScriptStatement(parsedStatement)) {
        return new AnalyzedStatement(parsedStatement, Optional.empty());
      }

      parseResumeLocation.setBytePosition(startLocation);

      ResolvedStatement resolvedStatement = analyzeNextStatement(parseResumeLocation);

      this.applyCatalogMutation(resolvedStatement);

      return new AnalyzedStatement(parsedStatement, Optional.of(resolvedStatement));
    }

    private ASTStatement parseNextStatement(ParseResumeLocation parseResumeLocation) {
      try {
        return Parser.parseNextScriptStatement(
            parseResumeLocation, analyzerOptions.getLanguageOptions());
      } catch (SqlException err) {
        throw new AnalysisException(err);
      }
    }

    private ResolvedStatement analyzeNextStatement(ParseResumeLocation parseResumeLocation) {
      try {
        return Analyzer.analyzeNextStatement(
            parseResumeLocation, analyzerOptions, catalog.getZetaSQLCatalog());
      } catch (SqlException err) {
        throw new AnalysisException(err);
      }
    }

    private boolean isScriptStatement(ASTStatement parsedStatement) {
      return parsedStatement instanceof ASTScriptStatement;
    }

    private boolean isComplexScriptStatement(ASTStatement parsedStatement) {
      boolean isVariableDeclarationOrSet =
          parsedStatement instanceof ASTVariableDeclaration
              || parsedStatement instanceof ASTSingleAssignment;

      return this.isScriptStatement(parsedStatement) && !isVariableDeclarationOrSet;
    }

    private Type parseASTType(ASTType astType) {
      String typeString = query.substring(
          astType.getParseLocationRange().start(),
          astType.getParseLocationRange().end()
      );
      return ZetaSQLTypeParser.parse(typeString);
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

    private void coerceExpressionToType(Type type, ResolvedExpr resolvedExpr) {
      Type expressionType = resolvedExpr.getType();
      boolean isLiteral = resolvedExpr instanceof ResolvedLiteral;
      boolean isParameter = resolvedExpr instanceof ResolvedParameter;

      boolean coerces = this.coercer.coercesTo(expressionType, type, isLiteral, isParameter);

      if (!coerces) {
        String message = String.format(
            "Cannot coerce expression of type %s to type %s",
            type, expressionType);
        throw new AnalysisException(message);
      }
    }

    private void applyVariableDeclaration(ASTVariableDeclaration declaration) {
      Optional<Type> explicitType = Optional.ofNullable(declaration.getType())
          .map(this::parseASTType);

      Optional<ResolvedExpr> defaultValueExpr = Optional.ofNullable(declaration.getDefaultValue())
          .map(expression -> AnalyzerExtensions.analyzeExpression(
              query, expression, analyzerOptions, catalog.getZetaSQLCatalog()));

      if (explicitType.isEmpty() && defaultValueExpr.isEmpty()) {
        // Should not happen, since this is enforced by the parser
        throw new AnalysisException(
            "Either the type or the default value must be present for variable declarations");
      }

      if (explicitType.isPresent() && defaultValueExpr.isPresent()) {
        this.coerceExpressionToType(explicitType.get(), defaultValueExpr.get());
      }

      Type variableType = explicitType.orElseGet(() -> defaultValueExpr.get().getType());

      List<Constant> constants = declaration.getVariableList()
          .getIdentifierList()
          .stream()
          .map(ASTIdentifier::getIdString)
          .map(variableName -> this.buildConstant(variableName, variableType))
          .collect(Collectors.toList());

      // TODO: Add constants to the catalog without breaking encapsulation
      constants.forEach(catalog.getZetaSQLCatalog()::addConstant);
    }

    private void validateSingleAssignment(ASTSingleAssignment singleAssignment) {
      String assignmentTarget = singleAssignment.getVariable().getIdString();
      ASTExpression expression = singleAssignment.getExpression();

      ResolvedExpr analyzedExpression = AnalyzerExtensions.analyzeExpression(
          query, expression, analyzerOptions, catalog.getZetaSQLCatalog());

      try {
        Constant constant = catalog.getZetaSQLCatalog().findConstant(List.of(assignmentTarget));
        this.coerceExpressionToType(constant.getType(), analyzedExpression);
      } catch (NotFoundException e) {
        throw new AnalysisException("Undeclared variable: " + assignmentTarget);
      }
    }

    private void applyCatalogMutation(ResolvedStatement statement) {
      statement.accept(catalogUpdaterVisitor);
    }

  }

}
