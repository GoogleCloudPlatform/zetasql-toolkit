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
import com.google.zetasql.LanguageOptions;
import com.google.zetasql.ParseLocationRange;
import com.google.zetasql.ParseResumeLocation;
import com.google.zetasql.Parser;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.SqlException;
import com.google.zetasql.parser.ASTNode;
import com.google.zetasql.parser.ASTNodes.ASTCallStatement;
import com.google.zetasql.parser.ASTNodes.ASTExpression;
import com.google.zetasql.parser.ASTNodes.ASTFunctionCall;
import com.google.zetasql.parser.ASTNodes.ASTIdentifier;
import com.google.zetasql.parser.ASTNodes.ASTScript;
import com.google.zetasql.parser.ASTNodes.ASTStatement;
import com.google.zetasql.parser.ASTNodes.ASTTVF;
import com.google.zetasql.parser.ParseTreeVisitor;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Implements extensions to ZetaSQL's built-in {@link com.google.zetasql.Analyzer} */
public class AnalyzerExtensions {

  /**
   * Extracts the name paths for all functions called in a SQL statement
   *
   * @param sql The SQL statement from which to extract called functions
   * @param options The {@link LanguageOptions} to use when parsing the provided statement
   * @return The list of name paths for all functions called in the statement. If a function is
   *     called multiple times with different quoting (e.g. `catalog.function()` vs
   *     catalog.function()), it will be returned multiple times.
   */
  public static List<List<String>> extractFunctionNamesFromStatement(
      String sql, LanguageOptions options) {
    ASTStatement statement = Parser.parseStatement(sql, options);
    return extractFunctionNamesFromASTNode(statement);
  }

  /**
   * Extracts the name paths for all functions called in a SQL script
   *
   * @param sql The SQL script from which to extract called functions
   * @param options The {@link LanguageOptions} to use when parsing the provided script
   * @return The list of name paths for all functions called in the script. If a function is called
   *     multiple times with different quoting (e.g. `catalog.function()` vs catalog.function()), it
   *     will be returned multiple times.
   */
  public static List<List<String>> extractFunctionNamesFromScript(
      String sql, LanguageOptions options) {
    ASTScript script = Parser.parseScript(sql, options);
    return extractFunctionNamesFromASTNode(script);
  }

  /**
   * Extracts the name paths for all functions called in the next statement in the provided {@link
   * ParseResumeLocation}.
   *
   * @param parseResumeLocation The ParseResumeLocation from which to extract called functions
   * @param options The {@link LanguageOptions} to use when parsing the statement
   * @return The list of name paths for all functions called in the statement. If a function is
   *     called multiple times with different quoting (e.g. `catalog.function()` vs
   *     catalog.function()), it will be returned multiple times.
   */
  public static List<List<String>> extractFunctionNamesFromNextStatement(
      ParseResumeLocation parseResumeLocation, LanguageOptions options) {
    ASTStatement statement = Parser.parseNextStatement(parseResumeLocation, options);
    return extractFunctionNamesFromASTNode(statement);
  }

  /**
   * Extracts the name paths for all functions called in the provided parse tree
   *
   * @param node The root of the parse tree from which to extract functions called
   * @return The list of name paths for all functions called in the tree. If a function is called
   *     multiple times with different quoting (e.g. `catalog.function()` vs catalog.function()), it
   *     will be returned multiple times.
   */
  private static List<List<String>> extractFunctionNamesFromASTNode(ASTNode node) {
    Set<ImmutableList<String>> result = new LinkedHashSet<>();

    ParseTreeVisitor extractFunctionNamesVisitor =
        new ParseTreeVisitor() {

          @Override
          public void visit(ASTFunctionCall functionCall) {
            ImmutableList<String> functionNamePath =
                functionCall.getFunction().getNames().stream()
                    .map(ASTIdentifier::getIdString)
                    .collect(ImmutableList.toImmutableList());
            result.add(functionNamePath);
            super.visit(functionCall);
          }
        };

    node.accept(extractFunctionNamesVisitor);

    return ImmutableList.copyOf(result);
  }

  /**
   * Extracts the name paths for all TVFs called in a SQL statement
   *
   * @param sql The SQL statement from which to extract called functions
   * @param options The {@link LanguageOptions} to use when parsing the provided statement
   * @return The list of name paths for all functions called in the statement. If a function is
   *     called multiple times with different quoting (e.g. `catalog.function()` vs
   *     catalog.function()), it will be returned multiple times.
   */
  public static List<List<String>> extractTVFNamesFromStatement(
      String sql, LanguageOptions options) {
    ASTStatement statement = Parser.parseStatement(sql, options);
    return extractTVFNamesFromASTNode(statement);
  }

  /**
   * Extracts the name paths for all TVFs called in a SQL script
   *
   * @param sql The SQL script from which to extract called functions
   * @param options The {@link LanguageOptions} to use when parsing the provided script
   * @return The list of name paths for all functions called in the script. If a function is called
   *     multiple times with different quoting (e.g. `catalog.function()` vs catalog.function()), it
   *     will be returned multiple times.
   */
  public static List<List<String>> extractTVFNamesFromScript(String sql, LanguageOptions options) {
    ASTScript script = Parser.parseScript(sql, options);
    return extractTVFNamesFromASTNode(script);
  }

  /**
   * Extracts the name paths for all TVFs called in the next statement in the provided {@link
   * ParseResumeLocation}.
   *
   * @param parseResumeLocation The ParseResumeLocation from which to extract called functions
   * @param options The {@link LanguageOptions} to use when parsing the statement
   * @return The list of name paths for all functions called in the statement. If a function is
   *     called multiple times with different quoting (e.g. `catalog.function()` vs
   *     catalog.function()), it will be returned multiple times.
   */
  public static List<List<String>> extractTVFNamesFromNextStatement(
      ParseResumeLocation parseResumeLocation, LanguageOptions options) {
    ASTStatement statement = Parser.parseNextStatement(parseResumeLocation, options);
    return extractTVFNamesFromASTNode(statement);
  }

  /**
   * Extracts the name paths for all TVFs called in the provided parse tree
   *
   * @param node The root of the parse tree from which to extract functions called
   * @return The list of name paths for all functions called in the tree. If a function is called
   *     multiple times with different quoting (e.g. `catalog.function()` vs catalog.function()), it
   *     will be returned multiple times.
   */
  private static List<List<String>> extractTVFNamesFromASTNode(ASTNode node) {
    Set<ImmutableList<String>> result = new LinkedHashSet<>();

    ParseTreeVisitor extractFunctionNamesVisitor =
        new ParseTreeVisitor() {

          @Override
          public void visit(ASTTVF tvfCall) {
            ImmutableList<String> functionNamePath =
                tvfCall.getName().getNames().stream()
                    .map(ASTIdentifier::getIdString)
                    .collect(ImmutableList.toImmutableList());
            result.add(functionNamePath);
            super.visit(tvfCall);
          }
        };

    node.accept(extractFunctionNamesVisitor);

    return ImmutableList.copyOf(result);
  }

  /**
   * Extracts the name paths for all procedures called in a SQL statement. It can either return one
   * procedure or no procedures, depending on whether the provided statement is a CALL statement.
   *
   * @param sql The SQL statement from which to extract called procedures
   * @param options The {@link LanguageOptions} to use when parsing the provided statement
   * @return The list of name paths for all procedures called in the statement. If a procedure is
   *     called multiple times with different quoting (e.g. `catalog.procedure()` vs
   *     catalog.procedure()), it will be returned multiple times.
   */
  public static List<List<String>> extractProcedureNamesFromStatement(
      String sql, LanguageOptions options) {
    ASTStatement statement = Parser.parseStatement(sql, options);
    return extractProcedureNamesFromASTNode(statement);
  }

  /**
   * Extracts the name paths for all procedures called in a SQL script
   *
   * @param sql The SQL script from which to extract called procedures
   * @param options The {@link LanguageOptions} to use when parsing the provided script
   * @return The list of name paths for all procedures called in the script. If a procedure is
   *     called multiple times with different quoting (e.g. `catalog.procedure()` vs
   *     catalog.procedure()), it will be returned multiple times.
   */
  public static List<List<String>> extractProcedureNamesFromScript(
      String sql, LanguageOptions options) {
    ASTScript script = Parser.parseScript(sql, options);
    return extractProcedureNamesFromASTNode(script);
  }

  /**
   * Extracts the name paths for all procedures called in the next statement in the provided {@link
   * ParseResumeLocation}. It can either return one procedure or no procedures, depending on whether
   * the statement is a CALL statement.
   *
   * @param parseResumeLocation The ParseResumeLocation from which to extract called procedures
   * @param options The {@link LanguageOptions} to use when parsing the statement
   * @return The list of name paths for all procedures called in the statement. If a procedure is
   *     called multiple times with different quoting (e.g. `catalog.procedure()` vs
   *     catalog.procedure()), it will be returned multiple times.
   */
  public static List<List<String>> extractProcedureNamesFromNextStatement(
      ParseResumeLocation parseResumeLocation, LanguageOptions options) {
    ASTStatement statement = Parser.parseNextStatement(parseResumeLocation, options);
    return extractProcedureNamesFromASTNode(statement);
  }

  /**
   * Extracts the name paths for all procedures called in the provided parse tree
   *
   * @param node The root of the parse tree from which to extract procedures called
   * @return The list of name paths for all procedures called in the tree. If a procedure is called
   *     multiple times with different quoting (e.g. `catalog.procedure()` vs catalog.procedure()),
   *     it will be returned multiple times.
   */
  private static List<List<String>> extractProcedureNamesFromASTNode(ASTNode node) {
    Set<ImmutableList<String>> result = new LinkedHashSet<>();

    ParseTreeVisitor extractProcedureNamesVisitor =
        new ParseTreeVisitor() {

          @Override
          public void visit(ASTCallStatement procedureCall) {
            ImmutableList<String> functionNamePath =
                procedureCall.getProcedureName().getNames().stream()
                    .map(ASTIdentifier::getIdString)
                    .collect(ImmutableList.toImmutableList());
            result.add(functionNamePath);
            super.visit(procedureCall);
          }
        };

    node.accept(extractProcedureNamesVisitor);

    return ImmutableList.copyOf(result);
  }

  /**
   * Analyzes a parsed ASTExpression given the original SQL it belongs to
   *
   * @param sql The original SQL string the ASTExpression was parsed from
   * @param expression The ASTExpression that should be analyzed
   * @param options The {@link AnalyzerOptions} to use for analysis
   * @param catalog The {@link SimpleCatalog} to use for analysis
   * @return The {@link ResolvedExpr} resulting from the analysis
   * @throws SqlException if the analysis fails
   * @throws IllegalArgumentException if the expression's parse location exceeds the SQL string
   *     length. It will not happen if the SQL string is the query the expression was parsed from.
   */
  public static ResolvedExpr analyzeExpression(
      String sql, ASTExpression expression, AnalyzerOptions options, SimpleCatalog catalog) {
    ParseLocationRange expressionRange = expression.getParseLocationRange();

    if (expressionRange.end() > sql.length()) {
      String message =
          String.format(
              "Expression parse location %d exceeds SQL query length of %d",
              expressionRange.end(), sql.length());
      throw new IllegalArgumentException(message);
    }

    String expressionSource = sql.substring(expressionRange.start(), expressionRange.end());
    return Analyzer.analyzeExpression(expressionSource, options, catalog);
  }

  private AnalyzerExtensions() {}
}
