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

import com.google.zetasql.parser.ASTNode;
import com.google.zetasql.parser.ASTNodes.ASTIdentifier;
import com.google.zetasql.parser.ASTNodes.ASTPathExpression;
import com.google.zetasql.parser.ASTNodes.ASTStatement;
import com.google.zetasql.parser.ParseTreeVisitor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implements query-level rewrites based on the parsed tree. It allows modifying the query text
 * after parsing but before analyzing.
 *
 * <p> These rewrites are done at the query level because the parsed tree is immutable and can't
 * be modified itself.
 */
class StatementRewriter {

  /**
   * Rewrites the query text to ensure all resource name paths present in a parsed statement from
   * said query are quoted entirely. For example, it rewrites "FROM project.dataset.table" to
   * "FROM `project.dataset.table`".
   *
   * <p> To do so; it finds all {@link ASTPathExpression} nodes in the parse tree, builds their
   * fully quoted representation and replaces their original text in the query with their quoted
   * representation.
   *
   * @param query The original query text the statement was parsed from
   * @param parsedStatement The parsed statement for which to rewrite name paths
   * @return The rewritten version of the query
   */
  public static String quoteNamePaths(String query, ASTStatement parsedStatement) {
    List<ASTPathExpression> pathExpressions =
        getPathExpressionsFromParseTree(parsedStatement).stream()
            .sorted(Comparator.comparing(expression -> expression.getParseLocationRange().start()))
            .collect(Collectors.toList());

    StringBuilder builder = new StringBuilder(query);

    int rewritingOffset = 0;
    for (ASTPathExpression pathExpression : pathExpressions) {
      int startPosition = pathExpression.getParseLocationRange().start();
      int endPosition = pathExpression.getParseLocationRange().end();

      String originalNamePath = query.substring(startPosition, endPosition);
      String quotedNamePath = buildQuotedNamePath(pathExpression);

      int rewriteStartPosition = startPosition + rewritingOffset;
      int rewriteEndPosition = endPosition + rewritingOffset;

      builder.replace(rewriteStartPosition, rewriteEndPosition, quotedNamePath);

      rewritingOffset += (quotedNamePath.length() - originalNamePath.length());
    }

    return builder.toString();
  }

  /**
   * Returns the fully quoted string representation of an {@link ASTPathExpression}.
   *
   * @param pathExpression The path expression for which to build the fully quoted representation
   * @return The fully quoted representation of the path expression
   */
  private static String buildQuotedNamePath(ASTPathExpression pathExpression) {
    String fullName = pathExpression.getNames()
        .stream()
        .map(ASTIdentifier::getIdString)
        .collect(Collectors.joining("."));
    return "`" + fullName + "`";
  }

  /**
   * Extracts all {@link ASTPathExpression} nodes from a parse tree
   *
   * @param tree The parse tree
   * @return The list of all {@link ASTPathExpression} in the tree
   */
  private static List<ASTPathExpression> getPathExpressionsFromParseTree(ASTNode tree) {
    ArrayList<ASTPathExpression> result = new ArrayList<>();

    tree.accept(new ParseTreeVisitor() {
      @Override
      public void visit(ASTPathExpression pathExpression) {
        result.add(pathExpression);
      }
    });

    return result;
  }

}
