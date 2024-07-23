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
import com.google.zetasql.parser.ASTNodes.ASTAlterAllRowAccessPoliciesStatement;
import com.google.zetasql.parser.ASTNodes.ASTAlterApproxViewStatement;
import com.google.zetasql.parser.ASTNodes.ASTAlterDatabaseStatement;
import com.google.zetasql.parser.ASTNodes.ASTAlterEntityStatement;
import com.google.zetasql.parser.ASTNodes.ASTAlterMaterializedViewStatement;
import com.google.zetasql.parser.ASTNodes.ASTAlterModelStatement;
import com.google.zetasql.parser.ASTNodes.ASTAlterPrivilegeRestrictionStatement;
import com.google.zetasql.parser.ASTNodes.ASTAlterRowAccessPolicyStatement;
import com.google.zetasql.parser.ASTNodes.ASTAlterSchemaStatement;
import com.google.zetasql.parser.ASTNodes.ASTAlterTableStatement;
import com.google.zetasql.parser.ASTNodes.ASTAlterViewStatement;
import com.google.zetasql.parser.ASTNodes.ASTAuxLoadDataStatement;
import com.google.zetasql.parser.ASTNodes.ASTCallStatement;
import com.google.zetasql.parser.ASTNodes.ASTCloneDataSource;
import com.google.zetasql.parser.ASTNodes.ASTCloneDataStatement;
import com.google.zetasql.parser.ASTNodes.ASTConnectionClause;
import com.google.zetasql.parser.ASTNodes.ASTCopyDataSource;
import com.google.zetasql.parser.ASTNodes.ASTCreateApproxViewStatement;
import com.google.zetasql.parser.ASTNodes.ASTCreateConstantStatement;
import com.google.zetasql.parser.ASTNodes.ASTCreateDatabaseStatement;
import com.google.zetasql.parser.ASTNodes.ASTCreateEntityStatement;
import com.google.zetasql.parser.ASTNodes.ASTCreateExternalTableStatement;
import com.google.zetasql.parser.ASTNodes.ASTCreateIndexStatement;
import com.google.zetasql.parser.ASTNodes.ASTCreateMaterializedViewStatement;
import com.google.zetasql.parser.ASTNodes.ASTCreateModelStatement;
import com.google.zetasql.parser.ASTNodes.ASTCreatePrivilegeRestrictionStatement;
import com.google.zetasql.parser.ASTNodes.ASTCreateProcedureStatement;
import com.google.zetasql.parser.ASTNodes.ASTCreateRowAccessPolicyStatement;
import com.google.zetasql.parser.ASTNodes.ASTCreateSchemaStatement;
import com.google.zetasql.parser.ASTNodes.ASTCreateSnapshotStatement;
import com.google.zetasql.parser.ASTNodes.ASTCreateSnapshotTableStatement;
import com.google.zetasql.parser.ASTNodes.ASTCreateTableStatement;
import com.google.zetasql.parser.ASTNodes.ASTCreateViewStatement;
import com.google.zetasql.parser.ASTNodes.ASTDefineTableStatement;
import com.google.zetasql.parser.ASTNodes.ASTDescribeStatement;
import com.google.zetasql.parser.ASTNodes.ASTDropAllRowAccessPoliciesStatement;
import com.google.zetasql.parser.ASTNodes.ASTDropEntityStatement;
import com.google.zetasql.parser.ASTNodes.ASTDropFunctionStatement;
import com.google.zetasql.parser.ASTNodes.ASTDropMaterializedViewStatement;
import com.google.zetasql.parser.ASTNodes.ASTDropPrivilegeRestrictionStatement;
import com.google.zetasql.parser.ASTNodes.ASTDropRowAccessPolicyStatement;
import com.google.zetasql.parser.ASTNodes.ASTDropSearchIndexStatement;
import com.google.zetasql.parser.ASTNodes.ASTDropSnapshotTableStatement;
import com.google.zetasql.parser.ASTNodes.ASTDropStatement;
import com.google.zetasql.parser.ASTNodes.ASTDropTableFunctionStatement;
import com.google.zetasql.parser.ASTNodes.ASTDropVectorIndexStatement;
import com.google.zetasql.parser.ASTNodes.ASTExportMetadataStatement;
import com.google.zetasql.parser.ASTNodes.ASTExportModelStatement;
import com.google.zetasql.parser.ASTNodes.ASTForeignKeyReference;
import com.google.zetasql.parser.ASTNodes.ASTFunctionCall;
import com.google.zetasql.parser.ASTNodes.ASTFunctionDeclaration;
import com.google.zetasql.parser.ASTNodes.ASTGrantStatement;
import com.google.zetasql.parser.ASTNodes.ASTImportStatement;
import com.google.zetasql.parser.ASTNodes.ASTMergeStatement;
import com.google.zetasql.parser.ASTNodes.ASTModelClause;
import com.google.zetasql.parser.ASTNodes.ASTModuleStatement;
import com.google.zetasql.parser.ASTNodes.ASTPathExpression;
import com.google.zetasql.parser.ASTNodes.ASTRenameStatement;
import com.google.zetasql.parser.ASTNodes.ASTRenameToClause;
import com.google.zetasql.parser.ASTNodes.ASTRevokeStatement;
import com.google.zetasql.parser.ASTNodes.ASTSequenceArg;
import com.google.zetasql.parser.ASTNodes.ASTShowStatement;
import com.google.zetasql.parser.ASTNodes.ASTSpannerInterleaveClause;
import com.google.zetasql.parser.ASTNodes.ASTStatement;
import com.google.zetasql.parser.ASTNodes.ASTSystemVariableExpr;
import com.google.zetasql.parser.ASTNodes.ASTTVF;
import com.google.zetasql.parser.ASTNodes.ASTTableAndColumnInfo;
import com.google.zetasql.parser.ASTNodes.ASTTableClause;
import com.google.zetasql.parser.ASTNodes.ASTTablePathExpression;
import com.google.zetasql.parser.ASTNodes.ASTTruncateStatement;
import com.google.zetasql.parser.ASTNodes.ASTUndropStatement;
import com.google.zetasql.parser.ParseTreeVisitor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Implements query-level rewrites based on the parsed tree. It allows modifying the query text
 * after parsing but before analyzing.
 *
 * <p>These rewrites are done at the query level because the parsed tree is immutable and can't be
 * modified itself.
 */
public class StatementRewriter {

  /**
   * Represents a rewrite that can be applied to a query string. When applied using {@link
   * #applyRewrites(String, List)}, the StatementRewriter replaces the substring between index from
   * and to (exclusive) with the content.
   */
  public static class Rewrite {
    public final int from;
    public final int to;
    public final String content;

    public Rewrite(int from, int to, String content) {
      this.from = from;
      this.to = to;
      this.content = content;
    }
  }

  /**
   * Applies a set of {@link Rewrite}s to a query string.
   *
   * @param query The query string to apply rewrites to
   * @param rewrites The list of rewrites to apply
   * @return The new query string with the rewrites applied
   * @throws IllegalArgumentException if there are any overlapping rewrites
   */
  public static String applyRewrites(String query, List<Rewrite> rewrites) {
    StringBuilder builder = new StringBuilder(query);
    List<Rewrite> sortedRewrites =
        rewrites.stream()
            .sorted(Comparator.comparing(rewrite -> rewrite.from))
            .collect(Collectors.toList());

    int rewritingOffset = 0;
    int previousRewriteTo = -1;
    for (Rewrite rewrite : sortedRewrites) {
      if (rewrite.to < previousRewriteTo) {
        throw new IllegalArgumentException("Found overlapping rewrites when applying");
      }

      int rewriteStartPosition = rewrite.from + rewritingOffset;
      int rewriteEndPosition = rewrite.to + rewritingOffset;

      String replacedString = "";
      if (rewrite.from == query.length()) {
        builder.append(rewrite.content);
      } else {
        builder.replace(rewriteStartPosition, rewriteEndPosition, rewrite.content);
        replacedString = query.substring(rewrite.from, rewrite.to);
        rewritingOffset += (rewrite.content.length() - replacedString.length());
        previousRewriteTo = rewrite.to;
      }
    }

    return builder.toString();
  }

  /**
   * Rewrites the query text to ensure all resource name paths present in a parsed statement from
   * said query are quoted entirely. For example, it rewrites "FROM project.dataset.table" to "FROM
   * `project.dataset.table`".
   *
   * <p>To do so; it finds all {@link ASTPathExpression} nodes in the parse tree refer to a resource
   * (i.e. tables, functions, etc.), builds their fully quoted representation and replaces their
   * original text in the query with their quoted representation.
   *
   * @param query The original query text the statement was parsed from
   * @param parsedStatement The parsed statement for which to rewrite name paths
   * @return The rewritten version of the query
   */
  public static String quoteNamePaths(String query, ASTStatement parsedStatement) {
    List<Rewrite> rewrites =
        getResourcePathExpressionFromParseTree(parsedStatement).stream()
            .filter(
                pathExpression ->
                    !pathExpression.getNames().get(0).getIdString().equalsIgnoreCase("SAFE"))
            .map(
                pathExpression ->
                    new Rewrite(
                        pathExpression.getParseLocationRange().start(),
                        pathExpression.getParseLocationRange().end(),
                        buildQuotedNamePath(pathExpression)))
            .collect(Collectors.toList());

    return applyRewrites(query, rewrites);
  }

  /**
   * Returns the fully quoted string representation of an {@link ASTPathExpression}.
   *
   * @param pathExpression The path expression for which to build the fully quoted representation
   * @return The fully quoted representation of the path expression
   */
  private static String buildQuotedNamePath(ASTPathExpression pathExpression) {
    String fullName = ParseTreeUtils.pathExpressionToString(pathExpression);
    return "`" + fullName + "`";
  }

  /**
   * Extracts all {@link ASTPathExpression} nodes that refer to a resource (i.e. tables, functions,
   * etc.) from a parse tree
   *
   * @param tree The parse tree
   * @return The list of all {@link ASTPathExpression} in the tree that refer to a resource
   */
  private static List<ASTPathExpression> getResourcePathExpressionFromParseTree(ASTNode tree) {
    ArrayList<ASTPathExpression> result = new ArrayList<>();

    tree.accept(
        new ParseTreeVisitor() {

          public void visit(ASTTablePathExpression node) {
            if (Objects.nonNull(node.getPathExpr())) {
              result.add(node.getPathExpr());
            }
            super.visit(node);
          }

          public void visit(ASTFunctionCall node) {
            result.add(node.getFunction());
            super.visit(node);
          }

          public void visit(ASTSequenceArg node) {
            result.add(node.getSequencePath());
          }

          public void visit(ASTDescribeStatement node) {
            result.add(node.getName());
            if (node.getOptionalFromName() != null) {
              result.add(node.getOptionalFromName());
            }
            super.visit(node);
          }

          public void visit(ASTShowStatement node) {
            result.add(node.getOptionalName());
            super.visit(node);
          }

          public void visit(ASTDropEntityStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTDropFunctionStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTDropTableFunctionStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTDropAllRowAccessPoliciesStatement node) {
            result.add(node.getTableName());
            super.visit(node);
          }

          public void visit(ASTDropMaterializedViewStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTDropSnapshotTableStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTDropSearchIndexStatement node) {
            result.add(node.getName());
            result.add(node.getTableName());
            super.visit(node);
          }

          public void visit(ASTDropVectorIndexStatement node) {
            result.add(node.getName());
            result.add(node.getTableName());
            super.visit(node);
          }

          public void visit(ASTRenameStatement node) {
            result.add(node.getOldName());
            result.add(node.getNewName());
            super.visit(node);
          }

          public void visit(ASTImportStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTModuleStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTSystemVariableExpr node) {
            result.add(node.getPath());
          }

          public void visit(ASTFunctionDeclaration node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTTVF node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTTableClause node) {
            result.add(node.getTablePath());
            super.visit(node);
          }

          public void visit(ASTModelClause node) {
            result.add(node.getModelPath());
          }

          public void visit(ASTConnectionClause node) {
            result.add(node.getConnectionPath());
          }

          public void visit(ASTCloneDataSource node) {
            result.add(node.getPathExpr());
            super.visit(node);
          }

          public void visit(ASTCopyDataSource node) {
            result.add(node.getPathExpr());
            super.visit(node);
          }

          public void visit(ASTCloneDataStatement node) {
            result.add(node.getTargetPath());
            super.visit(node);
          }

          public void visit(ASTCreateConstantStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTCreateDatabaseStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTCreateProcedureStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTCreateSchemaStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTCreateModelStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTCreateIndexStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTExportModelStatement node) {
            result.add(node.getModelNamePath());
            super.visit(node);
          }

          public void visit(ASTExportMetadataStatement node) {
            result.add(node.getNamePath());
            super.visit(node);
          }

          public void visit(ASTCallStatement node) {
            result.add(node.getProcedureName());
            super.visit(node);
          }

          public void visit(ASTDefineTableStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTCreateSnapshotStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTCreateSnapshotTableStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTTableAndColumnInfo node) {
            result.add(node.getTableName());
            super.visit(node);
          }

          public void visit(ASTTruncateStatement node) {
            result.add(node.getTargetPath());
            super.visit(node);
          }

          public void visit(ASTMergeStatement node) {
            result.add(node.getTargetPath());
            super.visit(node);
          }

          public void visit(ASTGrantStatement node) {
            result.add(node.getTargetPath());
            super.visit(node);
          }

          public void visit(ASTRevokeStatement node) {
            result.add(node.getTargetPath());
            super.visit(node);
          }

          public void visit(ASTRenameToClause node) {
            result.add(node.getNewName());
            super.visit(node);
          }

          public void visit(ASTAlterAllRowAccessPoliciesStatement node) {
            result.add(node.getTableNamePath());
            super.visit(node);
          }

          public void visit(ASTForeignKeyReference node) {
            result.add(node.getTableName());
            super.visit(node);
          }

          public void visit(ASTCreateEntityStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTDropPrivilegeRestrictionStatement node) {
            result.add(node.getNamePath());
            super.visit(node);
          }

          public void visit(ASTDropRowAccessPolicyStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTCreatePrivilegeRestrictionStatement node) {
            result.add(node.getNamePath());
            super.visit(node);
          }

          public void visit(ASTCreateRowAccessPolicyStatement node) {
            result.add(node.getName());
            result.add(node.getTargetPath());
            super.visit(node);
          }

          public void visit(ASTDropStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTCreateTableStatement node) {
            result.add(node.getName());
            if (node.getLikeTableName() != null) {
              result.add(node.getLikeTableName());
            }
            super.visit(node);
          }

          public void visit(ASTCreateExternalTableStatement node) {
            result.add(node.getName());
            if (node.getLikeTableName() != null) {
              result.add(node.getLikeTableName());
            }
            super.visit(node);
          }

          public void visit(ASTAuxLoadDataStatement node) {
            result.add(node.getName());
            if (node.getLikeTableName() != null) {
              result.add(node.getLikeTableName());
            }
            super.visit(node);
          }

          public void visit(ASTCreateViewStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTCreateApproxViewStatement node) {
            result.add(node.getName());
            super.visit(node);
          }

          public void visit(ASTCreateMaterializedViewStatement node) {
            result.add(node.getName());
            if (node.getReplicaSource() != null) {
              result.add(node.getReplicaSource());
            }
            super.visit(node);
          }

          public void visit(ASTAlterDatabaseStatement node) {
            result.add(node.getPath());
            super.visit(node);
          }

          public void visit(ASTAlterSchemaStatement node) {
            result.add(node.getPath());
            super.visit(node);
          }

          public void visit(ASTAlterTableStatement node) {
            result.add(node.getPath());
            super.visit(node);
          }

          public void visit(ASTAlterViewStatement node) {
            result.add(node.getPath());
            super.visit(node);
          }

          public void visit(ASTAlterMaterializedViewStatement node) {
            result.add(node.getPath());
            super.visit(node);
          }

          public void visit(ASTAlterApproxViewStatement node) {
            result.add(node.getPath());
            super.visit(node);
          }

          public void visit(ASTAlterModelStatement node) {
            result.add(node.getPath());
            super.visit(node);
          }

          public void visit(ASTAlterPrivilegeRestrictionStatement node) {
            result.add(node.getPath());
            super.visit(node);
          }

          public void visit(ASTAlterRowAccessPolicyStatement node) {
            result.add(node.getPath());
            super.visit(node);
          }

          public void visit(ASTAlterEntityStatement node) {
            result.add(node.getPath());
            super.visit(node);
          }

          public void visit(ASTSpannerInterleaveClause node) {
            result.add(node.getTableName());
            super.visit(node);
          }

          public void visit(ASTUndropStatement node) {
            result.add(node.getName());
            super.visit(node);
          }
        });

    return result;
  }
}
