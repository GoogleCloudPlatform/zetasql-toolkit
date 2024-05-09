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

package com.google.zetasql.toolkit.tools.migration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.zetasql.Parser;
import com.google.zetasql.SqlFormatter;
import com.google.zetasql.parser.ASTNode;
import com.google.zetasql.parser.ASTNodes.ASTFromClause;
import com.google.zetasql.parser.ASTNodes.ASTIdentifier;
import com.google.zetasql.parser.ASTNodes.ASTJoin;
import com.google.zetasql.parser.ASTNodes.ASTPathExpression;
import com.google.zetasql.parser.ASTNodes.ASTScript;
import com.google.zetasql.parser.ASTNodes.ASTSelect;
import com.google.zetasql.parser.ASTNodes.ASTStatement;
import com.google.zetasql.parser.ASTNodes.ASTTVF;
import com.google.zetasql.parser.ASTNodes.ASTTablePathExpression;
import com.google.zetasql.parser.ASTNodes.ASTTableSubquery;
import com.google.zetasql.parser.ASTNodes.ASTWhereClause;
import com.google.zetasql.parser.ParseTreeVisitor;
import com.google.zetasql.toolkit.ParseTreeUtils;
import com.google.zetasql.toolkit.StatementRewriter;
import com.google.zetasql.toolkit.StatementRewriter.Rewrite;
import com.google.zetasql.toolkit.options.BigQueryLanguageOptions;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class TableSuffixMigrator {

  private final List<Rewrite> rewrites = new ArrayList<>();
  private final String projectId;
  private final Map<String, String> tablesToPartitionColumns;

  public TableSuffixMigrator(String projectId, Map<String, String> tablesToPartitionColumns) {
    this.projectId = projectId;
    this.tablesToPartitionColumns = tablesToPartitionColumns;
  }

  private void addRewrite(int from, int to, String content) {
    this.rewrites.add(new Rewrite(from, to, content));
  }

  private void addRewrite(ASTNode node, String content) {
    this.rewrites.add(new Rewrite(
        node.getParseLocationRange().start(),
        node.getParseLocationRange().end(),
        content));
  }

  private List<ASTTablePathExpression> findTablePathExpressions(ASTSelect select) {
    ArrayList<ASTTablePathExpression> result = new ArrayList<>();

    select.accept(new ParseTreeVisitor() {
      public void visit(ASTTVF tvf) {}
      public void visit(ASTTableSubquery subquery) {}
      public void visit(ASTJoin join) {
        join.getLhs().accept(this);
        join.getRhs().accept(this);
      }
      public void visit(ASTTablePathExpression tablePathExpression) {
        result.add(tablePathExpression);
      }
    });

    return result;
  }

  private void insertNewWhereFilter(ASTSelect select, List<WildcardTable> wildcardTables) {
    List<String> extraWhereClauseExpressions = new ArrayList<>();

    for (WildcardTable wildcardTable : wildcardTables) {
      String partitionColumn =
          Optional.ofNullable(tablesToPartitionColumns.get(wildcardTable.fullNamePrefix))
              .orElse("_PARTITIONTIME");
      wildcardTable.getEquivalentWhereFilter(partitionColumn)
              .ifPresent(extraWhereClauseExpressions::add);
    }

    if (extraWhereClauseExpressions.isEmpty()) {
      return;
    }

    String newWhereClauseExpression = String.join(" AND ", extraWhereClauseExpressions);
    newWhereClauseExpression = "(" + newWhereClauseExpression + ")";
    ASTWhereClause whereClause = select.getWhereClause();

    if (whereClause == null) {
      ASTFromClause fromClause = select.getFromClause();
      this.addRewrite(
          fromClause.getParseLocationRange().end(),
          fromClause.getParseLocationRange().end(),
          " WHERE " + newWhereClauseExpression);
    } else {
      this.addRewrite(
          whereClause.getParseLocationRange().start(),
          whereClause.getParseLocationRange().start() + 6,
          "WHERE (");
      this.addRewrite(
          whereClause.getParseLocationRange().end(),
          whereClause.getParseLocationRange().end(),
          ") AND " + newWhereClauseExpression);
    }
  }

  private List<ASTPathExpression> findTableSuffixReferences(
      ASTSelect select,
      Optional<String> withAlias) {
    ArrayList<ASTPathExpression> result = new ArrayList<>();

    select.accept(new ParseTreeVisitor() {
      public void visit(ASTTVF tvf) {}
      public void visit(ASTTableSubquery subquery) {}
      public void visit(ASTPathExpression pathExpression) {
        List<ASTIdentifier> names = pathExpression.getNames();
        String lastNamePathElement = names.get(names.size() - 1).getIdString();

        if (!lastNamePathElement.equalsIgnoreCase("_TABLE_SUFFIX")) {
          return;
        }

        if (!withAlias.isPresent() && names.size() == 1) {
          result.add(pathExpression);
        } else if (
            withAlias.isPresent()
                && names.size() == 2
                && names.get(0).getIdString().equalsIgnoreCase(withAlias.get())) {
          result.add(pathExpression);
        }
      }
    });

    return result;
  }

  private void processSelect(ASTSelect select) throws ParseException {
    List<WildcardTable> wildcardTables = findTablePathExpressions(select)
        .stream()
        .map(tablePathExpression -> WildcardTable.tryBuild(this.projectId, tablePathExpression))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());

    for (WildcardTable wildcardTable : wildcardTables) {
      String replacementName = String.format("`%s`", wildcardTable.fullNamePrefix);
      String partitionColumn =
          Optional.ofNullable(tablesToPartitionColumns.get(wildcardTable.fullNamePrefix))
              .orElse("_PARTITIONTIME");
      this.addRewrite(wildcardTable.parseTreeNode, replacementName);
      List<ASTPathExpression> tableSuffixReferences =
          findTableSuffixReferences(select, wildcardTable.alias);
      for (ASTPathExpression tableSuffixReference : tableSuffixReferences) {
        this.addRewrite(
            tableSuffixReference,
            wildcardTable.getTableSuffixEquivalent(partitionColumn));
      }
    }

    insertNewWhereFilter(select, wildcardTables);
  }

  private void findRequiredRewrites(ASTStatement parsedStatement) throws ParseException {
    List<ASTSelect> selects = ParseTreeUtils.findDescendantSubtreesWithKind(
        parsedStatement, ASTSelect.class);

    for (ASTSelect select : selects) {
      processSelect(select);
    }

  }

  public String migrate(String query) throws ParseException {
    ASTScript parsedScript =
        Parser.parseScript(query, BigQueryLanguageOptions.get());
    List<ASTStatement> statements = parsedScript
        .getStatementListNode()
        .getStatementList();

    for (ASTStatement statement : statements) {
      findRequiredRewrites(statement);
    }

    return StatementRewriter.applyRewrites(query, rewrites);
  }

  public static void main(String[] args) throws ParseException {
    String query = "SELECT 1";
    ImmutableMap<String, String> tablesToPartitionColumns = ImmutableMap.<String, String>builder()
        .build();
    String result = new TableSuffixMigrator("bigquery-public-data", tablesToPartitionColumns)
        .migrate(query);
    String formatted = new SqlFormatter().lenientFormatSql(result);
    System.out.println(formatted);
  }

}
