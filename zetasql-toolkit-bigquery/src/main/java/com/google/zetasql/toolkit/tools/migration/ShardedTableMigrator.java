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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Tool for migrating BigQuery jobs away from date-sharded tables to partitioned tables. This tool
 * automatically rewrites queries that use table wildcards and _TABLE_SUFFIX to use the new
 * partitioned tables. Only SELECT statements are currently rewritten.
 *
 * <p> This tool currently makes the following assumptions about the migration:
 * <ul>
 *   <li> The original sharded tables use the format `table_YYYYMMDD`
 *   <li> The new tables use the same names as the shared tables, but drop the `_YYYYMMDD` suffix
 *   <li> All partitioned tables have been created already (i.e. no incremental migration of
 *        queries for now)
 *   <li> All wildcard table references will follow one of these formats:
 *   <ul>
 *     <li> `[project].dataset.table_name_YYYYMMDD`
 *     <li> `[project].dataset.table_name_YYYYMM*`
 *     <li> `[project].dataset.table_name_YYYY*`
 *     <li> `[project].dataset.table_name_*`
 *   </ul>
 * </ul>
 *
 * <p> When migrating a SQL script, this tool will:
 * <ol>
 *   <li> Find all SELECT statements or expressions
 *   <li> Drop the wildcard or date-sharded suffix from table references (e.g. `table_*`,
 *        `table_20240517` and `table_2024*` all become `table`)
 *   <li> Replace references to _TABLE_SUFFIX with an equivalent expression like
 *        FORMAT_DATE("...", [PARTITION_COLUMN])
 *   <li> Add required WHERE condition filters to achieve the same results from queries (e.g.
 *        a query on `table_20240517` will be translated to a query on `table` with the condition
 *        DATE([PARTITION_COLUMN]) = '2024-05-17' added to the WHERE clause)
 * </ol>
 */
public class ShardedTableMigrator {

  private static Rewrite createRewrite(int from, int to, String content) {
    return new Rewrite(from, to, content);
  }

  private static Rewrite createRewrite(ASTNode node, String content) {
    return new Rewrite(
        node.getParseLocationRange().start(),
        node.getParseLocationRange().end(),
        content);
  }

  /**
   * Finds all tables referenced in an {@link ASTSelect}. Does not recurse into subqueries.
   *
   * @param select The ASTSelect to get _TABLE_SUFFIX references from
   * @return The list of {@link ASTTablePathExpression} referenced in the provided ASTSelect
   */
  private static List<ASTTablePathExpression> findTableExpressions(ASTSelect select) {
    ArrayList<ASTTablePathExpression> result = new ArrayList<>();

    select.accept(new ParseTreeVisitor() {
      public void visit(ASTTVF tvf) {}
      public void visit(ASTTableSubquery subquery) {}
      public void visit(ASTJoin join) {
        join.getLhs().accept(this);
        join.getRhs().accept(this);
      }
      public void visit(ASTTablePathExpression tablePathExpression) {
        if (Objects.nonNull(tablePathExpression.getPathExpr())) {
          result.add(tablePathExpression);
        }
      }
    });

    return result;
  }

  /**
   * Finds all references to _TABLE_SUFFIX in as {@link ASTSelect} that come from a specific table
   * given its alias. If withAlias is present, it will return all expressions in the shape
   * "[alias]._TABLE_SUFFIX". If withAlias is not present, it will return all expressions in the
   * shape "_TABLE_SUFFIX".
   *
   * <p> Does not recurse into subqueries
   *
   * @param select The ASTSelect to get _TABLE_SUFFIX references from
   * @param withAlias Optionally, the alias the table we're looking for has
   * @return The references to _TABLE_SUFFIX
   */
  private static List<ASTPathExpression> findTableSuffixReferences(
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

  /**
   * Returns the list of {@link StatementRewriter.Rewrite}s that replace references to sharded
   * tables in an {@link ASTSelect} with their corresponding partitioned columns by dropping
   * the `_YYYYMMDD` suffix
   *
   * @param referencedWildcardTables The sharded tables the ASTSelect references
   * @return The list of Rewrites that need to be applied the original query
   */
  private static List<Rewrite> replaceWildcardTableReferences(
      List<WildcardTable> referencedWildcardTables) {
    return referencedWildcardTables.stream()
        .map(wildcardTable -> createRewrite(
            wildcardTable.parseTreeNode,
            wildcardTable.getFullNamePrefixQuoted()))
        .collect(Collectors.toList());
  }

  /**
   * Returns the list of {@link StatementRewriter.Rewrite}s that replace references to _TABLE_SUFFIX
   * in an {@link ASTSelect} with equivalent expressions based on the partition column.
   *
   * @param select The ASTSelect to generate rewrites for
   * @param referencedWildcardTables The sharded tables the ASTSelect references
   * @param tablesToPartitionColumns A mapping from the names of partitioned tables to their
   *  time partitioning column. E.g. if the only sharded table is `table_YYYYMMDD`, this map
   *  should be ("table" -> "PARTITION_COLUMN_NAME")
   * @return The list of Rewrites that need to be applied to the original query
   */
  private static List<Rewrite> replaceTableSuffixReferences(
      ASTSelect select, List<WildcardTable> referencedWildcardTables,
      Map<String, String> tablesToPartitionColumns) {
    ArrayList<Rewrite> rewrites = new ArrayList<>();

    for (WildcardTable referencedWildcardTable : referencedWildcardTables) {
      String partitionColumn = tablesToPartitionColumns.get(referencedWildcardTable.fullNamePrefix);
      List<ASTPathExpression> tableSuffixReferences =
          findTableSuffixReferences(select, referencedWildcardTable.alias);

      if (Objects.isNull(partitionColumn)) {
        throw new IllegalArgumentException(
            "Missing partition column for wildcard table " + referencedWildcardTable.fullNamePrefix);
      }

      for (ASTPathExpression tableSuffixReference : tableSuffixReferences) {
        String tableSuffixEquivalent =
            referencedWildcardTable.getTableSuffixEquivalent(partitionColumn);
        rewrites.add(createRewrite(tableSuffixReference, tableSuffixEquivalent));
      }
    }

    return rewrites;
  }

  /**
   * Returns the list of {@link StatementRewriter.Rewrite}s that add the required WHERE conditions
   * to an {@link ASTSelect}, based on the list of sharded tables it queries.
   *
   * @param select The ASTSelect to generate rewrites for
   * @param referencedWildcardTables The sharded tables the ASTSelect references
   * @param tablesToPartitionColumns A mapping from the names of partitioned tables to their
   *  time partitioning column. E.g. if the only sharded table is `table_YYYYMMDD`, this map
   *  should be ("table" -> "PARTITION_COLUMN_NAME")
   * @return The list of Rewrites that need to be applied to the original query
   */
  private static List<Rewrite> insertNewWhereFilters(
      ASTSelect select, List<WildcardTable> referencedWildcardTables,
      Map<String, String> tablesToPartitionColumns) {
    List<String> extraWhereClauseExpressions = new ArrayList<>();

    for (WildcardTable referencedWildcardTable : referencedWildcardTables) {
      String partitionColumn = tablesToPartitionColumns.get(referencedWildcardTable.fullNamePrefix);

      if (Objects.isNull(partitionColumn)) {
        throw new IllegalArgumentException(
            "Missing partition column for wildcard table "
                + referencedWildcardTable.fullNamePrefix);
      }

      referencedWildcardTable.getEquivalentWhereFilter(partitionColumn)
          .ifPresent(extraWhereClauseExpressions::add);
    }

    if (extraWhereClauseExpressions.isEmpty()) {
      return ImmutableList.of();
    }

    ArrayList<Rewrite> rewrites = new ArrayList<>();
    String newWhereClauseExpression =
        "(" + String.join(" AND ", extraWhereClauseExpressions) + ")";
    ASTWhereClause whereClause = select.getWhereClause();

    if (Objects.isNull(whereClause)) {
      // The select had no WHERE clause, so we add "WHERE {newWhereClauseExpression}" after
      // the FROM clause
      ASTFromClause fromClause = select.getFromClause();
      rewrites.add(createRewrite(
          fromClause.getParseLocationRange().end(),
          fromClause.getParseLocationRange().end(),
          " WHERE " + newWhereClauseExpression));
    } else {
      // The select already had a WHERE clause. We rewrite "WHERE {previousExpression}" to
      // "WHERE ({previousExpression}) AND {newWhereClauseExpression}"
      rewrites.add(createRewrite(
          whereClause.getParseLocationRange().start() + 5,
          whereClause.getParseLocationRange().start() + 5,
          " ("));
      rewrites.add(createRewrite(
          whereClause.getParseLocationRange().end(),
          whereClause.getParseLocationRange().end(),
          ") AND " + newWhereClauseExpression));
    }

    return rewrites;
  }

  /**
   * Returns the list of {@link StatementRewriter.Rewrite}s that need to be applied to the provided
   * {@link ASTSelect}.
   *
   * @param select The ASTSelect to generate rewrites for
   * @param projectId The GCP project id this query would be run on
   * @param tablesToPartitionColumns A mapping from the names of partitioned tables to their
   *  time partitioning column. E.g. if the only sharded table is `table_YYYYMMDD`, this map
   *  should be ("table" -> "PARTITION_COLUMN_NAME")
   * @return The list of Rewrites that need to be applied to the original query
   */
  private static List<Rewrite> rewriteSelect(
      ASTSelect select, String projectId, Map<String, String> tablesToPartitionColumns) {
    List<ASTTablePathExpression> referencedTables = findTableExpressions(select);

    List<WildcardTable> referencedWildcardTables = referencedTables
        .stream()
        .map(tablePathExpression -> WildcardTable.tryBuild(projectId, tablePathExpression))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());

    ArrayList<Rewrite> rewrites = new ArrayList<>();

    rewrites.addAll(replaceWildcardTableReferences(referencedWildcardTables));
    rewrites.addAll(replaceTableSuffixReferences(select, referencedWildcardTables, tablesToPartitionColumns));
    rewrites.addAll(insertNewWhereFilters(select, referencedWildcardTables, tablesToPartitionColumns));

    return rewrites;
  }

  /**
   * Rewrites a query to use partitioned tables instead of date-shared tables and _TABLE_SUFFIX. See
   * this class' javadoc for more information.
   *
   * @param query The query to migrate
   * @param projectId The GCP project id this query would be run on
   * @param tablesToPartitionColumns A mapping from the names of partitioned tables to their
   *  time partitioning column. E.g. if the only sharded table is `table_YYYYMMDD`, this map
   *  should be ("table" -> "PARTITION_COLUMN_NAME")
   * @return The rewritten query
   */
  public static String migrate(String query, String projectId, Map<String, String> tablesToPartitionColumns) {
    ASTScript parsedScript =
        Parser.parseScript(query, BigQueryLanguageOptions.get());

    List<ASTSelect> selects = ParseTreeUtils.findDescendantSubtreesWithKind(
        parsedScript, ASTSelect.class);

    List<Rewrite> rewrites = selects.stream()
        .flatMap(select -> rewriteSelect(select, projectId, tablesToPartitionColumns).stream())
        .collect(Collectors.toList());

    return StatementRewriter.applyRewrites(query, rewrites);
  }

}
