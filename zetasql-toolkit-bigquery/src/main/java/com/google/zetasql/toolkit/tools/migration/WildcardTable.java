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

import com.google.zetasql.parser.ASTNodes.ASTAlias;
import com.google.zetasql.parser.ASTNodes.ASTIdentifier;
import com.google.zetasql.parser.ASTNodes.ASTPathExpression;
import com.google.zetasql.parser.ASTNodes.ASTTablePathExpression;
import com.google.zetasql.toolkit.ParseTreeUtils;
import com.google.zetasql.toolkit.catalog.bigquery.BigQueryReference;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a wildcard table that was referenced in a query.
 *
 * <p> This assumes that all wildcard table names follow the pattern "table_name_YYYYMMDD" and that
 * references to a wildcard table in a query will match one of these patterns:
 *
 * <ul>
 *   <li> `[project].dataset.table_name_*`
 *   <li> `[project].dataset.table_name_YYYY*`
 *   <li> `[project].dataset.table_name_YYYYMM*`
 *   <li> `[project].dataset.table_name_YYYYMMDD`
 * </ul>
 */
class WildcardTable {

  /**
   * The full name of the table, with the wildcard prefix removed.
   *
   * <p> E.g. "p.d.table_*" and "p.d.table_20240517" both have the prefix "p.d.table".
   */
  public final String fullNamePrefix;
  /**
   * The {@link Type} of this wildcard table.
   */
  public final Type type;
  /**
   * The concrete filter used at the end of the wildcard table.
   *
   * <p> E.g. "p.d.table_20240517" has the concreteFilter "20240517". "p.d.table_202405*" has
   * "202405".
   */
  public final String concreteFilter;
  /**
   * The {@link ASTPathExpression} that references this table.
   */
  public final ASTPathExpression parseTreeNode;
  /**
   * Optionally, the alias this table was given when referenced.
   */
  public final Optional<String> alias;

  /** Use {@link #tryBuild(String, ASTTablePathExpression)} */
  private WildcardTable(
      String fullNamePrefix,
      String concreteFilter,
      Type type,
      ASTPathExpression parseTreeNode,
      Optional<String> alias) {
    this.fullNamePrefix = fullNamePrefix;
    this.concreteFilter = concreteFilter;
    this.type = type;
    this.parseTreeNode = parseTreeNode;
    this.alias = alias;
  }

  public String getFullNamePrefixQuoted() {
    return String.format("`%s`", this.fullNamePrefix);
  }

  /**
   * Returns a filter expression using the partition column that has the same filtering effect
   * this wildcard reference did. This filter expression should be added to the query's WHERE
   * clause. Returns an empty optional if no filter should be added.
   *
   * <p> E.g. the wildcard table "p.d.table_20240517" will add the filter expression
   * "DATE([PARTITION_COLUMN]) = '2024-05-17'".
   *
   * @param partitionColumnName the name of the partition column for this table
   * @return The filter expression that should be added to the query's WHERE clause, an empty
   *  optional if no filter should be added.
   */
  public Optional<String> getEquivalentWhereFilter(String partitionColumnName) {
    Objects.requireNonNull(partitionColumnName);

    String dateColumn = this.alias
        .map(alias -> String.format("DATE(%s.%s)", alias, partitionColumnName))
        .orElse(String.format("DATE(%s)", partitionColumnName));

    String filter = null;

    switch (this.type) {
      case CONCRETE:
        SimpleDateFormat parser = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        try {
          String dateExpr = formatter.format(parser.parse(this.concreteFilter));
          filter = String.format("%s = '%s'", dateColumn, dateExpr);
        } catch (ParseException err) {
          // This shouldn't happen, otherwise this wouldn't have been recognized as a CONCRETE
          // wildcard table.
          throw new IllegalStateException(
              "Invalid date filter for CONCRETE wildcard table " + this.concreteFilter, err);
        }
        break;
      case WILDCARD_DAY:
        filter = String.format(
            "FORMAT_DATE('%%Y%%m', %s) = '%s'",
            dateColumn, this.concreteFilter);
        break;
      case WILDCARD_MONTH_DAY:
        filter = String.format(
            "FORMAT_DATE('%%Y', %s) = '%s'",
            dateColumn, this.concreteFilter);
        break;
      case WILDCARD_DATE:
        break;
    }

    return Optional.ofNullable(filter);
  }

  /**
   * Returns an expression using the partition column that's equivalent what _TABLE_SUFFIX would
   * be in this wildcard table.
   *
   * <p> E.g. the wildcard table "p.d.table_*" will return the expression
   *  FORMAT_DATE('%Y%m%d', [PARTITION_COLUMN]).
   *
   * @param partitionColumnName the name of the partition column for this table
   * @return an expression using the partition column that's equivalent to what _TABLE_SUFFIX
   *  would be in this wildcard table.
   */
  public String getTableSuffixEquivalent(String partitionColumnName) {
    Objects.requireNonNull(partitionColumnName);

    String dateColumn = this.alias
        .map(alias -> String.format("DATE(%s.%s)", alias, partitionColumnName))
        .orElse(String.format("DATE(%s)", partitionColumnName));

    String dateFormat = null;

    switch (this.type) {
      case CONCRETE:
        throw new IllegalArgumentException("Referenced _TABLE_SUFFIX on a table without wildcard");
      case WILDCARD_DAY:
        dateFormat = "%d";
        break;
      case WILDCARD_MONTH_DAY:
        dateFormat = "%m%d";
        break;
      case WILDCARD_DATE:
        dateFormat = "%Y%m%d";
        break;
    }

    return String.format("FORMAT_DATE('%s', %s)", dateFormat, dateColumn);
  }

  /**
   * Tries to build a WildcardTable based on an {@link ASTTablePathExpression} referencing it
   * and the GCP project id the query would be run on. Returns an empty optional if this isn't a
   * reference to a wildcard table.
   *
   * @param projectId the GCP project id the query would be run on
   * @param tablePathExpression the ASTTablePathExpression which potentially references a wildcard
   *  table
   * @return The built WildCardTable, an empty optional if this isn't a reference to a wildcard
   *  table
   */
  public static Optional<WildcardTable> tryBuild(
      String projectId, ASTTablePathExpression tablePathExpression) {
    ASTPathExpression pathExpression = tablePathExpression.getPathExpr();

    Optional<BigQueryReference> maybeBigQueryReference = Optional.ofNullable(pathExpression)
        .map(ParseTreeUtils::pathExpressionToString)
        .filter(BigQueryReference::isQualified)
        .map(pathName -> BigQueryReference.from(projectId, pathName));

    Optional<Type> maybeType = maybeBigQueryReference
        .map(BigQueryReference::getResourceName)
        .flatMap(Type::getType);

    if (!maybeBigQueryReference.isPresent() || !maybeType.isPresent()) {
      return Optional.empty();
    }

    BigQueryReference bigQueryReference = maybeBigQueryReference.get();
    Type type = maybeType.get();

    String tableNamePrefix = type.extractNamePrefix(bigQueryReference.getResourceName());
    String fullNamePrefix = String.format(
        "%s.%s.%s",
        bigQueryReference.getProjectId(), bigQueryReference.getDatasetId(), tableNamePrefix);
    String concreteFilter = type.extractConcreteFilter(bigQueryReference.getResourceName());

    return Optional.of(new WildcardTable(
        fullNamePrefix,
        concreteFilter,
        type,
        pathExpression,
        Optional.ofNullable(tablePathExpression.getAlias())
            .map(ASTAlias::getIdentifier)
            .map(ASTIdentifier::getIdString)
    ));
  }

  /**
   * Represents the type of a wildcard table reference. Supported types:
   *
   * <ul>
   *   <li> CONCRETE (`[project].dataset.table_name_YYYYMMDD`)
   *   <li> WILDCARD_DAY (`[project].dataset.table_name_YYYYMM*`)
   *   <li> WILDCARD_MONTH_DAY (`[project].dataset.table_name_YYYY*`)
   *   <li> WILDCARD_DATE (`[project].dataset.table_name_*`)
   * </ul>
   */
  private enum Type {
    CONCRETE(Pattern.compile("([a-zA-Z0-9\\_-]+)_([0-9]{8})")),
    WILDCARD_DAY(Pattern.compile("([a-zA-Z0-9\\_-]+)_([0-9]{6})\\*")),
    WILDCARD_MONTH_DAY(Pattern.compile("([a-zA-Z0-9\\_-]+)_([0-9]{4})\\*")),
    WILDCARD_DATE(Pattern.compile("([a-zA-Z0-9\\_-]+)_()\\*"));

    public final Pattern pattern;

    Type(Pattern pattern) {
      this.pattern = pattern;
    }

    /**
     * Gets the type of a wildcard table reference based on the name used to reference it.
     * Returns an empty optional if the name does not follow any of the supported formats.
     *
     * @param referenceName the name used to reference the table
     * @return the type of wildcard table referenced, an empty optional if the name does not follow
     *  any of the supported formats
     */
    public static Optional<Type> getType(String referenceName) {
      // We need to check in this order, since the patterns conflict with
      // each other otherwise.
      Type[] types = {
          WILDCARD_DATE, WILDCARD_MONTH_DAY, WILDCARD_DAY, CONCRETE
      };
      for (Type type : types) {
        Matcher matcher = type.pattern.matcher(referenceName);
        if (matcher.find()) {
          return Optional.of(type);
        }
      }

      return Optional.empty();
    }

    public Matcher match(String referenceName) {
      Matcher matcher = this.pattern.matcher(referenceName);

      if (!matcher.find()) {
        throw new IllegalArgumentException(String.format(
            "Invalid wildcard table %s for type %s",
            referenceName, this.name()
        ));
      }

      return matcher;
    }

    /**
     * Extracts the name prefix from the name used to reference a wildcard table of this type.
     *
     * <p> E.g. "table_*" and "table_20240517" both have the prefix "table".
     *
     * @param referenceName The name used to reference a wildcard table of this type
     * @return The table's name prefix
     */
    public String extractNamePrefix(String referenceName) {
      return this.match(referenceName).group(1);
    }

    /**
     * Extracts the concrete filter applied when referencing a wildcard table of this type from
     * the name used to reference it
     *
     * <p> E.g. "table_20240517" has the concreteFilter "20240517". "table_202405*" has
     *  "202405".
     *
     * @param referenceName The name used to reference a wildcard table of this type
     * @return The table's concrete filter
     */
    public String extractConcreteFilter(String referenceName) {
      return this.match(referenceName).group(2);
    }
  }
}
