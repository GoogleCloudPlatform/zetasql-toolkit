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

package com.google.zetasql.toolkit.catalog.spanner;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Spanner;
import com.google.common.collect.ImmutableList;
import com.google.zetasql.Analyzer;
import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.Constant;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.SimpleTable;
import com.google.zetasql.ZetaSQLBuiltinFunctionOptions;
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.CreateMode;
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.CreateScope;
import com.google.zetasql.toolkit.catalog.CatalogOperations;
import com.google.zetasql.toolkit.catalog.CatalogWrapper;
import com.google.zetasql.toolkit.catalog.FunctionInfo;
import com.google.zetasql.toolkit.catalog.ProcedureInfo;
import com.google.zetasql.toolkit.catalog.TVFInfo;
import com.google.zetasql.toolkit.catalog.exceptions.CatalogResourceAlreadyExists;
import com.google.zetasql.toolkit.catalog.io.CatalogResources;
import com.google.zetasql.toolkit.catalog.spanner.exceptions.InvalidSpannerTableName;
import com.google.zetasql.toolkit.options.SpannerLanguageOptions;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** {@link CatalogWrapper} implementation that follows Cloud Spanner semantics */
public class SpannerCatalog implements CatalogWrapper {

  private final SpannerResourceProvider spannerResourceProvider;
  private final SimpleCatalog catalog;

  /**
   * Constructs a SpannerCatalog that uses the provided {@link SpannerResourceProvider} for fetching
   * Spanner tables.
   *
   * @param spannerResourceProvider The SpannerResourceProvider to use
   */
  public SpannerCatalog(SpannerResourceProvider spannerResourceProvider) {
    this.spannerResourceProvider = spannerResourceProvider;
    this.catalog = new SimpleCatalog("catalog");
    this.catalog.addZetaSQLFunctionsAndTypes(
        new ZetaSQLBuiltinFunctionOptions(SpannerLanguageOptions.get()));
    SpannerBuiltIns.addToCatalog(this.catalog);
  }

  /**
   * Constructs a SpannerCatalog given a Spanner project, instance and database. It uses a {@link
   * DatabaseClient} with application default credentials to access Spanner.
   *
   * @deprecated Use {@link SpannerCatalog#usingSpannerClient(String, String, String)}
   * @param projectId The Spanner project id
   * @param instance The Spanner instance name
   * @param database The Spanner database name
   */
  @Deprecated
  public SpannerCatalog(String projectId, String instance, String database) {
    this(SpannerResourceProviderImpl.buildDefault(projectId, instance, database));
  }

  /**
   * Constructs a SpannerCatalog that uses the provided {@link DatabaseClient} for accessing
   * Spanner.
   *
   * @deprecated Use {@link SpannerCatalog#usingSpannerClient(String, String, String, Spanner)}
   * @param projectId The Spanner project id
   * @param instance The Spanner instance name
   * @param database The Spanner database name
   * @param spannerClient The Spanner client to use
   */
  @Deprecated
  public SpannerCatalog(String projectId, String instance, String database, Spanner spannerClient) {
    this(SpannerResourceProviderImpl.build(projectId, instance, database, spannerClient));
  }

  /** Private constructor used for implementing {@link #copy()} */
  private SpannerCatalog(
      SpannerResourceProvider spannerResourceProvider, SimpleCatalog internalCatalog) {
    this.spannerResourceProvider = spannerResourceProvider;
    this.catalog = internalCatalog;
  }

  /**
   * Constructs a SpannerCatalog given a Spanner project, instance and database. It uses a {@link
   * DatabaseClient} with application default credentials to access Spanner.
   *
   * @param projectId The Spanner project id
   * @param instance The Spanner instance name
   * @param database The Spanner database name
   * @return the new SpannerCatalog instance
   */
  public static SpannerCatalog usingSpannerClient(
      String projectId, String instance, String database) {
    SpannerResourceProvider resourceProvider =
        SpannerResourceProviderImpl.buildDefault(projectId, instance, database);
    return new SpannerCatalog(resourceProvider);
  }

  /**
   * Constructs a SpannerCatalog that uses the provided {@link DatabaseClient} for accessing
   * Spanner.
   *
   * @param projectId The Spanner project id
   * @param instance The Spanner instance name
   * @param database The Spanner database name
   * @param spannerClient The Spanner client to use
   * @return the new SpannerCatalog instance
   */
  public static SpannerCatalog usingSpannerClient(
      String projectId, String instance, String database, Spanner spannerClient) {
    SpannerResourceProvider resourceProvider =
        SpannerResourceProviderImpl.build(projectId, instance, database, spannerClient);
    return new SpannerCatalog(resourceProvider);
  }

  /**
   * Constructs a SpannerCatalog that can use the tables in the provided {@link CatalogResources}
   * object.
   *
   * @param resources The {@link CatalogResources} object from which this catalog will get tables
   * @return the new SpannerCatalog instance
   */
  public static SpannerCatalog usingResources(CatalogResources resources) {
    SpannerResourceProvider resourceProvider = new LocalSpannerResourceProvider(resources);
    return new SpannerCatalog(resourceProvider);
  }

  /**
   * {@inheritDoc}
   *
   * @throws InvalidSpannerTableName if the table name is invalid for Spanner
   * @throws CatalogResourceAlreadyExists if the table already exists and CreateMode !=
   *     CREATE_OR_REPLACE
   */
  @Override
  public void register(SimpleTable table, CreateMode createMode, CreateScope createScope) {
    String fullName = table.getFullName();

    if (fullName.contains(".")) {
      throw new InvalidSpannerTableName(fullName);
    }

    CatalogOperations.createTableInCatalog(this.catalog, table.getFullName(), table, createMode);
  }

  @Override
  public void register(FunctionInfo function, CreateMode createMode, CreateScope createScope) {
    throw new UnsupportedOperationException(
        "Cloud Spanner does not support user-defined functions");
  }

  @Override
  public void register(TVFInfo tvfInfo, CreateMode createMode, CreateScope createScope) {
    throw new UnsupportedOperationException(
        "Cloud Spanner does not support table valued functions");
  }

  @Override
  public void register(
      ProcedureInfo procedureInfo, CreateMode createMode, CreateScope createScope) {
    throw new UnsupportedOperationException("Cloud Spanner does not support procedures");
  }

  @Override
  public void register(Constant constant) {
    throw new UnsupportedOperationException("Cloud Spanner does not support constants");
  }

  @Override
  public void removeTable(String table) {
    this.validateSpannerTableNames(ImmutableList.of(table));
    CatalogOperations.deleteTableFromCatalog(this.catalog, table);
  }

  @Override
  public void removeFunction(String function) {
    throw new UnsupportedOperationException(
        "Cloud Spanner does not support user-defined functions");
  }

  @Override
  public void removeTVF(String function) {
    throw new UnsupportedOperationException(
        "Cloud Spanner does not support table valued functions");
  }

  @Override
  public void removeProcedure(String procedure) {
    throw new UnsupportedOperationException("Cloud Spanner does not support procedures");
  }

  private void validateSpannerTableNames(List<String> tableNames) {
    tableNames.stream()
        .filter(tableName -> tableName.contains("."))
        .findAny()
        .ifPresent(
            invalidTableName -> {
              throw new InvalidSpannerTableName(invalidTableName);
            });
  }

  /**
   * {@inheritDoc}
   *
   * @throws InvalidSpannerTableName if any of the table names is invalid for Spanner
   */
  @Override
  public void addTables(List<String> tableNames) {
    this.validateSpannerTableNames(tableNames);

    List<String> tablesNotInCatalog =
        tableNames.stream()
            .filter(tableName -> Objects.isNull(this.catalog.getTable(tableName, null)))
            .collect(Collectors.toList());

    this.spannerResourceProvider
        .getTables(tablesNotInCatalog)
        .forEach(
            table ->
                this.register(
                    table, CreateMode.CREATE_OR_REPLACE, CreateScope.CREATE_DEFAULT_SCOPE));
  }

  public void addAllTablesInDatabase() {
    this.spannerResourceProvider
        .getAllTablesInDatabase()
        .forEach(
            table ->
                this.register(
                    table, CreateMode.CREATE_OR_REPLACE, CreateScope.CREATE_DEFAULT_SCOPE));
  }

  /**
   * Adds all the tables used in the provided query to this catalog.
   *
   * <p>Uses Analyzer.extractTableNamesFromScript to extract the table names and later uses
   * this.addTables to add them.
   *
   * @param query The SQL query from which to get the tables that should be added to the catalog
   * @param options The ZetaSQL AnalyzerOptions to use when extracting the table names from the
   *     query
   */
  public void addAllTablesUsedInQuery(String query, AnalyzerOptions options) {
    Set<String> tables =
        Analyzer.extractTableNamesFromScript(query, options).stream()
            .map(tablePath -> String.join(".", tablePath))
            .collect(Collectors.toSet());
    this.addTables(ImmutableList.copyOf(tables));
  }

  @Override
  public void addFunctions(List<String> functions) {
    throw new UnsupportedOperationException(
        "Cloud Spanner does not support user-defined functions");
  }

  @Override
  public void addTVFs(List<String> functions) {
    throw new UnsupportedOperationException(
        "Cloud Spanner does not support table valued functions");
  }

  @Override
  public void addProcedures(List<String> procedures) {
    throw new UnsupportedOperationException("Cloud Spanner does not support procedures");
  }

  @Override
  public SpannerCatalog copy() {
    return new SpannerCatalog(
        this.spannerResourceProvider, CatalogOperations.copyCatalog(this.catalog));
  }

  @Override
  public SimpleCatalog getZetaSQLCatalog() {
    return this.catalog;
  }
}
