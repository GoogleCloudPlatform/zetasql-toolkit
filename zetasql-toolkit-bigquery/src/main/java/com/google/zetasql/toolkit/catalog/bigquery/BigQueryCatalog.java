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

package com.google.zetasql.toolkit.catalog.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonParseException;
import com.google.zetasql.*;
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.CreateMode;
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.CreateScope;
import com.google.zetasql.toolkit.AnalyzerExtensions;
import com.google.zetasql.toolkit.catalog.CatalogOperations;
import com.google.zetasql.toolkit.catalog.CatalogWrapper;
import com.google.zetasql.toolkit.catalog.FunctionInfo;
import com.google.zetasql.toolkit.catalog.ProcedureInfo;
import com.google.zetasql.toolkit.catalog.TVFInfo;
import com.google.zetasql.toolkit.catalog.bigquery.exceptions.BigQueryCreateError;
import com.google.zetasql.toolkit.catalog.bigquery.exceptions.MissingFunctionResultType;
import com.google.zetasql.toolkit.catalog.exceptions.CatalogResourceAlreadyExists;
import com.google.zetasql.toolkit.catalog.io.CatalogResources;
import com.google.zetasql.toolkit.options.BigQueryLanguageOptions;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link CatalogWrapper} implementation that follows BigQuery semantics. Facilitates building a
 * ZetaSQL {@link SimpleCatalog} with BigQuery resources and following BigQuery semantics for types
 * and name resolution.
 */
public class BigQueryCatalog implements CatalogWrapper {

  private final String defaultProjectId;
  private final BigQueryResourceProvider bigQueryResourceProvider;
  private final SimpleCatalog catalog;

  /**
   * Constructs a BigQueryCatalog that fetches resources from the BigQuery API using application
   * default credentials.
   *
   * @deprecated Use {@link BigQueryCatalog#usingBigQueryAPI(String)}
   *
   * @param defaultProjectId The BigQuery default project id, queries are assumed to be running on
   *     this project
   */
  @Deprecated
  public BigQueryCatalog(String defaultProjectId) {
    this(defaultProjectId, BigQueryAPIResourceProvider.buildDefault());
  }

  /**
   * Constructs a BigQueryCatalog that fetches resources from the BigQuery API using the provided
   * BigQuery Client.
   *
   * @deprecated Use {@link BigQueryCatalog#usingBigQueryAPI(String, BigQuery)}
   *
   * @param defaultProjectId The BigQuery default project id, queries are assumed to be running on
   *     this project
   * @param bigQueryClient The BigQuery client to use for accessing the API
   */
  @Deprecated
  public BigQueryCatalog(String defaultProjectId, BigQuery bigQueryClient) {
    this(defaultProjectId, BigQueryAPIResourceProvider.build(bigQueryClient));
  }

  /**
   * Constructs a BigQueryCatalog that uses the provided {@link BigQueryResourceProvider} for
   * getting resources.
   *
   * @param defaultProjectId The BigQuery default project id, queries are assumed to be running on
   *     this project
   * @param bigQueryResourceProvider The BigQueryResourceProvider this catalog will use to get
   *     resources
   */
  public BigQueryCatalog(
      String defaultProjectId, BigQueryResourceProvider bigQueryResourceProvider) {
    this.defaultProjectId = defaultProjectId;
    this.bigQueryResourceProvider = bigQueryResourceProvider;
    this.catalog = new SimpleCatalog("catalog");
    this.catalog.addZetaSQLFunctionsAndTypes(
        new ZetaSQLBuiltinFunctionOptions(BigQueryLanguageOptions.get()));
    BigQueryBuiltIns.addToCatalog(this.catalog);
  }

  /** Private constructor used for implementing {@link #copy()} */
  private BigQueryCatalog(
      String defaultProjectId,
      BigQueryResourceProvider bigQueryResourceProvider,
      SimpleCatalog internalCatalog) {
    this.defaultProjectId = defaultProjectId;
    this.bigQueryResourceProvider = bigQueryResourceProvider;
    this.catalog = internalCatalog;
  }

  /**
   * Constructs a BigQueryCatalog that fetches resources from the BigQuery API using application
   * default credentials.
   *
   * <p>A BigQueryCatalog constructed this way will use the default {@link
   * BigQueryAPIResourceProvider} for accessing the API.
   *
   * @param defaultProjectId The BigQuery default project id, queries are assumed to be running on
   *     this project
   */
  public static BigQueryCatalog usingBigQueryAPI(String defaultProjectId) {
    BigQueryResourceProvider resourceProvider = BigQueryAPIResourceProvider.buildDefault();
    return new BigQueryCatalog(defaultProjectId, resourceProvider);
  }

  /**
   * Constructs a BigQueryCatalog that fetches resources from the BigQuery API using the provided
   * BigQuery Client.
   *
   * <p>A BigQueryCatalog constructed this way will use the {@link BigQueryAPIResourceProvider} for
   * accessing the API using the provided BigQuery client.
   *
   * @param defaultProjectId The BigQuery default project id, queries are assumed to be running on
   *     this project
   * @param bigQueryClient The BigQuery client to use for accessing the API
   */
  public static BigQueryCatalog usingBigQueryAPI(String defaultProjectId, BigQuery bigQueryClient) {
    BigQueryResourceProvider resourceProvider = BigQueryAPIResourceProvider.build(bigQueryClient);
    return new BigQueryCatalog(defaultProjectId, resourceProvider);
  }

  /**
   * Constructs a BigQueryCatalog that can use the resources in the provided
   * {@link CatalogResources} object.
   *
   * @param defaultProjectId The BigQuery default project id, queries are assumed to be running on
   *     this project
   * @param resources The {@link CatalogResources} object from which this catalog will get resources
   */
  public static BigQueryCatalog usingResources(String defaultProjectId, CatalogResources resources)
      throws JsonParseException {
    LocalBigQueryResourceProvider resourceProvider = new LocalBigQueryResourceProvider(resources);
    return new BigQueryCatalog(defaultProjectId, resourceProvider);
  }

  /**
   * Returns whether the table referenced by the provided reference exists
   * in the catalog.
   *
   * @param reference The reference to check (e.g. "project.dataset.table")
   * @return Whether there's a table in the catalog referenced by the reference
   */
  private boolean tableExistsInCatalog(String reference) {
    return Objects.nonNull(this.catalog.getTable(reference, null));
  }

  /**
   * Returns whether the function referenced by the provided reference exists
   * in the catalog.
   *
   * @param reference The reference to check (e.g. "project.dataset.function")
   * @return Whether there's a function in the catalog referenced by the reference
   */
  private boolean functionExistsInCatalog(String reference) {
    try {
      this.catalog.findFunction(ImmutableList.of(reference));
      return true;
    } catch (NotFoundException err) {
      return false;
    }
  }

  /**
   * Returns whether the TVF referenced by the provided reference exists
   * in the catalog.
   *
   * @param reference The reference to check (e.g. "project.dataset.tvf")
   * @return Whether there's a TVF in the catalog referenced by the reference
   */
  private boolean tvfExistsInCatalog(String reference) {
    return Objects.nonNull(this.catalog.getTVFByName(reference));
  }

  /**
   * Returns whether the procedure referenced by the provided reference exists
   * in the catalog.
   *
   * @param reference The reference to check (e.g. "project.dataset.procedure")
   * @return Whether there's a procedure in the catalog referenced by the reference
   */
  private boolean procedureExistsInCatalog(String reference) {
    try {
      this.catalog.findProcedure(ImmutableList.of(reference));
      return true;
    } catch (NotFoundException err) {
      return false;
    }
  }

  /**
   * Validates that a {@link CreateScope} is in a list of allowed scopes, used before creation of
   * resources. Throws {@link BigQueryCreateError} in case the scope is not allowed.
   *
   * @param scope The CreateScope to be validated
   * @param allowedScopes The list of allowed CreateScopes
   * @param resourceFullName The full name of the resource being created, used for error reporting
   * @param resourceType The name of the type of resource being created, used for error reporting
   * @throws BigQueryCreateError if the validation fails
   */
  private void validateCreateScope(
      CreateScope scope,
      List<CreateScope> allowedScopes,
      String resourceFullName,
      String resourceType) {
    if (!allowedScopes.contains(scope)) {
      String message =
          String.format(
              "Invalid create scope %s for BigQuery %s %s", scope, resourceType, resourceFullName);
      throw new BigQueryCreateError(message, scope, resourceFullName);
    }
  }

  /**
   * Validates a resources name path before its creation. If the name path is invalid, throws {@link
   * BigQueryCreateError}.
   *
   * @param namePath The name path to be validated
   * @param createScope The CreateScope used to create this resource
   * @param resourceType he name of the type of resource being created, used for error reporting
   * @throws BigQueryCreateError if the validation fails
   */
  private void validateNamePathForCreation(
      List<String> namePath, CreateScope createScope, String resourceType) {
    String fullName = String.join(".", namePath);
    List<String> flattenedNamePath =
        namePath.stream()
            .flatMap(pathElement -> Arrays.stream(pathElement.split("\\.")))
            .collect(Collectors.toList());

    // Names for TEMP BigQuery resources should not be qualified
    if (createScope.equals(CreateScope.CREATE_TEMP) && flattenedNamePath.size() > 1) {
      String message =
          String.format(
              "Cannot create BigQuery TEMP %s %s, TEMP resources should not be qualified",
              resourceType, fullName);
      throw new BigQueryCreateError(message, createScope, fullName);
    }

    // Names for persistent BigQuery resources should be qualified
    if (!createScope.equals(CreateScope.CREATE_TEMP) && flattenedNamePath.size() == 1) {
      String message =
          String.format(
              "Cannot create BigQuery %s %s, persistent BigQuery resources should be qualified",
              resourceType, fullName);
      throw new BigQueryCreateError(message, createScope, fullName);
    }
  }

  /**
   * Returns the names a resource referenced by the provided string should have in the underlying
   * {@link SimpleCatalog}.
   *
   * <p> If the reference is qualified, its complete name in the catalog will be in the form of
   * "project.dataset.resource". If the resource is in the default project, an additional name
   * in the form of "dataset.resource" will be returned.
   *
   * <p> For unqualified resource (e.g. temporary tables), the name will be used as-is.
   *
   * @param reference The string used to reference the resource (e.g. "project.dataset.table",
   * "dataset.function", "tableName")
   * @return The list of names the resource should have in the underlying {@link SimpleCatalog}
   */
  private List<String> buildCatalogNamesForResource(String reference) {
    boolean isQualified = BigQueryReference.isQualified(reference);

    if (!isQualified) {
      return ImmutableList.of(reference);
    }

    BigQueryReference parsedReference =
        BigQueryReference.from(this.defaultProjectId, reference);
    boolean isInDefaultProject =
        parsedReference.getProjectId().equalsIgnoreCase(this.defaultProjectId);

    if (isInDefaultProject) {
      return ImmutableList.of(parsedReference.getFullName(), parsedReference.getNameWithDataset());
    } else {
      return ImmutableList.of(parsedReference.getFullName());
    }
  }

  /**
   * Creates the list of resource paths that should be used for creating a resource to make sure it
   * is always found when analyzing queries.
   *
   * <p>Given the way the ZetaSQL {@link SimpleCatalog} resolves names, different ways of
   * referencing resources while querying results in different lookups. For example; "SELECT * FROM
   * `A.B`" will look for a table named "A.B", while "SELECT * FROM `A`.`B`" will look for a table
   * named "B" on a catalog named "A".
   *
   * <p>Because of the previous point, a table or function being created needs to be registered in
   * multiple paths. This method creates all those distinct paths. Given the resource
   * "project.dataset.resource", this method will create these paths:
   *
   * <ul>
   *   <li>["project.dataset.resource"]
   *   <li>["project", "dataset", "resource"]
   *   <li>["project", "dataset.resource"]
   *   <li>["project.dataset", "resource"]
   *   <li>["dataset.resource"] if the resource project is this catalog's default project id
   *   <li>["dataset", "resource"] if the resource project is this catalog's default project id
   * </ul>
   *
   * @param reference The BigQueryReference for the resource that needs to be created
   * @return All the distinct name paths at which the resource should be created
   */
  private List<List<String>> buildCatalogPathsForResource(BigQueryReference reference) {
    String projectId = reference.getProjectId();
    String datasetName = reference.getDatasetId();
    String resourceName = reference.getResourceName();

    List<List<String>> resourcePaths =
        ImmutableList.of(
            ImmutableList.of(projectId, datasetName, resourceName), // format: project.dataset.table format
            ImmutableList.of(
                projectId
                    + "."
                    + datasetName
                    + "."
                    + resourceName), // format: `project.dataset.table`
            ImmutableList.of(projectId + "." + datasetName, resourceName), // format: `project.dataset`.table
            ImmutableList.of(projectId, datasetName + "." + resourceName) // format: project.`dataset.table`
            );

    List<List<String>> resourcePathsWithImplicitProject = ImmutableList.of();

    if (projectId.equals(this.defaultProjectId)) {
      resourcePathsWithImplicitProject =
          ImmutableList.of(
              ImmutableList.of(datasetName, resourceName), // format: dataset.table (project implied)
              ImmutableList.of(datasetName + "." + resourceName) // format: `dataset.table` (project implied)
              );
    }

    return Stream.concat(resourcePaths.stream(), resourcePathsWithImplicitProject.stream())
        .collect(Collectors.toList());
  }

  /** @see #buildCatalogPathsForResource(BigQueryReference) */
  private List<List<String>> buildCatalogPathsForResource(String referenceStr) {
    BigQueryReference reference = BigQueryReference.from(this.defaultProjectId, referenceStr);
    return this.buildCatalogPathsForResource(reference);
  }

  /** @see #buildCatalogPathsForResource(BigQueryReference) */
  private List<List<String>> buildCatalogPathsForResource(List<String> resourcePath) {
    return this.buildCatalogPathsForResource(String.join(".", resourcePath));
  }

  private CatalogResourceAlreadyExists addCaseInsensitivityWarning(
      CatalogResourceAlreadyExists error) {
    return new CatalogResourceAlreadyExists(
        error.getResourceName(),
        String.format(
            "Catalog resource already exists: %s. BigQuery resources are treated as "
                + "case-insensitive, so resources with the same name but different casing "
                + "are treated as equals and can trigger this error.",
            error.getResourceName()),
        error);
  }

  /**
   * {@inheritDoc}
   *
   * <p> If the table is not temporary and is in the catalog's default project, it will be
   * registered twice. Once as "project.dataset.table" and once as "dataset.table". That way,
   * queries that omit the project when referencing the table can be analyzed.
   *
   * @throws BigQueryCreateError if a pre-create validation fails
   * @throws CatalogResourceAlreadyExists if the table already exists and CreateMode !=
   *     CREATE_OR_REPLACE
   */
  @Override
  public void register(SimpleTable table, CreateMode createMode, CreateScope createScope) {
    this.validateCreateScope(
        createScope,
        ImmutableList.of(CreateScope.CREATE_DEFAULT_SCOPE, CreateScope.CREATE_TEMP),
        table.getFullName(),
        "table");
    this.validateNamePathForCreation(
        ImmutableList.of(table.getFullName()), createScope, "table");

    List<String> catalogNamesForTable = buildCatalogNamesForResource(table.getFullName());

    try {
      catalogNamesForTable.forEach(catalogName ->
          CatalogOperations.createTableInCatalog(catalog, catalogName, table, createMode));
    } catch (CatalogResourceAlreadyExists alreadyExists) {
      throw this.addCaseInsensitivityWarning(alreadyExists);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p> If the function is not temporary and is in the catalog's default project, it will be
   * registered twice. Once as "project.dataset.function" and once as "dataset.function". That way,
   * queries that omit the project when referencing the table can be analyzed.
   *
   * <p>If any function signature which does not include a valid return type, this method will
   * attempt to infer it. If inference is not possible, {@link MissingFunctionResultType} will be
   * thrown.
   *
   * @throws BigQueryCreateError if a pre-create validation fails
   * @throws CatalogResourceAlreadyExists if the function already exists and CreateMode !=
   *     CREATE_OR_REPLACE
   * @throws MissingFunctionResultType if the function does not have an explicit return type and if
   *     it cannot be automatically inferred
   */
  @Override
  public void register(FunctionInfo function, CreateMode createMode, CreateScope createScope) {
    List<String> functionNamePath = function.getNamePath();
    String fullName = String.join(".", functionNamePath);

    this.validateCreateScope(
        createScope,
        ImmutableList.of(CreateScope.CREATE_DEFAULT_SCOPE, CreateScope.CREATE_TEMP),
        fullName,
        "function");
    this.validateNamePathForCreation(functionNamePath, createScope, "function");

    FunctionInfo resolvedFunction =
        FunctionResultTypeResolver.resolveFunctionReturnTypes(
            function, BigQueryLanguageOptions.get(), this.catalog);

    List<String> catalogNamesForFunction = buildCatalogNamesForResource(fullName);

    try {
      catalogNamesForFunction.forEach(catalogName ->
          CatalogOperations.createFunctionInCatalog(
              catalog, catalogName, resolvedFunction, createMode));
    } catch (CatalogResourceAlreadyExists alreadyExists) {
      throw this.addCaseInsensitivityWarning(alreadyExists);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p> If the function is not temporary and is in the catalog's default project, it will be
   * registered twice. Once as "project.dataset.function" and once as "dataset.function". That way,
   * queries that omit the project when referencing the table can be analyzed.
   *
   * @throws BigQueryCreateError if a pre-create validation fails
   * @throws CatalogResourceAlreadyExists if the function already exists and CreateMode !=
   *     CREATE_OR_REPLACE
   */
  @Override
  public void register(TVFInfo tvfInfo, CreateMode createMode, CreateScope createScope) {
    String fullName = String.join(".", tvfInfo.getNamePath());

    this.validateCreateScope(
        createScope, ImmutableList.of(CreateScope.CREATE_DEFAULT_SCOPE), fullName, "TVF");
    this.validateNamePathForCreation(tvfInfo.getNamePath(), createScope, "TVF");

    TVFInfo resolvedTvfInfo =
        FunctionResultTypeResolver.resolveTVFOutputSchema(
            tvfInfo, BigQueryLanguageOptions.get(), this.catalog);

    List<String> catalogNamesForFunction = buildCatalogNamesForResource(fullName);

    try {
      catalogNamesForFunction.forEach(catalogName ->
          CatalogOperations.createTVFInCatalog(catalog, catalogName, resolvedTvfInfo, createMode));
    } catch (CatalogResourceAlreadyExists alreadyExists) {
      throw this.addCaseInsensitivityWarning(alreadyExists);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Multiple copies of the registered {@link Procedure} will be created in the Catalog to comply
   * with BigQuery name resolution semantics.
   *
   * @see #buildCatalogPathsForResource(BigQueryReference)
   * @throws BigQueryCreateError if a pre-create validation fails
   * @throws CatalogResourceAlreadyExists if the precedure already exists and CreateMode !=
   *     CREATE_OR_REPLACE
   */
  @Override
  public void register(
      ProcedureInfo procedureInfo, CreateMode createMode, CreateScope createScope) {
    String fullName = String.join(".", procedureInfo.getNamePath());

    this.validateCreateScope(
        createScope, ImmutableList.of(CreateScope.CREATE_DEFAULT_SCOPE), fullName, "procedure");
    this.validateNamePathForCreation(ImmutableList.of(fullName), createScope, "procedure");

    List<List<String>> procedurePaths = this.buildCatalogPathsForResource(fullName);

    try {
      CatalogOperations.createProcedureInCatalog(
          this.catalog, procedurePaths, procedureInfo, createMode);
    } catch (CatalogResourceAlreadyExists alreadyExists) {
      throw this.addCaseInsensitivityWarning(alreadyExists);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalArgumentException if the constant name is qualified
   * @throws CatalogResourceAlreadyExists if the constant already exits in this catalog
   */
  @Override
  public void register(Constant constant) {
    String fullName = constant.getFullName();

    if (constant.getNamePath().size() > 1) {
      throw new IllegalArgumentException(
          "BigQuery constants cannot be qualified, was: " + fullName);
    }

    boolean constantExists = this.catalog.getConstantList()
        .stream()
        .anyMatch(existingConstant -> existingConstant.getFullName().equalsIgnoreCase(fullName));

    if (constantExists) {
      throw new CatalogResourceAlreadyExists(
          fullName, "Constant " + fullName + "already exists");
    }

    this.catalog.addConstant(constant);
  }

  @Override
  public void removeTable(String tableReference) {
    List<String> catalogNamesForTable = buildCatalogNamesForResource(tableReference);

    catalogNamesForTable.forEach(catalogName ->
        CatalogOperations.deleteTableFromCatalog(catalog, catalogName));
  }

  @Override
  public void removeFunction(String functionReference) {
    List<String> catalogNamesForFunction = buildCatalogNamesForResource(functionReference);

    catalogNamesForFunction.forEach(catalogName ->
        CatalogOperations.deleteFunctionFromCatalog(catalog, catalogName));
  }

  @Override
  public void removeTVF(String functionReference) {
    List<String> catalogNamesForFunction = buildCatalogNamesForResource(functionReference);

    catalogNamesForFunction.forEach(catalogName ->
        CatalogOperations.deleteTVFFromCatalog(catalog, catalogName));
  }

  @Override
  public void removeProcedure(String procedureReference) {
    List<List<String>> functionPaths = this.buildCatalogPathsForResource(procedureReference);
    CatalogOperations.deleteProcedureFromCatalog(this.catalog, functionPaths);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Table references should be in the format "project.dataset.table" or "dataset.table"
   */
  @Override
  public void addTables(List<String> tableReferences) {
    List<String> tablesNotInCatalog = tableReferences.stream()
        .filter(tableRef -> !this.tableExistsInCatalog(tableRef))
        .collect(Collectors.toList());

    this.bigQueryResourceProvider
        .getTables(this.defaultProjectId, tablesNotInCatalog)
        .forEach(
            table ->
                this.register(
                    table, CreateMode.CREATE_OR_REPLACE, CreateScope.CREATE_DEFAULT_SCOPE));
  }

  /**
   * Adds all tables in the provided dataset to this catalog
   *
   * @param projectId The project id the dataset belongs to
   * @param datasetName The name of the dataset to get tables from
   */
  public void addAllTablesInDataset(String projectId, String datasetName) {
    this.bigQueryResourceProvider
        .getAllTablesInDataset(projectId, datasetName)
        .forEach(
            table ->
                this.register(
                    table, CreateMode.CREATE_OR_REPLACE, CreateScope.CREATE_DEFAULT_SCOPE));
  }

  /**
   * Adds all tables in the provided project to this catalog
   *
   * @param projectId The project id to get tables from
   */
  public void addAllTablesInProject(String projectId) {
    this.bigQueryResourceProvider
        .getAllTablesInProject(projectId)
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
            .filter(BigQueryReference::isQualified) // Remove non-qualified tables
            .collect(Collectors.toSet());
    this.addTables(ImmutableList.copyOf((tables)));
  }

  /**
   * {@inheritDoc}
   *
   * <p>Function references should be in the format "project.dataset.function" or "dataset.function"
   *
   * <p>If any function signature which does not include a valid return type, this method will
   * attempt to infer it. If inference is not possible, {@link MissingFunctionResultType} will be
   * thrown.
   */
  @Override
  public void addFunctions(List<String> functionReferences) {
    List<String> functionsNotInCatalog = functionReferences.stream()
        .filter(functionRef -> !this.functionExistsInCatalog(functionRef))
        .collect(Collectors.toList());

    this.bigQueryResourceProvider
        .getFunctions(this.defaultProjectId, functionsNotInCatalog)
        .forEach(
            function ->
                this.register(
                    function, CreateMode.CREATE_OR_REPLACE, CreateScope.CREATE_DEFAULT_SCOPE));
  }

  /**
   * Attempts to resolve the return type for a function, ignores it if resolution fails.
   *
   * <p>Meant to be used as a mapping function for {@link Stream#flatMap(Function)}
   *
   * @param functionInfo The {@link FunctionInfo} representing the function for which return types
   *     should be resolved
   * @return A {@link Stream} containing the resolved FunctionInfo if resolution was successful, an
   *     empty stream otherwise
   */
  private Stream<FunctionInfo> resolveFunctionReturnTypeWhenPossible(FunctionInfo functionInfo) {
    try {
      FunctionInfo resolvedFunctionInfo =
          FunctionResultTypeResolver.resolveFunctionReturnTypes(
              functionInfo, BigQueryLanguageOptions.get(), this.catalog);
      return Stream.of(resolvedFunctionInfo);
    } catch (MissingFunctionResultType err) {
      return Stream.of();
    }
  }

  /**
   * Adds all functions in the provided dataset to this catalog
   *
   * <p>Functions for which a proper return type cannot be determined are silently ignored
   *
   * @param projectId The project id the dataset belongs to
   * @param datasetName The name of the dataset to get functions from
   */
  public void addAllFunctionsInDataset(String projectId, String datasetName) {
    this.bigQueryResourceProvider.getAllFunctionsInDataset(projectId, datasetName).stream()
        .flatMap(this::resolveFunctionReturnTypeWhenPossible)
        .forEach(
            function ->
                this.register(
                    function, CreateMode.CREATE_OR_REPLACE, CreateScope.CREATE_DEFAULT_SCOPE));
  }

  /**
   * Adds all functions in the provided project to this catalog
   *
   * <p>Functions for which a proper return type cannot be determined are silently ignored
   *
   * @param projectId The project id to get functions from
   */
  public void addAllFunctionsInProject(String projectId) {
    this.bigQueryResourceProvider.getAllFunctionsInProject(projectId).stream()
        .flatMap(this::resolveFunctionReturnTypeWhenPossible)
        .forEach(
            function ->
                this.register(
                    function, CreateMode.CREATE_OR_REPLACE, CreateScope.CREATE_DEFAULT_SCOPE));
  }

  /**
   * Adds all the functions used in the provided query to this catalog.
   *
   * <p>Uses {@link AnalyzerExtensions#extractFunctionNamesFromScript(String, LanguageOptions)} to
   * extract the functions names and later uses {@link #addFunctions(List)} to add them.
   *
   * @param query The SQL query from which to get the functions that should be added to the catalog
   */
  public void addAllFunctionsUsedInQuery(String query) {
    Set<String> functions =
        AnalyzerExtensions.extractFunctionNamesFromScript(query, BigQueryLanguageOptions.get())
            .stream()
            .map(functionPath -> String.join(".", functionPath))
            .filter(BigQueryReference::isQualified) // Remove non-qualified functions
            .collect(Collectors.toSet());
    this.addFunctions(ImmutableList.copyOf((functions)));
  }

  /**
   * {@inheritDoc}
   *
   * <p>Function references should be in the format "project.dataset.function" or "dataset.function"
   */
  @Override
  public void addTVFs(List<String> functionReferences) {
    List<String> functionsNotInCatalog = functionReferences.stream()
        .filter(functionRef -> !this.tvfExistsInCatalog(functionRef))
        .collect(Collectors.toList());

    this.bigQueryResourceProvider
        .getTVFs(this.defaultProjectId, functionsNotInCatalog)
        .forEach(
            tvfInfo ->
                this.register(
                    tvfInfo, CreateMode.CREATE_OR_REPLACE, CreateScope.CREATE_DEFAULT_SCOPE));
  }

  /**
   * Attempts to resolve the output schema for a TVF, ignores it if resolution fails.
   *
   * <p>Meant to be used as a mapping function for {@link Stream#flatMap(Function)}
   *
   * @param tvfInfo The {@link TVFInfo} representing the TVF for which the output schema should be
   *     resolved
   * @return A {@link Stream} containing the resolved TVFInfo if resolution was successful, an empty
   *     stream otherwise
   */
  private Stream<TVFInfo> resolveTVFResultTypeWhenPossible(TVFInfo tvfInfo) {
    try {
      TVFInfo resolvedTVFInfo =
          FunctionResultTypeResolver.resolveTVFOutputSchema(
              tvfInfo, BigQueryLanguageOptions.get(), this.catalog);
      return Stream.of(resolvedTVFInfo);
    } catch (MissingFunctionResultType err) {
      return Stream.of();
    }
  }

  /**
   * Adds all TVFs in the provided dataset to this catalog
   *
   * @param projectId The project id the dataset belongs to
   * @param datasetName The name of the dataset to get TVFs from
   */
  public void addAllTVFsInDataset(String projectId, String datasetName) {
    this.bigQueryResourceProvider.getAllTVFsInDataset(projectId, datasetName).stream()
        .flatMap(this::resolveTVFResultTypeWhenPossible)
        .forEach(
            tvfInfo ->
                this.register(
                    tvfInfo, CreateMode.CREATE_OR_REPLACE, CreateScope.CREATE_DEFAULT_SCOPE));
  }

  /**
   * Adds all TVFs in the provided project to this catalog
   *
   * @param projectId The project id to get TVFs from
   */
  public void addAllTVFsInProject(String projectId) {
    this.bigQueryResourceProvider.getAllTVFsInProject(projectId).stream()
        .flatMap(this::resolveTVFResultTypeWhenPossible)
        .forEach(
            tvfInfo ->
                this.register(
                    tvfInfo, CreateMode.CREATE_OR_REPLACE, CreateScope.CREATE_DEFAULT_SCOPE));
  }

  /**
   * Adds all the TVFs used in the provided query to this catalog.
   *
   * <p>Uses {@link AnalyzerExtensions#extractTVFNamesFromScript(String, LanguageOptions)} to
   * extract the TVF names and later uses {@link #addTVFs} to add them.
   *
   * @param query The SQL query from which to get the TVFs that should be added to the catalog
   */
  public void addAllTVFsUsedInQuery(String query) {
    Set<String> functions =
        AnalyzerExtensions.extractTVFNamesFromScript(query, BigQueryLanguageOptions.get()).stream()
            .map(functionPath -> String.join(".", functionPath))
            .filter(BigQueryReference::isQualified) // Remove non-qualified functions
            .collect(Collectors.toSet());
    this.addTVFs(ImmutableList.copyOf((functions)));
  }

  /**
   * {@inheritDoc}
   *
   * <p>Procedure references should be in the format "project.dataset.procedure" or
   * "dataset.procedure"
   */
  @Override
  public void addProcedures(List<String> procedureReferences) {
    List<String> proceduresNotInCatalog = procedureReferences.stream()
        .filter(procedureRef -> !this.procedureExistsInCatalog(procedureRef))
        .collect(Collectors.toList());

    this.bigQueryResourceProvider
        .getProcedures(this.defaultProjectId, proceduresNotInCatalog)
        .forEach(
            procedureInfo ->
                this.register(
                    procedureInfo, CreateMode.CREATE_OR_REPLACE, CreateScope.CREATE_DEFAULT_SCOPE));
  }

  /**
   * Adds all procedures in the provided dataset to this catalog
   *
   * @param projectId The project id the dataset belongs to
   * @param datasetName The name of the dataset to get procedures from
   */
  public void addAllProceduresInDataset(String projectId, String datasetName) {
    this.bigQueryResourceProvider
        .getAllProceduresInDataset(projectId, datasetName)
        .forEach(
            procedureInfo ->
                this.register(
                    procedureInfo, CreateMode.CREATE_OR_REPLACE, CreateScope.CREATE_DEFAULT_SCOPE));
  }

  /**
   * Adds all procedures in the provided project to this catalog
   *
   * @param projectId The project id to get procedures from
   */
  public void addAllProceduresInProject(String projectId) {
    this.bigQueryResourceProvider
        .getAllProceduresInProject(projectId)
        .forEach(
            procedureInfo ->
                this.register(
                    procedureInfo, CreateMode.CREATE_OR_REPLACE, CreateScope.CREATE_DEFAULT_SCOPE));
  }

  /**
   * Adds all the procedures called in the provided query to this catalog.
   *
   * <p>Uses {@link AnalyzerExtensions#extractProcedureNamesFromScript} to extract the procedure
   * names and later uses {@link #addProcedures(List)} to add them.
   *
   * @param query The SQL query from which to get the procedures that should be added to the catalog
   */
  public void addAllProceduresUsedInQuery(String query) {
    Set<String> procedures =
        AnalyzerExtensions.extractProcedureNamesFromScript(query, BigQueryLanguageOptions.get())
            .stream()
            .map(functionPath -> String.join(".", functionPath))
            .filter(BigQueryReference::isQualified) // Remove non-qualified functions
            .collect(Collectors.toSet());

    this.addProcedures(ImmutableList.copyOf((procedures)));
  }

  /**
   * Adds all the resources used in the provided query to this catalog. Includes tables, functions,
   * TVFs and procedures.
   *
   * <p> It calls {@link #addAllTablesUsedInQuery(String, AnalyzerOptions)},
   * {@link #addAllFunctionsUsedInQuery(String)}, {@link #addAllTVFsUsedInQuery(String)}
   * and {@link #addAllProceduresUsedInQuery(String)}.
   *
   * @param query The SQL query from which to get the resources that should be added to the catalog
   * @param options The ZetaSQL AnalyzerOptions to use when extracting the resource names from the
   *     query
   */
  public void addAllResourcesUsedInQuery(String query, AnalyzerOptions options) {
    addAllTablesUsedInQuery(query, options);
    addAllFunctionsUsedInQuery(query);
    addAllTVFsUsedInQuery(query);
    addAllProceduresUsedInQuery(query);
  }

  @Override
  public BigQueryCatalog copy() {
    return new BigQueryCatalog(
        this.defaultProjectId,
        this.bigQueryResourceProvider,
        CatalogOperations.copyCatalog(this.getZetaSQLCatalog()));
  }

  @Override
  public SimpleCatalog getZetaSQLCatalog() {
    return this.catalog;
  }
}
