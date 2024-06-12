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

package com.google.zetasql.toolkit.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.zetasql.*;
import com.google.zetasql.TableValuedFunction.FixedOutputSchemaTVF;
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.CreateMode;
import com.google.zetasql.toolkit.catalog.exceptions.CatalogResourceAlreadyExists;
import com.google.zetasql.toolkit.catalog.exceptions.CatalogResourceDoesNotExist;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Utility class that exposes static methods for performing various operations on ZetaSQL
 * SimpleCatalogs and related resources. Supports:
 *
 * <ul>
 *   <li>Building properly configured SimpleTable objects
 *   <li>Adding tables, functions, TVFs and procedures to SimpleCatalogs
 *   <li>Creating copies of SimpleCatalogs
 * </ul>
 */
public class CatalogOperations {

  private CatalogOperations() {}

  /** Get a child catalog from an existing catalog, creating it if it does not exist */
  private static SimpleCatalog getOrCreateNestedCatalog(SimpleCatalog parent, String name) {
    Optional<SimpleCatalog> maybeExistingCatalog =
        parent.getCatalogList().stream()
            .filter(catalog -> catalog.getFullName().equalsIgnoreCase(name))
            .findFirst();

    return maybeExistingCatalog.orElseGet(() -> parent.addNewSimpleCatalog(name));
  }

  /** Returns true if a table with path tablePath exists in the SimpleCatalog */
  private static boolean tableExists(SimpleCatalog catalog, List<String> tablePath) {
    try {
      catalog.findTable(tablePath);
      return true;
    } catch (NotFoundException err) {
      return false;
    }
  }

  /** Returns true if a table named tableName exists in the SimpleCatalog */
  private static boolean tableExists(SimpleCatalog catalog, String tableName) {
    return tableExists(catalog, ImmutableList.of(tableName));
  }

  private static String removeGroupFromFunctionName(String functionName) {
    return functionName.substring(functionName.indexOf(":") + 1);
  }

  /** Returns true if a function with the provided fullName exists in the SimpleCatalog */
  private static boolean functionExists(SimpleCatalog catalog, String fullName) {
    // TODO: switch to using Catalog.findFunction once available
    String fullNameWithoutGroup = removeGroupFromFunctionName(fullName);
    return catalog.getFunctionNameList().stream()
        .map(CatalogOperations::removeGroupFromFunctionName)
        .anyMatch(fullNameWithoutGroup::equalsIgnoreCase);
  }

  /** Returns true if the TVF named tvfName exists in the SimpleCatalog */
  private static boolean tvfExists(SimpleCatalog catalog, String tvfName) {
    return catalog.getTVFNameList().contains(tvfName.toLowerCase());
  }

  /** Returns true if the named procedureName exists in the SimpleCatalog */
  private static boolean procedureExists(SimpleCatalog catalog, String procedureName) {
    return catalog.getProcedureList().stream()
        .map(Procedure::getName)
        .anyMatch(name -> name.equalsIgnoreCase(procedureName));
  }

  /**
   * Gets the SimpleCatalog in which a resource should be created, based on the root catalog and the
   * resource path.
   *
   * <p>The path for the resource determines whether it should be created in the root catalog itself
   * or in a nested catalog. For example; a resource with the path ["A.B"] should be created in the
   * root catalog, but a resource with the path ["A", "B"] should be created in an "A" catalog
   * nested in the root catalog.
   *
   * @param rootCatalog The root SimpleCatalog the analyzer will use
   * @param resourcePath The path for the resource
   * @return The SimpleCatalog object where the resource should be created
   */
  private static SimpleCatalog getSubCatalogForResource(
      SimpleCatalog rootCatalog, List<String> resourcePath) {
    if (resourcePath.size() > 1) {
      String nestedCatalogName = resourcePath.get(0);
      List<String> pathSuffix = resourcePath.subList(1, resourcePath.size());
      SimpleCatalog nestedCatalog = getOrCreateNestedCatalog(rootCatalog, nestedCatalogName);
      return getSubCatalogForResource(nestedCatalog, pathSuffix);
    } else {
      return rootCatalog;
    }
  }

  /**
   * Generic function for creating a resource in a {@link SimpleCatalog}
   *
   * @param nameInCatalog The name the resource will have in the catalog
   * @param createMode The {@link CreateMode} to use
   * @param resourceType The resource type name (e.g. "table", "function")
   * @param alreadyExists Whether the resource already exists
   * @param creator A {@link Runnable} that will create the resource in the catalog if run
   * @param deleter A Runnable that will delete the resource from the catalog if run
   */
  private static void createResource(
      String nameInCatalog,
      CreateMode createMode,
      String resourceType,
      boolean alreadyExists,
      Runnable creator,
      Runnable deleter) {
    if (createMode.equals(CreateMode.CREATE_IF_NOT_EXISTS) && alreadyExists) {
      return;
    }

    if (createMode.equals(CreateMode.CREATE_OR_REPLACE) && alreadyExists) {
      deleter.run();
    }

    if (createMode.equals(CreateMode.CREATE_DEFAULT) && alreadyExists) {
      String errorMessage =
          String.format("%s %s already exists in catalog", resourceType, nameInCatalog);
      throw new CatalogResourceAlreadyExists(nameInCatalog, errorMessage);
    }

    creator.run();
  }

  /**
   * Deletes a table with the provided name from {@link SimpleCatalog}
   *
   * @param catalog The catalog from which to delete tables
   * @param name The name for the table in the catalog
   * @throws CatalogResourceDoesNotExist if the table does not exist in the catalog
   */
  public static void deleteTableFromCatalog(SimpleCatalog catalog, String name) {
    if (!tableExists(catalog, name)) {
      String errorMessage = String.format("Tried to delete table which does not exist: %s", name);
      throw new CatalogResourceDoesNotExist(name, errorMessage);
    }

    catalog.removeSimpleTable(name);
  }

  /**
   * Creates a table in a {@link SimpleCatalog} using the provided paths and complying with the
   * provided CreateMode.
   *
   * @param catalog The catalog in which to create the table
   * @param nameInCatalog The name under which the table will be registered in the catalog
   * @param table The {@link SimpleTable} object representing the table
   * @param createMode The CreateMode to use
   * @throws CatalogResourceAlreadyExists if the table already exists at any of the provided paths
   *     and CreateMode != CREATE_OR_REPLACE.
   */
  public static void createTableInCatalog(
      SimpleCatalog catalog, String nameInCatalog, SimpleTable table, CreateMode createMode) {

    boolean alreadyExists = tableExists(catalog, nameInCatalog);

    createResource(
        nameInCatalog,
        createMode,
        "Table",
        alreadyExists,
        /*creator=*/ () -> catalog.addSimpleTable(nameInCatalog, table),
        /*deleter=*/ () -> deleteTableFromCatalog(catalog, nameInCatalog));
  }

  /**
   * Deletes a function with the provided name from the {@link SimpleCatalog}
   *
   * @param catalog The catalog from which to delete the function
   * @param fullName The full name of the function in the catalog
   * @throws CatalogResourceDoesNotExist if the function does not exist in the catalog
   */
  public static void deleteFunctionFromCatalog(SimpleCatalog catalog, String fullName) {
    String fullNameWithoutGroup = removeGroupFromFunctionName(fullName);

    Optional<String> fullNameToDelete =
        catalog.getFunctionNameList().stream()
            .filter(
                name -> removeGroupFromFunctionName(name).equalsIgnoreCase(fullNameWithoutGroup))
            .findFirst();

    if (fullNameToDelete.isPresent()) {
      catalog.removeFunction(fullNameToDelete.get());
    } else {
      String errorMessage =
          String.format("Tried to delete function which does not exist: %s", fullName);
      throw new CatalogResourceDoesNotExist(fullName, errorMessage);
    }
  }

  /**
   * Creates a function in a {@link SimpleCatalog} using the provided paths and complying with the
   * provided CreateMode.
   *
   * @param catalog The catalog in which to create the function
   * @param nameInCatalog The name under which the function will be registered in the catalog
   * @param functionInfo The {@link FunctionInfo} object representing the function that should be
   *     created
   * @param createMode The CreateMode to use
   * @throws CatalogResourceAlreadyExists if the function already exists at any of the provided
   *     paths and CreateMode != CREATE_OR_REPLACE.
   */
  public static void createFunctionInCatalog(
      SimpleCatalog catalog,
      String nameInCatalog,
      FunctionInfo functionInfo,
      CreateMode createMode) {

    boolean alreadyExists = functionExists(catalog, nameInCatalog);

    Function function =
        new Function(
            ImmutableList.of(nameInCatalog),
            functionInfo.getGroup(),
            functionInfo.getMode(),
            functionInfo.getSignatures());

    createResource(
        nameInCatalog,
        createMode,
        "Function",
        alreadyExists,
        /*creator=*/ () -> catalog.addFunction(function),
        /*deleter=*/ () -> deleteFunctionFromCatalog(catalog, nameInCatalog));
  }

  /**
   * Deletes a TVF with the provided name from the {@link SimpleCatalog}
   *
   * @param catalog The catalog from which to delete the function
   * @param fullName The full name of the function in the catalog
   * @throws CatalogResourceDoesNotExist if the function does not exist in the catalog
   */
  public static void deleteTVFFromCatalog(SimpleCatalog catalog, String fullName) {
    if (!tvfExists(catalog, fullName)) {
      String errorMessage = String.format("Tried to delete TVF which does not exist: %s", fullName);
      throw new CatalogResourceDoesNotExist(fullName, errorMessage);
    }

    catalog.removeTableValuedFunction(fullName);
  }

  /**
   * Creates a TVF in a {@link SimpleCatalog} using the provided name and complying with the
   * provided CreateMode.
   *
   * @param catalog The catalog in which to create the TVF
   * @param nameInCatalog The name under which the TVF will be registered in the catalog
   * @param tvfInfo The {@link TVFInfo} object representing the TVF that should be created
   * @param createMode The CreateMode to use
   * @throws CatalogResourceAlreadyExists if the TVF already exists at any of the provided paths and
   *     CreateMode != CREATE_OR_REPLACE.
   */
  public static void createTVFInCatalog(
      SimpleCatalog catalog, String nameInCatalog, TVFInfo tvfInfo, CreateMode createMode) {
    Preconditions.checkArgument(
        tvfInfo.getOutputSchema().isPresent(), "Cannot create a a TVF without an output schema");

    boolean alreadyExists = tvfExists(catalog, nameInCatalog);

    TableValuedFunction tvf =
        new FixedOutputSchemaTVF(
            ImmutableList.of(nameInCatalog),
            tvfInfo.getSignature(),
            tvfInfo.getOutputSchema().get());

    createResource(
        nameInCatalog,
        createMode,
        "TVF",
        alreadyExists,
        /*creator=*/ () -> catalog.addTableValuedFunction(tvf),
        /*deleter=*/ () -> deleteTVFFromCatalog(catalog, nameInCatalog));
  }

  private static void deleteProcedureFromCatalogImpl(SimpleCatalog catalog, String fullName) {
    if (!procedureExists(catalog, fullName)) {
      String errorMessage =
          String.format("Tried to delete procedure which does not exist: %s", fullName);
      throw new CatalogResourceDoesNotExist(fullName, errorMessage);
    }

    catalog.removeProcedure(fullName);
  }

  /**
   * Deletes a procedure with the provided name from the {@link SimpleCatalog}
   *
   * <p>Qualified procedures need to be registered two times in the catalog for analysis to work as
   * expected. This method takes care of deleting both copies of the procedure if necessary.
   *
   * @param catalog The catalog from which to delete the procedure
   * @param fullName The full name of the procedure in the catalog
   * @throws CatalogResourceDoesNotExist if the procedure does not exist in the catalog
   */
  public static void deleteProcedureFromCatalog(SimpleCatalog catalog, String fullName) {
    deleteProcedureFromCatalogImpl(catalog, fullName);

    if (fullName.contains(".")) {
      List<String> nameComponents = Arrays.asList(fullName.split("\\."));
      String nestedName = nameComponents.get(nameComponents.size() - 1);
      SimpleCatalog nestedCatalog = getSubCatalogForResource(catalog, nameComponents);
      deleteProcedureFromCatalogImpl(nestedCatalog, nestedName);
    }
  }

  /**
   * Creates a procedure in a {@link SimpleCatalog} using the provided name and complying with the
   * provided CreateMode.
   *
   * <p>Qualified procedures will be registered two times in the catalog for analysis to work as
   * expected. A procedure with name "project.dataset.table" will be registered at name paths:
   * ["project.dataset.table"] and ["project", "dataset", "table"].
   *
   * @param catalog The SimpleCatalog in which to create the procedure
   * @param nameInCatalog The name under which the procedure will be registered in the catalog
   * @param procedureInfo The ProcedureInfo object representing the procedure that should be created
   * @param createMode The CreateMode to use
   * @throws CatalogResourceAlreadyExists if the procedure already exists at any of the provided
   *     paths and CreateMode != CREATE_OR_REPLACE.
   */
  public static void createProcedureInCatalog(
      SimpleCatalog catalog,
      String nameInCatalog,
      ProcedureInfo procedureInfo,
      CreateMode createMode) {

    boolean alreadyExists = procedureExists(catalog, nameInCatalog);

    Runnable creatorFunction =
        () -> {
          Procedure procedure =
              new Procedure(ImmutableList.of(nameInCatalog), procedureInfo.getSignature());
          catalog.addProcedure(procedure);

          if (nameInCatalog.contains(".")) {
            List<String> nameComponents = Arrays.asList(nameInCatalog.split("\\."));
            String nestedName = nameComponents.get(nameComponents.size() - 1);
            SimpleCatalog nestedCatalog = getSubCatalogForResource(catalog, nameComponents);
            Procedure nestedProcedure =
                new Procedure(ImmutableList.of(nestedName), procedureInfo.getSignature());
            nestedCatalog.addProcedure(nestedProcedure);
          }
        };

    createResource(
        nameInCatalog,
        createMode,
        "Procedure",
        alreadyExists,
        /*creator=*/ creatorFunction,
        /*deleter=*/ () -> deleteProcedureFromCatalog(catalog, nameInCatalog));
  }

  /**
   * Creates a copy of a SimpleCatalog.
   *
   * @param sourceCatalog The SimpleCatalog that should be copied.
   * @return The copy of the provided SimpleCatalog.
   */
  public static SimpleCatalog copyCatalog(SimpleCatalog sourceCatalog) {
    return SimpleCatalogUtil.copyCatalog(sourceCatalog);
  }
}
