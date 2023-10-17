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

package com.google.zetasql.toolkit.catalog.basic;

import com.google.common.collect.ImmutableList;
import com.google.zetasql.Constant;
import com.google.zetasql.LanguageOptions;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.SimpleTable;
import com.google.zetasql.ZetaSQLBuiltinFunctionOptions;
import com.google.zetasql.ZetaSQLOptions.LanguageFeature;
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.CreateMode;
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.CreateScope;
import com.google.zetasql.toolkit.catalog.CatalogOperations;
import com.google.zetasql.toolkit.catalog.CatalogWrapper;
import com.google.zetasql.toolkit.catalog.FunctionInfo;
import com.google.zetasql.toolkit.catalog.ProcedureInfo;
import com.google.zetasql.toolkit.catalog.TVFInfo;
import com.google.zetasql.toolkit.catalog.exceptions.CatalogException;
import com.google.zetasql.toolkit.catalog.exceptions.CatalogResourceAlreadyExists;
import java.util.Arrays;
import java.util.List;

/**
 * Basic implementation of CatalogWrapper which does not implement the semantics of any particular
 * SQL engine. It does not support adding resources by name.
 */
public class BasicCatalogWrapper implements CatalogWrapper {

  private final SimpleCatalog catalog;

  // TODO: Go back to including all language features when possible.
  //  See CatalogOperations.copyCatalog().
  private static final LanguageOptions languageOptionsForFunctionsAndTypes = new LanguageOptions();

  private static final List<LanguageFeature> excludedLanguageFeatures = ImmutableList.of(
      LanguageFeature.FEATURE_ROUND_WITH_ROUNDING_MODE,
      LanguageFeature.FEATURE_V_1_4_ARRAY_ZIP,
      LanguageFeature.FEATURE_V_1_4_ARRAY_FIND_FUNCTIONS,
      LanguageFeature.FEATURE_RANGE_TYPE,
      LanguageFeature.FEATURE_DIFFERENTIAL_PRIVACY_REPORT_FUNCTIONS
  );

  static {
    Arrays.stream(LanguageFeature.values())
        .filter(feature -> !excludedLanguageFeatures.contains(feature))
        .forEach(languageOptionsForFunctionsAndTypes::enableLanguageFeature);
  }

  public BasicCatalogWrapper() {
    this.catalog = new SimpleCatalog("catalog");
    this.catalog.addZetaSQLFunctionsAndTypes(
        new ZetaSQLBuiltinFunctionOptions(languageOptionsForFunctionsAndTypes));
  }

  public BasicCatalogWrapper(SimpleCatalog initialCatalog) {
    this.catalog = initialCatalog;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Registers the table using its full name
   */
  @Override
  public void register(SimpleTable table, CreateMode createMode, CreateScope createScope) {
    CatalogOperations.createTableInCatalog(this.catalog, table.getFullName(), table, createMode);
  }

  @Override
  public void register(FunctionInfo function, CreateMode createMode, CreateScope createScope) {
    CatalogOperations.createFunctionInCatalog(
        this.catalog, function.getFullName(), function, createMode);
  }

  @Override
  public void register(TVFInfo tvfInfo, CreateMode createMode, CreateScope createScope) {
    CatalogOperations.createTVFInCatalog(
        this.catalog, tvfInfo.getFullName(), tvfInfo, createMode);
  }

  /**
   * {@inheritDoc}
   *
   * @throws CatalogException if the name path for the procedure has more than one item
   */
  @Override
  public void register(
      ProcedureInfo procedureInfo, CreateMode createMode, CreateScope createScope) {
    CatalogOperations.createProcedureInCatalog(
        this.catalog, procedureInfo.getFullName(), procedureInfo, createMode);
  }

  /**
   * {@inheritDoc}
   *
   * @throws CatalogResourceAlreadyExists if the constant already exits in this catalog
   */
  @Override
  public void register(Constant constant) {
    String fullName = constant.getFullName();

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
  public void removeTable(String fullTableName) {
    CatalogOperations.deleteTableFromCatalog(this.catalog, fullTableName);
  }

  @Override
  public void removeFunction(String function) {
    CatalogOperations.deleteFunctionFromCatalog(this.catalog, function);
  }

  @Override
  public void removeTVF(String function) {
    CatalogOperations.deleteTVFFromCatalog(this.catalog, function);
  }

  @Override
  public void removeProcedure(String procedure) {
    CatalogOperations.deleteProcedureFromCatalog(this.catalog, procedure);
  }

  @Override
  public void addTables(List<String> tables) {
    throw new UnsupportedOperationException("The BasicCatalogWrapper cannot add tables by name");
  }

  @Override
  public void addFunctions(List<String> functions) {
    throw new UnsupportedOperationException("The BasicCatalogWrapper cannot add functions by name");
  }

  @Override
  public void addTVFs(List<String> functions) {
    throw new UnsupportedOperationException("The BasicCatalogWrapper cannot add TVFs by name");
  }

  @Override
  public void addProcedures(List<String> procedures) {
    throw new UnsupportedOperationException(
        "The BasicCatalogWrapper cannot add procedures by name");
  }

  @Override
  public SimpleCatalog getZetaSQLCatalog() {
    return this.catalog;
  }

  @Override
  public BasicCatalogWrapper copy() {
    return new BasicCatalogWrapper(CatalogOperations.copyCatalog(this.getZetaSQLCatalog()));
  }
}
