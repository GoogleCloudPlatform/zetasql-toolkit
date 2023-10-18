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

import com.google.common.collect.ImmutableList;
import com.google.zetasql.Constant;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.SimpleModel;
import com.google.zetasql.SimpleTable;
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.CreateMode;
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.CreateScope;
import java.util.List;

/**
 * Interface for an object that wraps a ZetaSQL SimpleCatalog and allows adding resources to it by
 * value (providing the actual resource object) or by name. Should be implemented when creating a
 * Catalog implementation that follows the semantics of a specific SQL engine, for example,
 * BigQuery.
 */
public interface CatalogWrapper {

  /**
   * Registers a SimpleTable in this catalog.
   *
   * @param table The SimpleTable to register
   * @param createMode The CreateMode for creating the table
   * @param createScope The CreateScope for creating the table
   */
  void register(SimpleTable table, CreateMode createMode, CreateScope createScope);

  /**
   * Registers a Function in this catalog.
   *
   * @param function The Function to register in this catalog
   * @param createMode The CreateMode for creating the function
   * @param createScope The CreateScope for creating the function
   */
  void register(FunctionInfo function, CreateMode createMode, CreateScope createScope);

  /**
   * Registers a TVF in this catalog.
   *
   * @param tvfInfo The TVFInfo object representing the TVF to register
   * @param createMode The CreateMode for creating the TVF
   * @param createScope The CreateScope for creating the TVF
   */
  void register(TVFInfo tvfInfo, CreateMode createMode, CreateScope createScope);

  /**
   * Registers a procedure in this catalog.
   *
   * @param procedureInfo The ProcedureInfo object representing the procedure to register
   * @param createMode The CreateMode for creating the procedure
   * @param createScope The CreateScope for creating the procedure
   */
  void register(ProcedureInfo procedureInfo, CreateMode createMode, CreateScope createScope);

  /**
   * Registers a model in this catalog.
   *
   * @param model The {@link SimpleModel} object representing the model to register
   * @param createMode The CreateMode for creating the model
   * @param createScope The CreateScope for creating the model
   */
  void register(SimpleModel model, CreateMode createMode, CreateScope createScope);

  /**
   * Registers a constant in this catalog.
   *
   * @param constant The {@link Constant} object representing the constant to register
   */
  void register(Constant constant);

  /**
   * Removes a table to this catalog by name.
   *
   * @param table The reference to the table to remove
   */
  void removeTable(String table);

  /**
   * Removes a function to this catalog by name.
   *
   * @param function The reference to the function to remove
   */
  void removeFunction(String function);

  /**
   * Removes a TVF to this catalog by name.
   *
   * @param function The reference to the TVF to remove
   */
  void removeTVF(String function);

  /**
   * Removes a procedure to this catalog by name.
   *
   * @param procedure The reference to the procedure to remove
   */
  void removeProcedure(String procedure);

  /**
   * Removes a model to this catalog by name.
   *
   * @param model The reference to the model to remove
   */
  void removeModel(String model);

  /**
   * Adds a set of tables to this catalog by name.
   *
   * @param tables The list of table references to add
   */
  void addTables(List<String> tables);

  /**
   * Adds a set of functions to this catalog by name.
   *
   * @param functions The list of function references to add
   */
  void addFunctions(List<String> functions);

  /**
   * Adds a set of TVFs to this catalog by name.
   *
   * @param functions The list of function references to add
   */
  void addTVFs(List<String> functions);

  /**
   * Adds a set of procedures to this catalog by name.
   *
   * @param procedures The list of procedure references to add
   */
  void addProcedures(List<String> procedures);

  /**
   * Adds a set of models to this catalog by name.
   *
   * @param models The list of model references to add
   */
  void addModels(List<String> models);

  /**
   * Adds a table to this catalog by name.
   *
   * @param table The reference to the table to add
   */
  default void addTable(String table) {
    this.addTables(ImmutableList.of(table));
  }

  /**
   * Adds a function to this catalog by name.
   *
   * @param function The reference to the function to add
   */
  default void addFunction(String function) {
    this.addFunctions(ImmutableList.of(function));
  }

  /**
   * Adds a TVF to this catalog by name.
   *
   * @param function The reference to the TVF to add
   */
  default void addTVF(String function) {
    this.addTVFs(ImmutableList.of(function));
  }

  /**
   * Adds a procedure to this catalog by name.
   *
   * @param procedure The reference to the procedure to add
   */
  default void addProcedure(String procedure) {
    this.addProcedures(ImmutableList.of(procedure));
  }

  /**
   * Creates a copy of this CatalogWrapper.
   *
   * <p>Each implementation is responsible for determining how itself should be copied.
   *
   * @return The copy for this CatalogWrapper
   */
  CatalogWrapper copy();

  /**
   * Gets the underlying ZetaSQL SimpleCatalog.
   *
   * @return The underlying ZetaSQL SimpleCatalog that can be used for analyzing queries
   */
  SimpleCatalog getZetaSQLCatalog();
}
