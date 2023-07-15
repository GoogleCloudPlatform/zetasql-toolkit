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

package com.google.zetasql.toolkit.catalog.io;

import com.google.gson.JsonParseException;
import com.google.zetasql.SimpleTable;
import com.google.zetasql.toolkit.catalog.FunctionInfo;
import com.google.zetasql.toolkit.catalog.ProcedureInfo;
import com.google.zetasql.toolkit.catalog.TVFInfo;
import java.util.List;

/**
 * Dataclass containing the resources of a Catalog
 */
public class CatalogResources {

  private final List<SimpleTable> tables;
  private final List<FunctionInfo> functions;
  private final List<TVFInfo> tvfs;
  private final List<ProcedureInfo> procedures;

  public CatalogResources(
      List<SimpleTable> tables,
      List<FunctionInfo> functions,
      List<TVFInfo> tvfs,
      List<ProcedureInfo> procedures) {
    this.tables = tables;
    this.functions = functions;
    this.tvfs = tvfs;
    this.procedures = procedures;
  }

  public List<SimpleTable> getTables() {
    return tables != null ? tables : List.of();
  }

  public List<FunctionInfo> getFunctions() {
    return functions != null ? functions : List.of();
  }

  public List<TVFInfo> getTVFs() {
    return tvfs != null ? tvfs : List.of();
  }

  public List<ProcedureInfo> getProcedures() {
    return procedures != null ? procedures : List.of();
  }

  /**
   * Deserializes a JSON object representing a Catalog into a {@link CatalogResources} instance. 
   * JSON objects representing catalogs support tables, scalar functions, TVFs and procedures.
   *
   * <pre>
   * JSON objects should follow the following format:
   *
   * {
   *   "tables": [
   *     {
   *       "name": "project.dataset.table",
   *       "columns": [
   *         { "name": "column", "type": "INT64" },
   *         { "name": "column2", "type": "TIMESTAMP", "isPseudoColumn": true }
   *       ]
   *     }
   *   ],
   *   "functions": [
   *     {
   *       "name": "project.dataset.function",
   *       "signatures": [
   *         {
   *           "arguments": [
   *             {  "name": "arg1", "type": "ARRAY&lt;STRING&gt;" }
   *           ],
   *           "returnType": "STRING"
   *         }
   *       ]
   *     }
   *   ],
   *   "tvfs": [
   *     {
   *       "name": "project.dataset.tvf",
   *       "arguments": [
   *         { "name": "arg1", "type": "ARRAY&lt;STRUCT&lt;x INT64&gt;&gt;" }
   *       ],
   *       "outputColumns": [
   *         { "name": "out1", "type": "INT64" }
   *       ]
   *     }
   *   ],
   *   "procedures": [
   *     {
   *       "name": "project.dataset.proc",
   *       "arguments": [
   *         { "name": "arg1", "type": "ARRAY&lt;STRUCT&lt;x INT64&gt;&gt;" }
   *       ]
   *     }
   *   ]
   * }
   * </pre>
   * 
   * @param json The JSON string for the object to deserialize into {@link CatalogResources}
   * @return The resulting {@link CatalogResources} instance
   * @throws JsonParseException when parsing the JSON catalog fails
   */
  public static CatalogResources fromJson(String json) {
    return JsonCatalogDeserializer.readJsonCatalog(json);
  }

}
