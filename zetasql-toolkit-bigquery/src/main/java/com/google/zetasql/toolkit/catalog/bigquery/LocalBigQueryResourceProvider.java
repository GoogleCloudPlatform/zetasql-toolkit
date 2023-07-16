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

import com.google.zetasql.SimpleTable;
import com.google.zetasql.toolkit.catalog.FunctionInfo;
import com.google.zetasql.toolkit.catalog.ProcedureInfo;
import com.google.zetasql.toolkit.catalog.TVFInfo;
import com.google.zetasql.toolkit.catalog.io.CatalogResources;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link BigQueryResourceProvider} implementation that fetches catalog resources from a local
 * {@link CatalogResources} object.
 */
public class LocalBigQueryResourceProvider implements BigQueryResourceProvider {

  private final CatalogResources catalogResources;

  public LocalBigQueryResourceProvider(CatalogResources catalogResources) {
    this.catalogResources = catalogResources;
  }

  private Set<BigQueryReference> parseBigQueryReferences(
      String projectId, List<String> references) {
    return references.stream()
        .map(reference -> BigQueryReference.from(projectId, reference))
        .collect(Collectors.toSet());
  }

  @Override
  public List<SimpleTable> getTables(String projectId, List<String> tableReferences) {
    Set<BigQueryReference> references = parseBigQueryReferences(projectId, tableReferences);

    return catalogResources.getTables()
        .stream()
        .filter(table -> {
          BigQueryReference tableReference = BigQueryReference.from(projectId, table.getName());
          return references.contains(tableReference);
        })
        .collect(Collectors.toList());
  }

  @Override
  public List<SimpleTable> getAllTablesInDataset(String projectId, String datasetName) {
    return catalogResources.getTables()
        .stream()
        .filter(table -> {
          BigQueryReference tableReference = BigQueryReference.from(projectId, table.getName());
          return tableReference.getProjectId().equalsIgnoreCase(projectId)
              && tableReference.getDatasetId().equals(datasetName);
        })
        .collect(Collectors.toList());
  }

  @Override
  public List<SimpleTable> getAllTablesInProject(String projectId) {
    return catalogResources.getTables()
        .stream()
        .filter(table -> {
          BigQueryReference tableReference = BigQueryReference.from(projectId, table.getName());
          return tableReference.getProjectId().equalsIgnoreCase(projectId);
        })
        .collect(Collectors.toList());
  }

  @Override
  public List<FunctionInfo> getFunctions(String projectId, List<String> functionReferences) {
    Set<BigQueryReference> references = parseBigQueryReferences(projectId, functionReferences);

    return catalogResources.getFunctions()
        .stream()
        .filter(function -> {
          BigQueryReference functionReference =
              BigQueryReference.from(projectId, function.getFullName());
          return references.contains(functionReference);
        })
        .collect(Collectors.toList());
  }

  @Override
  public List<FunctionInfo> getAllFunctionsInDataset(String projectId, String datasetName) {
    return catalogResources.getFunctions()
        .stream()
        .filter(function -> {
          BigQueryReference functionReference =
              BigQueryReference.from(projectId, function.getFullName());
          return functionReference.getProjectId().equalsIgnoreCase(projectId)
              && functionReference.getDatasetId().equals(datasetName);
        })
        .collect(Collectors.toList());
  }

  @Override
  public List<FunctionInfo> getAllFunctionsInProject(String projectId) {
    return catalogResources.getFunctions()
        .stream()
        .filter(function -> {
          BigQueryReference functionReference =
              BigQueryReference.from(projectId, function.getFullName());
          return functionReference.getProjectId().equalsIgnoreCase(projectId);
        })
        .collect(Collectors.toList());
  }

  @Override
  public List<TVFInfo> getTVFs(String projectId, List<String> functionReferences) {
    Set<BigQueryReference> references = parseBigQueryReferences(projectId, functionReferences);

    return catalogResources.getTVFs()
        .stream()
        .filter(tvf -> {
          BigQueryReference functionReference =
              BigQueryReference.from(projectId, tvf.getFullName());
          return references.contains(functionReference);
        })
        .collect(Collectors.toList());
  }

  @Override
  public List<TVFInfo> getAllTVFsInDataset(String projectId, String datasetName) {
    return catalogResources.getTVFs()
        .stream()
        .filter(tvf -> {
          BigQueryReference functionReference =
              BigQueryReference.from(projectId, tvf.getFullName());
          return functionReference.getProjectId().equalsIgnoreCase(projectId)
              && functionReference.getDatasetId().equals(datasetName);
        })
        .collect(Collectors.toList());
  }

  @Override
  public List<TVFInfo> getAllTVFsInProject(String projectId) {
    return catalogResources.getTVFs()
        .stream()
        .filter(tvf -> {
          BigQueryReference functionReference =
              BigQueryReference.from(projectId, tvf.getFullName());
          return functionReference.getProjectId().equalsIgnoreCase(projectId);
        })
        .collect(Collectors.toList());
  }

  @Override
  public List<ProcedureInfo> getProcedures(String projectId, List<String> procedureReferences) {
    Set<BigQueryReference> references = parseBigQueryReferences(projectId, procedureReferences);

    return catalogResources.getProcedures()
        .stream()
        .filter(procedure -> {
          BigQueryReference procedureReference =
              BigQueryReference.from(projectId, procedure.getFullName());
          return references.contains(procedureReference);
        })
        .collect(Collectors.toList());
  }

  @Override
  public List<ProcedureInfo> getAllProceduresInDataset(String projectId, String datasetName) {
    return catalogResources.getProcedures()
        .stream()
        .filter(procedure -> {
          BigQueryReference procedureReference =
              BigQueryReference.from(projectId, procedure.getFullName());
          return procedureReference.getProjectId().equalsIgnoreCase(projectId)
              && procedureReference.getDatasetId().equals(datasetName);
        })
        .collect(Collectors.toList());
  }

  @Override
  public List<ProcedureInfo> getAllProceduresInProject(String projectId) {
    return catalogResources.getProcedures()
        .stream()
        .filter(procedure -> {
          BigQueryReference procedureReference =
              BigQueryReference.from(projectId, procedure.getFullName());
          return procedureReference.getProjectId().equalsIgnoreCase(projectId);
        })
        .collect(Collectors.toList());
  }

}
