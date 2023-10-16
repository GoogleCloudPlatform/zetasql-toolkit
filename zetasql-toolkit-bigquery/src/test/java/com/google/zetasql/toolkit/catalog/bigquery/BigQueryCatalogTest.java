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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.zetasql.Function;
import com.google.zetasql.FunctionArgumentType;
import com.google.zetasql.FunctionSignature;
import com.google.zetasql.NotFoundException;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.SimpleColumn;
import com.google.zetasql.SimpleTable;
import com.google.zetasql.TVFRelation;
import com.google.zetasql.Table;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.ZetaSQLFunctions.FunctionEnums.Mode;
import com.google.zetasql.ZetaSQLFunctions.SignatureArgumentKind;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.CreateMode;
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.CreateScope;
import com.google.zetasql.toolkit.catalog.CatalogTestUtils;
import com.google.zetasql.toolkit.catalog.FunctionInfo;
import com.google.zetasql.toolkit.catalog.ProcedureInfo;
import com.google.zetasql.toolkit.catalog.TVFInfo;
import com.google.zetasql.toolkit.catalog.bigquery.exceptions.InvalidBigQueryReference;
import com.google.zetasql.toolkit.catalog.exceptions.CatalogResourceAlreadyExists;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BigQueryCatalogTest {

  final String testProjectId = "test-project-id";
  BigQueryCatalog bigQueryCatalog;
  @Mock BigQueryResourceProvider bigQueryResourceProviderMock;

  FunctionInfo exampleFunction =
      FunctionInfo.newBuilder()
          .setNamePath(ImmutableList.of(testProjectId, "dataset", "examplefunction"))
          .setGroup("UDF")
          .setMode(Mode.SCALAR)
          .setSignatures(
              ImmutableList.of(
                  new FunctionSignature(
                      new FunctionArgumentType(TypeFactory.createSimpleType(TypeKind.TYPE_STRING)),
                      ImmutableList.of(),
                      -1)))
          .build();

  TVFInfo exampleTVF =
      TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of(testProjectId, "dataset", "exampletvf"))
          .setSignature(
              new FunctionSignature(
                  new FunctionArgumentType(SignatureArgumentKind.ARG_TYPE_RELATION), ImmutableList.of(), -1))
          .setOutputSchema(
              TVFRelation.createValueTableBased(TypeFactory.createSimpleType(TypeKind.TYPE_STRING)))
          .build();

  ProcedureInfo exampleProcedure =
      new ProcedureInfo(
          ImmutableList.of(testProjectId, "dataset", "exampleprocedure"),
          new FunctionSignature(
              new FunctionArgumentType(TypeFactory.createSimpleType(TypeKind.TYPE_STRING)),
              ImmutableList.of(),
              -1));

  @BeforeEach
  void init() {
    this.bigQueryCatalog =
        new BigQueryCatalog(this.testProjectId, this.bigQueryResourceProviderMock);
  }

  private List<List<String>> buildPathsWhereResourceShouldBe(String resourceReference) {
    BigQueryReference ref = BigQueryReference.from(this.testProjectId, resourceReference);

    List<List<String>> fullyQualifiedPaths =
        ImmutableList.of(
            ImmutableList.of(ref.getProjectId() + "." + ref.getDatasetId() + "." + ref.getResourceName()),
            ImmutableList.of(ref.getProjectId(), ref.getDatasetId() + "." + ref.getResourceName()),
            ImmutableList.of(ref.getProjectId() + "." + ref.getDatasetId(), ref.getResourceName()),
            ImmutableList.of(ref.getProjectId(), ref.getDatasetId(), ref.getResourceName()));

    List<List<String>> result = new ArrayList<>(fullyQualifiedPaths);

    if (ref.getProjectId().equals(this.testProjectId)) {
      List<List<String>> implicitProjectPaths =
          ImmutableList.of(
              ImmutableList.of(ref.getDatasetId() + "." + ref.getResourceName()),
              ImmutableList.of(ref.getDatasetId(), ref.getResourceName()));
      result.addAll(implicitProjectPaths);
    }

    return result;
  }

  private SimpleTable buildExampleTable(String tableRef) {
    SimpleTable table = new SimpleTable(
        tableRef,
        ImmutableList.of(
            new SimpleColumn(
                tableRef,
                "col1",
                TypeFactory.createSimpleType(TypeKind.TYPE_STRING))));
    table.setFullName(tableRef);
    return table;
  }

  private Table assertTableExists(BigQueryCatalog catalog, String tableRef) {
    Preconditions.checkNotNull(catalog, "Catalog cannot be null");
    Preconditions.checkNotNull(tableRef, "Table reference cannot be null");

    SimpleCatalog underlyingCatalog = catalog.getZetaSQLCatalog();
    List<String> tablePath = ImmutableList.of(tableRef);

    assertDoesNotThrow(
        () -> underlyingCatalog.findTable(tablePath),
        String.format(
            "Expected table %s to exist", tableRef));

    try {
      return underlyingCatalog.findTable(tablePath);
    } catch (NotFoundException e) {
      throw new AssertionError(e);
    }
  }

  private void assertTableDoesNotExist(BigQueryCatalog catalog, String tableRef) {
    Preconditions.checkNotNull(catalog, "Catalog cannot be null");
    Preconditions.checkNotNull(tableRef, "Table reference cannot be null");

    SimpleCatalog underlyingCatalog = catalog.getZetaSQLCatalog();
    List<String> tablePath = ImmutableList.of(tableRef);

    assertThrows(
        NotFoundException.class,
        () -> underlyingCatalog.findTable(tablePath),
        String.format("Expected table %s to not exist", tableRef));
  }

  @Test
  void testCatalogSupportsBigQueryTypeNames() {
    SimpleCatalog underlyingCatalog = this.bigQueryCatalog.getZetaSQLCatalog();

    Type integerType =
        assertDoesNotThrow(
            () -> underlyingCatalog.findType(ImmutableList.of("INTEGER")),
            "BigQuery catalogs should support the INTEGER type name");
    Type decimalType =
        assertDoesNotThrow(
            () -> underlyingCatalog.findType(ImmutableList.of("DECIMAL")),
            "BigQuery catalogs should support the DECIMAL type name");

    assertEquals(
        TypeKind.TYPE_INT64,
        integerType.getKind(),
        "BigQuery catalog's INTEGER type should be an alias for INT64");

    assertEquals(
        TypeKind.TYPE_NUMERIC,
        decimalType.getKind(),
        "BigQuery catalog's DECIMAL type should be an alias for NUMERIC");
  }

  @Test
  void testRegisterInvalidTableName() {
    String invalidFullName = "An.Invalid.BQ.Reference";
    SimpleTable table = buildExampleTable(invalidFullName);

    when(this.bigQueryResourceProviderMock.getTables(anyString(), anyList()))
        .thenReturn(ImmutableList.of(table));

    assertThrows(
        InvalidBigQueryReference.class,
        () ->
            this.bigQueryCatalog.register(
                table, CreateMode.CREATE_DEFAULT, CreateScope.CREATE_DEFAULT_SCOPE),
        "Expected BigQueryCatalog to fail when registering a table with an invalid name");

    assertThrows(
        InvalidBigQueryReference.class,
        () -> this.bigQueryCatalog.addTable(invalidFullName),
        "Expected BigQueryCatalog to fail when adding a table with an invalid name");
  }

  @Test
  void testRegisterTableFromDefaultProject() {
    SimpleTable table  =
        new SimpleTable(
            "example",
            ImmutableList.of(
                new SimpleColumn(
                    "example",
                    "col1",
                    TypeFactory.createSimpleType(TypeKind.TYPE_STRING))));
    table.setFullName(String.format("%s.dataset.example", this.testProjectId));

    this.bigQueryCatalog.register(
        table, CreateMode.CREATE_DEFAULT, CreateScope.CREATE_DEFAULT_SCOPE);

    Table foundTable = assertTableExists(bigQueryCatalog, table.getFullName());
    assertTableExists(bigQueryCatalog, "dataset.example");

    assertTrue(
        CatalogTestUtils.tableColumnsEqual(table, foundTable),
        "Expected table created in Catalog to be equal to the original");
  }

  @Test
  void testRegisterTableFromNonDefaultProject() {
    SimpleTable table  =
        new SimpleTable(
            "example",
            ImmutableList.of(
                new SimpleColumn(
                    "example",
                    "col1",
                    TypeFactory.createSimpleType(TypeKind.TYPE_STRING))));
    table.setFullName("another-project-id.dataset.example");

    this.bigQueryCatalog.register(
        table, CreateMode.CREATE_DEFAULT, CreateScope.CREATE_DEFAULT_SCOPE);

    assertTableExists(bigQueryCatalog, table.getFullName());
    assertTableDoesNotExist(bigQueryCatalog, "dataset.example");
  }

  @Test
  void testRemoveTable() {
    String tableRef = String.format("%s.dataset.example", testProjectId);
    SimpleTable table = buildExampleTable(tableRef);

    bigQueryCatalog.register(
        table, CreateMode.CREATE_DEFAULT, CreateScope.CREATE_DEFAULT_SCOPE);

    assertTableExists(bigQueryCatalog, tableRef);
    assertTableExists(bigQueryCatalog, "dataset.example");

    this.bigQueryCatalog.removeTable(tableRef);

    assertTableDoesNotExist(bigQueryCatalog, tableRef);
    assertTableDoesNotExist(bigQueryCatalog, "dataset.example");
  }

  @Test
  void testReplaceTable() {
    String tableRef = String.format("%s.dataset.example", testProjectId);
    SimpleTable table = buildExampleTable(tableRef);

    SimpleTable replacement = new SimpleTable(tableRef, ImmutableList.of(
        new SimpleColumn(tableRef, "newCol",
            TypeFactory.createSimpleType(TypeKind.TYPE_STRING))));

    bigQueryCatalog.register(
        table, CreateMode.CREATE_DEFAULT, CreateScope.CREATE_DEFAULT_SCOPE);

    // Replace the table and validate it has been replaced
    bigQueryCatalog.register(
        replacement,
        CreateMode.CREATE_OR_REPLACE,
        CreateScope.CREATE_DEFAULT_SCOPE);

    Table foundTable = assertTableExists(this.bigQueryCatalog, tableRef);

    assertTrue(
        CatalogTestUtils.tableColumnsEqual(replacement, foundTable),
        "Expected table to have been replaced");
  }

  @Test
  void testTableAlreadyExists() {
    String tableRef = String.format("%s.dataset.example", testProjectId);
    SimpleTable table = buildExampleTable(tableRef);

    this.bigQueryCatalog.register(
        table, CreateMode.CREATE_DEFAULT, CreateScope.CREATE_DEFAULT_SCOPE);

    // Try to replace the table without using CreateMode.CREATE_OR_REPLACE
    assertThrows(
        CatalogResourceAlreadyExists.class,
        () ->
            this.bigQueryCatalog.register(
                table,
                CreateMode.CREATE_DEFAULT,
                CreateScope.CREATE_DEFAULT_SCOPE),
        "Expected fail creating table that already exists");
  }

  @Test
  void testAddTablesByName() {
    String tableRef = String.format("%s.dataset.example", testProjectId);
    SimpleTable table = buildExampleTable(tableRef);

    // When BigQueryResourceProvider.getTables() is called, return the test table
    when(bigQueryResourceProviderMock.getTables(anyString(), anyList()))
        .thenReturn(ImmutableList.of(table));

    // Add the tables by name
    bigQueryCatalog.addTables(ImmutableList.of(table.getFullName()));

    // Verify the BigQueryCatalog got the tables from the BigQueryResourceProvider
    verify(bigQueryResourceProviderMock, times(1))
        .getTables(anyString(), anyList());

    // Verify the test table was added to the catalog
    assertTableExists(bigQueryCatalog, tableRef);
  }

  @Test
  void testAddAllTablesInDataset() {
    String tableRef = String.format("%s.dataset.example", testProjectId);
    SimpleTable table = buildExampleTable(tableRef);

    when(bigQueryResourceProviderMock.getAllTablesInDataset(anyString(), anyString()))
        .thenReturn(ImmutableList.of(table));

    bigQueryCatalog.addAllTablesInDataset(testProjectId, "dataset");

    verify(bigQueryResourceProviderMock, times(1))
        .getAllTablesInDataset(anyString(), anyString());

    assertTableExists(bigQueryCatalog, tableRef);
  }

  @Test
  void testAddAllTablesInProject() {
    String tableRef = String.format("%s.dataset.example", testProjectId);
    SimpleTable table = buildExampleTable(tableRef);

    when(bigQueryResourceProviderMock.getAllTablesInProject(anyString()))
        .thenReturn(ImmutableList.of(table));

    bigQueryCatalog.addAllTablesInProject(testProjectId);

    verify(bigQueryResourceProviderMock, times(1))
        .getAllTablesInProject(anyString());

    assertTableExists(bigQueryCatalog, tableRef);
  }

  @Test
  void testRegisterFunction() {
    this.bigQueryCatalog.register(
        exampleFunction, CreateMode.CREATE_DEFAULT, CreateScope.CREATE_DEFAULT_SCOPE);

    SimpleCatalog underlyingCatalog = this.bigQueryCatalog.getZetaSQLCatalog();
    assertDoesNotThrow(
        () -> underlyingCatalog.findFunction(exampleFunction.getNamePath()),
        String.format(
            "Expected function to exist at path %s",
            String.join(".", exampleFunction.getNamePath())));
  }

  @Test
  void testInferFunctionReturnType() {
    FunctionInfo functionWithUnknownReturnType =
        FunctionInfo.newBuilder()
            .setNamePath(ImmutableList.of(testProjectId, "dataset", "function"))
            .setGroup("UDF")
            .setMode(Mode.SCALAR)
            .setSignatures(
                ImmutableList.of(
                    new FunctionSignature(
                        new FunctionArgumentType(
                            TypeFactory.createSimpleType(TypeKind.TYPE_UNKNOWN)),
                        ImmutableList.of(),
                        -1)))
            .setLanguage(FunctionInfo.Language.SQL)
            .setBody("5.6 + 5")
            .build();

    this.bigQueryCatalog.register(
        functionWithUnknownReturnType, CreateMode.CREATE_DEFAULT, CreateScope.CREATE_DEFAULT_SCOPE);

    SimpleCatalog underlyingCatalog = this.bigQueryCatalog.getZetaSQLCatalog();
    Function foundFunction =
        assertDoesNotThrow(
            () -> underlyingCatalog.findFunction(functionWithUnknownReturnType.getNamePath()),
            String.format(
                "Expected function to exist at path %s",
                String.join(".", functionWithUnknownReturnType.getNamePath())));

    assertTrue(foundFunction.getSignatureList().get(0).getResultType().getType().isFloatingPoint());
  }

  @Test
  void testRegisterProcedure() {
    this.bigQueryCatalog.register(
        exampleProcedure, CreateMode.CREATE_DEFAULT, CreateScope.CREATE_DEFAULT_SCOPE);

    SimpleCatalog underlyingCatalog = this.bigQueryCatalog.getZetaSQLCatalog();
    assertDoesNotThrow(
        () -> underlyingCatalog.findProcedure(exampleProcedure.getNamePath()),
        String.format(
            "Expected procedure to exist at path %s",
            String.join(".", exampleProcedure.getNamePath())));
  }

  @Test
  void testCopy() {
    BigQueryCatalog copy = this.bigQueryCatalog.copy();

    assertAll(
        () -> assertNotSame(this.bigQueryCatalog, copy),
        () -> assertNotSame(this.bigQueryCatalog.getZetaSQLCatalog(), copy.getZetaSQLCatalog()));
  }
}
