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

package com.google.zetasql.toolkit;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.ImmutableList;
import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.SimpleColumn;
import com.google.zetasql.SimpleTable;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.parser.ASTNodes.ASTSimpleType;
import com.google.zetasql.parser.ASTNodes.ASTVariableDeclaration;
import com.google.zetasql.resolvedast.ResolvedNodes.*;
import com.google.zetasql.toolkit.catalog.basic.BasicCatalogWrapper;
import java.util.Iterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ZetaSQLToolkitTest {

  private ZetaSQLToolkitAnalyzer analyzer;

  @BeforeEach
  void init() {
    AnalyzerOptions analyzerOptions = new AnalyzerOptions();
    analyzerOptions.getLanguageOptions().enableMaximumLanguageFeatures();
    analyzerOptions.getLanguageOptions().setSupportsAllStatementKinds();

    this.analyzer = new ZetaSQLToolkitAnalyzer(analyzerOptions);
  }

  @Test
  void testSimpleSelectStmt() {
    String stmt = "SELECT 1 AS col";

    AnalyzedStatement analyzedStmt = this.analyzer.analyzeStatements(stmt).next();

    assertTrue(analyzedStmt.getResolvedStatement().isPresent());
    ResolvedQueryStmt queryStmt =
        assertInstanceOf(ResolvedQueryStmt.class, analyzedStmt.getResolvedStatement().get());

    ResolvedProjectScan projectScan =
        assertInstanceOf(ResolvedProjectScan.class, queryStmt.getQuery());
    assertInstanceOf(ResolvedSingleRowScan.class, projectScan.getInputScan());
    assertEquals(1, projectScan.getColumnList().size());
    assertAll(
        () -> assertEquals("col", projectScan.getColumnList().get(0).getName()),
        () -> assertTrue(projectScan.getColumnList().get(0).getType().isInteger()));
    assertEquals(1, projectScan.getExprList().size());

    ResolvedLiteral literal =
        assertInstanceOf(ResolvedLiteral.class, projectScan.getExprList().get(0).getExpr());
    assertEquals(1, literal.getValue().getInt64Value());
  }

  @Test
  void testSelectStmtWithUnnest() {
    SimpleCatalog catalog = new SimpleCatalog("catalog");
    catalog.addSimpleTable(
        "t",
        new SimpleTable(
            "t",
            ImmutableList.of(
                new SimpleColumn("t", "column1", TypeFactory.createSimpleType(TypeKind.TYPE_INT64)),
                new SimpleColumn(
                    "t",
                    "column2",
                    TypeFactory.createArrayType(
                        TypeFactory.createSimpleType(TypeKind.TYPE_STRING))))));

    String stmt = "SELECT column1, unnested FROM t CROSS JOIN UNNEST(t.column2) AS unnested;";

    AnalyzedStatement analyzedStmt =
        this.analyzer.analyzeStatements(stmt, new BasicCatalogWrapper(catalog)).next();

    assertTrue(analyzedStmt.getResolvedStatement().isPresent());
    ResolvedQueryStmt queryStmt =
        assertInstanceOf(ResolvedQueryStmt.class, analyzedStmt.getResolvedStatement().get());

    ResolvedProjectScan projectScan =
        assertInstanceOf(ResolvedProjectScan.class, queryStmt.getQuery());
    assertEquals(2, projectScan.getColumnList().size());
    assertAll(
        () -> assertEquals("column1", projectScan.getColumnList().get(0).getName()),
        () -> assertTrue(projectScan.getColumnList().get(0).getType().isInteger()),
        () -> assertEquals("unnested", projectScan.getColumnList().get(1).getName()),
        () -> assertTrue(projectScan.getColumnList().get(1).getType().isString()));

    ResolvedArrayScan arrayScan =
        assertInstanceOf(ResolvedArrayScan.class, projectScan.getInputScan());
    ResolvedColumnRef unnestedColumn =
        assertInstanceOf(ResolvedColumnRef.class, arrayScan.getArrayExpr());
    assertEquals("column2", unnestedColumn.getColumn().getName());

    ResolvedTableScan tableScan =
        assertInstanceOf(ResolvedTableScan.class, arrayScan.getInputScan());
    assertEquals("t", tableScan.getTable().getName());
  }

  @Test
  void testTableDDL() {
    String query = "CREATE TEMP TABLE t AS (SELECT 1 AS column);\n" + "SELECT * FROM t;";
    SimpleCatalog catalog = new SimpleCatalog("catalog");

    Iterator<AnalyzedStatement> statementIterator =
        this.analyzer.analyzeStatements(query, new BasicCatalogWrapper(catalog), true);

    AnalyzedStatement first = statementIterator.next();
    assertTrue(first.getResolvedStatement().isPresent());
    assertInstanceOf(ResolvedCreateTableAsSelectStmt.class, first.getResolvedStatement().get());
    assertNotNull(catalog.getTable("t", null));
    SimpleTable table = catalog.getTable("t", null);
    assertAll(
        () -> assertEquals("t", table.getName()),
        () -> assertEquals(1, table.getColumnList().size()),
        () -> assertEquals("column", table.getColumnList().get(0).getName()));

    // Just check that the second statement was analyzed successfully
    AnalyzedStatement second = statementIterator.next();
    assertTrue(second.getResolvedStatement().isPresent());
    assertInstanceOf(ResolvedQueryStmt.class, second.getResolvedStatement().get());
  }

  @Test
  void testVariableDeclaration() {
    String query = "DECLARE x INT64 DEFAULT 1;\n" + "SELECT x;";
    SimpleCatalog catalog = new SimpleCatalog("catalog");

    Iterator<AnalyzedStatement> statementIterator =
        this.analyzer.analyzeStatements(query, new BasicCatalogWrapper(catalog), true);

    AnalyzedStatement first = statementIterator.next();
    ASTVariableDeclaration variableDeclaration =
        assertInstanceOf(ASTVariableDeclaration.class, first.getParsedStatement());
    ASTSimpleType type = assertInstanceOf(ASTSimpleType.class, variableDeclaration.getType());
    assertEquals("INT64", type.getTypeName().getNames().get(0).getIdString());
    assertEquals(1, catalog.getConstantList().size());
    assertEquals("x", catalog.getConstantList().get(0).getFullName());
    assertTrue(catalog.getConstantList().get(0).getType().isInt64());

    AnalyzedStatement second = statementIterator.next();
    assertTrue(second.getResolvedStatement().isPresent());
    ResolvedQueryStmt queryStmt =
        assertInstanceOf(ResolvedQueryStmt.class, second.getResolvedStatement().get());
    ResolvedProjectScan projectScan =
        assertInstanceOf(ResolvedProjectScan.class, queryStmt.getQuery());
    assertEquals(1, projectScan.getColumnList().size());
    ResolvedConstant resolvedConstant =
        assertInstanceOf(ResolvedConstant.class, projectScan.getExprList().get(0).getExpr());
    assertEquals("x", resolvedConstant.getConstant().getFullName());
    assertTrue(resolvedConstant.getConstant().getType().isInt64());
  }

  @Test
  void testTypeCoercion() {
    // Invalid type for the default value
    String query = "DECLARE x INT64 DEFAULT 'ASD';";
    Iterator<AnalyzedStatement> statementIterator = this.analyzer.analyzeStatements(query);
    assertThrows(AnalysisException.class, statementIterator::next);

    // Invalid type for SET statement
    query = "DECLARE x INT64;\n" + "SET x = 'ASD';";
    statementIterator = this.analyzer.analyzeStatements(query);
    statementIterator.next();
    assertThrows(AnalysisException.class, statementIterator::next);
  }
}
