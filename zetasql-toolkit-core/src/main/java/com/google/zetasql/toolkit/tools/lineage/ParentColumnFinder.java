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

package com.google.zetasql.toolkit.tools.lineage;

import com.google.common.collect.ImmutableList;
import com.google.zetasql.StructType;
import com.google.zetasql.Type;
import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedArrayScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedComputedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedMakeStruct;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedSetOperationItem;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedSetOperationScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedTVFScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedTableScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWithEntry;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWithRefScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWithScan;
import com.google.zetasql.resolvedast.ResolvedNodes.Visitor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * Implements finding the parent columns for other columns or for expressions within a resolved
 * statement. Only "terminal" columns are considered to be parents, meaning columns that read
 * directly from a table. Intermediate computed columns in the statement are not considered.
 *
 * <p>Column A being a parent of column B means the content of A is (either directly or indirectly)
 * used to write to B. For example: "SELECT A AS B", "UPDATE ... SET B = UPPER(A)", "INSERT INTO
 * t(B) SELECT something FROM (SELECT A AS something)", etc.
 *
 * <p>The parent columns of an expression E are the terminal parents of all the columns E uses as
 * output. For example, the parents of expression E = "UPPER(A, B)" are A and B. If A or B happen to
 * not be terminal columns, the parents of E are the terminal parents of A and B themselves. Another
 * example; the parents for "IF(condition, trueCase, falseCase)" are all the parents from the
 * trueCase and the falseCase, but not the parents from the condition since the condition is not
 * used as output.
 *
 * <p>Finding parent columns is implemented by traversing the statement the column or expression
 * belongs to. When a {@link ResolvedComputedColumn} node is found; it is registered in the
 * columnsToParents Map, together with its direct parents. After traversal, the columnsToParents map
 * is used to find all terminal parents for the column or expression in question.
 *
 * <p>There's a special case for WITH statements, since each reference to a WITH entry creates a new
 * unique set of {@link ResolvedColumn}s instead of referencing the ones created in the WITH
 * subquery. While traversing the statement, this visitor maintains a stack of the {@link
 * ResolvedWithEntry}s in scope. When visiting a {@link ResolvedWithRefScan} it uses those scopes to
 * correlate the ResolvedWithRefScan columns to their parents in the corresponding
 * ResolvedWithEntry.
 */
public class ParentColumnFinder extends Visitor {

  private final HashMap<String, List<ResolvedColumn>> columnsToParents = new HashMap<>();
  private final Set<String> terminalColumns = new HashSet<>();
  private final Stack<List<ResolvedWithEntry>> withEntryScopes = new Stack<>();
  private final Stack<ResolvedColumn> columnsBeingComputed = new Stack<>();

  private ParentColumnFinder() {}

  /**
   * Finds the terminal parents for a particular {@link ResolvedColumn}.
   *
   * @param statement The {@link ResolvedStatement} the column belongs to.
   * @param column The ResolvedColumn to find parents for.
   * @return A list of ResolvedColumns containing the terminal parents on the provided column.
   */
  public static List<ResolvedColumn> findParentsForColumn(
      ResolvedStatement statement, ResolvedColumn column) {
    return new ParentColumnFinder().findImpl(statement, column);
  }

  /**
   * Finds the terminal parents for a particular {@link ResolvedExpr}.
   *
   * @param statement The {@link ResolvedStatement} the expression belongs to.
   * @param expression The ResolvedExpr to find parents for.
   * @return A list of ResolvedColumns containing the terminal parents on the provided expression.
   */
  public static List<ResolvedColumn> findParentsForExpression(
      ResolvedStatement statement, ResolvedExpr expression) {
    List<ResolvedColumn> parentsReferenced =
        ExpressionParentFinder.findDirectParentsForExpression(expression);

    return parentsReferenced.stream()
        .map(parent -> ParentColumnFinder.findParentsForColumn(statement, parent))
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  public List<ResolvedColumn> findImpl(ResolvedNode containerNode, ResolvedColumn column) {
    // 1. Traverse the containerNode.
    // This will populate this.columnsToParents with the ResolvedColumns in the containerNode
    // and
    // the direct parents for each of them.
    // columnsToParents can be thought of as a tree where the root node is the original
    // ResolvedColumn and the leaves are its terminal parents.
    containerNode.accept(this);

    // 2. Use this.columnsToParents to find the terminal parents for the desired column.
    // Traverses the tree-like structured mentioned above using breadth-first search.
    ArrayList<ResolvedColumn> result = new ArrayList<>();
    Queue<ResolvedColumn> resolutionQueue = new ArrayDeque<>(ImmutableList.of(column));

    while (resolutionQueue.peek() != null) {
      ResolvedColumn currentColumn = resolutionQueue.remove();
      String currentColumnKey = makeColumnKey(currentColumn);
      List<ResolvedColumn> parents = getParentsOfColumn(currentColumn);

      if (parents.isEmpty() && terminalColumns.contains(currentColumnKey)) {
        result.add(currentColumn);
      } else {
        resolutionQueue.addAll(parents);
      }
    }

    return result;
  }

  private String makeColumnKey(ResolvedColumn column) {
    return String.format("%s.%s#%d", column.getTableName(), column.getName(), column.getId());
  }

  private List<ResolvedColumn> getParentsOfColumn(ResolvedColumn column) {
    String key = makeColumnKey(column);
    return columnsToParents.computeIfAbsent(key, k -> new ArrayList<>());
  }

  private void addParentsToColumn(ResolvedColumn column, List<ResolvedColumn> newParents) {
    List<ResolvedColumn> parents = getParentsOfColumn(column);
    parents.addAll(newParents);
  }

  private void addParentToColumn(ResolvedColumn column, ResolvedColumn newParent) {
    addParentsToColumn(column, ImmutableList.of(newParent));
  }

  private ResolvedColumn buildColumnSubfield(
      ResolvedColumn column, String fieldName, Type fieldType) {
    return new ResolvedColumn(
        column.getId(), column.getTableName(), column.getName() + "." + fieldName, fieldType);
  }

  private List<ResolvedColumn> expandColumn(ResolvedColumn column) {
    ArrayList<ResolvedColumn> result = new ArrayList<>();
    result.add(column);

    Type type = column.getType();

    if (type.isStruct()) {
      type.asStruct().getFieldList().stream()
          .map(field -> buildColumnSubfield(column, field.getName(), field.getType()))
          .flatMap(subColumn -> expandColumn(subColumn).stream())
          .forEachOrdered(result::add);
    }

    return result;
  }

  public void visit(ResolvedComputedColumn computedColumn) {
    // When visiting a resolved column, register it in the columnsToParents Map together with
    // its direct parents.

    ResolvedColumn column = computedColumn.getColumn();
    ResolvedExpr expression = computedColumn.getExpr();

    columnsBeingComputed.push(column);

    if (expression instanceof ResolvedMakeStruct) {
      expandMakeStruct(column, (ResolvedMakeStruct) expression);
    } else {
      List<ResolvedColumn> expressionParents =
          ExpressionParentFinder.findDirectParentsForExpression(expression);
      columnsBeingComputed.forEach(
          columnBeingComputed -> addParentsToColumn(columnBeingComputed, expressionParents));
    }

    columnsBeingComputed.pop();
  }

  public void expandMakeStruct(ResolvedColumn targetColumn, ResolvedMakeStruct makeStruct) {
    StructType structType = makeStruct.getType().asStruct();
    int numberOfFields = structType.getFieldCount();

    for (int i = 0; i < numberOfFields; i++) {
      String fieldName = structType.getField(i).getName();
      ResolvedExpr fieldExpression = makeStruct.getFieldList().get(i);
      ResolvedColumn fieldColumn =
          buildColumnSubfield(targetColumn, fieldName, fieldExpression.getType());

      columnsBeingComputed.push(fieldColumn);
      List<ResolvedColumn> expressionParents =
          ExpressionParentFinder.findDirectParentsForExpression(fieldExpression);
      columnsBeingComputed.forEach(
          columnBeingComputed -> addParentsToColumn(columnBeingComputed, expressionParents));
      columnsBeingComputed.pop();

      if (fieldExpression instanceof ResolvedMakeStruct) {
        expandMakeStruct(fieldColumn, (ResolvedMakeStruct) fieldExpression);
      }
    }
  }

  private void registerTerminalColumns(List<ResolvedColumn> newTerminalColumns) {
    newTerminalColumns.stream()
        .map(this::expandColumn)
        .flatMap(List::stream)
        .map(this::makeColumnKey)
        .forEach(terminalColumns::add);
  }

  public void visit(ResolvedTableScan tableScan) {
    registerTerminalColumns(tableScan.getColumnList());
  }

  public void visit(ResolvedTVFScan tvfScan) {
    registerTerminalColumns(tvfScan.getColumnList());
  }

  public void visit(ResolvedWithScan withScan) {
    // When visiting a WITH scan, push the WITH entries to the scope stack
    // and traverse the scan body
    withEntryScopes.push(withScan.getWithEntryList());
    withScan.getWithEntryList().forEach(withEntry -> withEntry.accept(this));
    withScan.getQuery().accept(this);

    // The WITH entries go out of scope when we exit the WITH scan
    withEntryScopes.pop();
  }

  private Optional<ResolvedWithEntry> findInScopeWithEntryByName(String name) {
    Optional<ResolvedWithEntry> maybeWithEntry = Optional.empty();

    // Traverse the scopes stack top-to-bottom and use the first matching WITH in scope
    for (int i = withEntryScopes.size() - 1; i >= 0; i--) {
      List<ResolvedWithEntry> inScopeWithEntries = withEntryScopes.get(i);

      maybeWithEntry =
          inScopeWithEntries.stream()
              .filter(withEntry -> withEntry.getWithQueryName().equalsIgnoreCase(name))
              .findFirst();

      if (maybeWithEntry.isPresent()) {
        break;
      }
    }

    return maybeWithEntry;
  }

  public void visit(ResolvedWithRefScan withRefScan) {
    // WithRefScans create new ResolvedColumns for each column in the WITH entry instead
    // of referencing the WITH entry directly.
    // Here we find the corresponding with entry which is in scope and register the original
    // WITH entry columns a parents of their corresponding columns in the WithRefScan.
    Optional<ResolvedWithEntry> maybeWithEntry =
        findInScopeWithEntryByName(withRefScan.getWithQueryName());

    if (!maybeWithEntry.isPresent()) {
      // Should never happen, since the query would be invalid.
      return;
    }

    ResolvedWithEntry withEntry = maybeWithEntry.get();

    // Columns in the WITH entry and the WithRefScan map 1:1.
    // Register each column in the WITH entry as a parent of its corresponding column in this
    // WithRefScan
    // If a column is a STRUCT, also register the 1:1 parent relationship between fields
    for (int i = 0; i < withRefScan.getColumnList().size(); i++) {
      ResolvedColumn withRefScanColumn = withRefScan.getColumnList().get(i);
      ResolvedColumn matchingWithEntryColumn = withEntry.getWithSubquery().getColumnList().get(i);

      List<ResolvedColumn> expandedRefScanColumn = expandColumn(withRefScanColumn);
      List<ResolvedColumn> expandedMatchingWithEntryColumn = expandColumn(matchingWithEntryColumn);

      for (int j = 0; j < expandedRefScanColumn.size(); j++) {
        addParentToColumn(expandedRefScanColumn.get(j), expandedMatchingWithEntryColumn.get(j));
      }
    }
  }

  public void visit(ResolvedArrayScan arrayScan) {
    ResolvedColumn elementColumn = arrayScan.getElementColumn();
    columnsBeingComputed.push(elementColumn);
    arrayScan.getArrayExpr().accept(this);
    columnsBeingComputed.pop();

    if (arrayScan.getInputScan() != null) {
      arrayScan.getInputScan().accept(this);
    }
  }

  public void visit(ResolvedSetOperationScan setOperationScan) {
    List<ResolvedColumn> generatedColumns = setOperationScan.getColumnList();
    List<ResolvedSetOperationItem> setOperationItems = setOperationScan.getInputItemList();

    for (int i = 0; i < generatedColumns.size(); i++) {
      int columnIndex = i;
      ResolvedColumn generatedColumn = generatedColumns.get(columnIndex);
      List<ResolvedColumn> parentColumns =
          setOperationItems.stream()
              .map(ResolvedSetOperationItem::getOutputColumnList)
              .map(outputColumnList -> outputColumnList.get(columnIndex))
              .collect(Collectors.toList());
      addParentsToColumn(generatedColumn, parentColumns);
    }

    setOperationItems.stream()
        .map(ResolvedSetOperationItem::getScan)
        .forEach(innerScan -> innerScan.accept(this));
  }
}
