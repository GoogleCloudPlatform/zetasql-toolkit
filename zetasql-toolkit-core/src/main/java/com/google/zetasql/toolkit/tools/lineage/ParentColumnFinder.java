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

import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAggregateScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAnalyticScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedComputedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedProjectScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWithEntry;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWithRefScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWithScan;
import com.google.zetasql.resolvedast.ResolvedNodes.Visitor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;

class ParentColumnFinder extends Visitor {

  private final HashMap<String, List<ResolvedColumn>> columnsToParents = new HashMap<>();
  private final List<ResolvedWithEntry> inScopeWithEntries = new ArrayList<>();

  private ParentColumnFinder() {}

  public static List<ResolvedColumn> find(ResolvedStatement statement, ResolvedColumn column) {
    return new ParentColumnFinder().findImpl(statement, column);
  }

  public static List<ResolvedColumn> find(ResolvedStatement statement, ResolvedExpr expression) {
    List<ResolvedColumn> parentsReferenced =
        ColumnReferenceExtractor.extractFromExpression(expression);

    return parentsReferenced.stream()
        .map(parent -> ParentColumnFinder.find(statement, parent))
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  public List<ResolvedColumn> findImpl(ResolvedNode containingNode, ResolvedColumn column) {
    containingNode.accept(this);

    ArrayList<ResolvedColumn> result = new ArrayList<>();
    Queue<ResolvedColumn> resolutionQueue = new ArrayDeque<>(List.of(column));

    while (resolutionQueue.peek() != null) {
      ResolvedColumn current = resolutionQueue.remove();
      List<ResolvedColumn> parents = columnsToParents.get(current.shortDebugString());

      if (parents == null) {
        // If it does not have any parents, it is terminal
        result.add(current);
      } else {
        resolutionQueue.addAll(parents);
      }
    }

    return result;
  }

  public void visit(ResolvedProjectScan projectScan) {
    projectScan.getExprList().forEach(computedColumn -> computedColumn.accept(this));
    projectScan.getInputScan().accept(this);
  }

  public void visit(ResolvedAggregateScan aggregateScan) {
    aggregateScan.getGroupByList().forEach(computedColumn -> computedColumn.accept(this));
    aggregateScan.getAggregateList().forEach(computedColumn -> computedColumn.accept(this));
    aggregateScan.getInputScan().accept(this);
  }

  public void visit(ResolvedAnalyticScan analyticScan) {
    analyticScan.getFunctionGroupList()
        .forEach(analyticFunctionGroup -> analyticFunctionGroup.accept(this));
    analyticScan.getInputScan().accept(this);
  }

  public void visit(ResolvedWithScan withScan) {
    this.inScopeWithEntries.addAll(withScan.getWithEntryList());
    withScan.getWithEntryList().forEach(withEntry -> withEntry.accept(this));
    withScan.getQuery().accept(this);
    this.inScopeWithEntries.removeAll(withScan.getWithEntryList());
  }

  public void visit(ResolvedWithRefScan withRefScan) {
    Optional<ResolvedWithEntry> maybeWithEntry = inScopeWithEntries.stream()
        .filter(withEntry -> withEntry.getWithQueryName().equals(withRefScan.getWithQueryName()))
        .findFirst();

    if (maybeWithEntry.isEmpty()) {
      return;
    }

    ResolvedWithEntry withEntry = maybeWithEntry.get();

    for (int i = 0; i < withRefScan.getColumnList().size(); i++) {
      ResolvedColumn withRefScanColumn = withRefScan.getColumnList().get(i);
      ResolvedColumn matchingWithEntryColumn = withEntry.getWithSubquery().getColumnList().get(i);
      List<ResolvedColumn> parentsToWithRefScanColumn = columnsToParents.computeIfAbsent(
          withRefScanColumn.shortDebugString(), key -> new ArrayList<>());
      parentsToWithRefScanColumn.add(matchingWithEntryColumn);
    }

  }

  public void visit(ResolvedComputedColumn computedColumn) {
    ResolvedColumn column = computedColumn.getColumn();
    List<ResolvedColumn> knownColumnParents = columnsToParents.computeIfAbsent(
        column.shortDebugString(), key -> new ArrayList<>());
    List<ResolvedColumn> expressionParents =
        ColumnReferenceExtractor.extractFromExpression(computedColumn.getExpr());
    knownColumnParents.addAll(expressionParents);
  }

}
