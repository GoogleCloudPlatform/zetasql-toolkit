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

import com.google.protobuf.ExperimentalApi;
import com.google.zetasql.Table;
import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedColumnRef;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedCreateTableAsSelectStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedInsertRow;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedInsertStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedMergeStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedMergeWhen;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedOutputColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedUpdateItem;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedUpdateStmt;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ColumnLineageExtractor {

  private static Set<ColumnLineage> extractColumnLevelLineage(
      ResolvedCreateTableAsSelectStmt createTableAsSelectStmt) {

    String tablePath = String.join(".", createTableAsSelectStmt.getNamePath());

    List<ResolvedOutputColumn> outputColumns = createTableAsSelectStmt.getOutputColumnList();

    Map<String, List<ResolvedColumn>> outputColumnToParentColumns = outputColumns.stream()
        .collect(Collectors.toMap(
            ResolvedOutputColumn::getName,
            outputColumn ->
                ParentColumnFinder.find(createTableAsSelectStmt, outputColumn.getColumn())
        ));

    return outputColumnToParentColumns.entrySet()
        .stream()
        .map(entry -> {
          ColumnEntity target = new ColumnEntity(tablePath, entry.getKey());
          Set<ColumnEntity> parents = entry.getValue()
              .stream()
              .map(ColumnEntity::forResolvedColumn)
              .collect(Collectors.toSet());
          return new ColumnLineage(target, parents);
        })
        .collect(Collectors.toSet());
  }

  private static Set<ColumnLineage> extractColumnLevelLineage(ResolvedInsertStmt insertStmt) {
    if (Objects.isNull(insertStmt.getQuery())) {
      // The statement is inserting rows manually using "INSERT INTO ... VALUES ..."
      // Since it does not query any tables, it does not produce lineage
      return Set.of();
    }

    Table targetTable = insertStmt.getTableScan().getTable();
    List<ResolvedColumn> insertedColumns = insertStmt.getInsertColumnList();
    ResolvedScan query = insertStmt.getQuery();

    Set<ColumnLineage> result = new HashSet<>(insertedColumns.size());

    for (int i = 0; i < insertedColumns.size(); i++) {
      ResolvedColumn insertedColumn = insertedColumns.get(i);
      ResolvedColumn matchingColumnInQuery = query.getColumnList().get(i);
      List<ResolvedColumn> parents = ParentColumnFinder.find(insertStmt, matchingColumnInQuery);

      ColumnEntity target = new ColumnEntity(targetTable.getFullName(), insertedColumn.getName());
      Set<ColumnEntity> parentEntities = parents
          .stream()
          .map(ColumnEntity::forResolvedColumn)
          .collect(Collectors.toSet());
      result.add(new ColumnLineage(target, parentEntities));
    }

    return result;
  }

  private static Optional<ColumnLineage> extractColumnLevelLineage(
      Table targetTable,
      ResolvedStatement originalStatement,
      ResolvedUpdateItem updateItem) {

    ResolvedExpr target = updateItem.getTarget();
    ResolvedExpr updateExpression = updateItem.getSetValue().getValue();

    if (!(target instanceof ResolvedColumnRef)) {
      // TODO: Do we need to handle other types of inserts?
      //  See: https://github.com/google/zetasql/blob/5133c6e373a3f67f7f40b0619a2913c3fcab8171/zetasql/resolved_ast/gen_resolved_ast.py#L5564
      return Optional.empty();
    }

    ResolvedColumnRef targetColumnRef = (ResolvedColumnRef) target;
    List<ResolvedColumn> parents = ParentColumnFinder.find(originalStatement, updateExpression);

    ColumnEntity targetColumnEntity =
        new ColumnEntity(targetTable.getFullName(), targetColumnRef.getColumn().getName());
    Set<ColumnEntity> parentEntities = parents.stream()
        .map(ColumnEntity::forResolvedColumn)
        .collect(Collectors.toSet());

    ColumnLineage result = new ColumnLineage(targetColumnEntity, parentEntities);

    return Optional.of(result);
  }

  private static Set<ColumnLineage> extractColumnLevelLineage(ResolvedUpdateStmt updateStmt) {
    Table targetTable = updateStmt.getTableScan().getTable();
    List<ResolvedUpdateItem> updateItems = updateStmt.getUpdateItemList();

    return updateItems.stream()
        .map(updateItem -> extractColumnLevelLineage(targetTable, updateStmt, updateItem))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toSet());
  }

  private static Set<ColumnLineage> extractColumnLevelLineage(
      Table targetTable,
      ResolvedStatement originalStatement,
      ResolvedMergeWhen mergeWhen) {

    List<ResolvedColumn> insertedColumns = mergeWhen.getInsertColumnList();
    ResolvedInsertRow insertRow = mergeWhen.getInsertRow();
    List<ResolvedUpdateItem> updateItems = mergeWhen.getUpdateItemList();

    if (Objects.nonNull(insertRow)) {
      // WHEN ... THEN INSERT
      Set<ColumnLineage> result = new HashSet<>(insertedColumns.size());

      for (int i = 0; i < insertedColumns.size(); i++) {
        ResolvedColumn insertedColumn = insertedColumns.get(i);
        ResolvedExpr rowColumn = insertRow.getValueList().get(i).getValue();
        List<ResolvedColumn> parents = ParentColumnFinder.find(originalStatement, rowColumn);

        ColumnEntity target = new ColumnEntity(targetTable.getFullName(), insertedColumn.getName());
        Set<ColumnEntity> parentEntities = parents
            .stream()
            .map(ColumnEntity::forResolvedColumn)
            .collect(Collectors.toSet());
        result.add(new ColumnLineage(target, parentEntities));
      }

      return result;
    } else if (Objects.nonNull(updateItems)) {
      // WHEN ... THEN UPDATE
      return updateItems.stream()
          .map(updateItem -> extractColumnLevelLineage(targetTable, originalStatement, updateItem))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toSet());
    }

    return Set.of();
  }

  private static Set<ColumnLineage> extractColumnLevelLineage(ResolvedMergeStmt mergeStmt) {
    Table targetTable = mergeStmt.getTableScan().getTable();

    return mergeStmt.getWhenClauseList()
        .stream()
        .map(mergeWhen -> extractColumnLevelLineage(targetTable, mergeStmt, mergeWhen))
        .flatMap(Set::stream)
        .collect(Collectors.toSet());
  }

  @ExperimentalApi
  public static Set<ColumnLineage> extractColumnLevelLineage(ResolvedStatement statement) {
    if (statement instanceof ResolvedCreateTableAsSelectStmt) {
      return extractColumnLevelLineage((ResolvedCreateTableAsSelectStmt) statement);
    } else if (statement instanceof ResolvedInsertStmt) {
      return extractColumnLevelLineage((ResolvedInsertStmt) statement);
    } else if (statement instanceof ResolvedUpdateStmt) {
      return extractColumnLevelLineage((ResolvedUpdateStmt) statement);
    } else if (statement instanceof ResolvedMergeStmt) {
      return extractColumnLevelLineage((ResolvedMergeStmt) statement);
    }

    return Set.of();
  }

}
