package com.google.zetasql.toolkit.tools;

import com.google.protobuf.ExperimentalApi;
import com.google.zetasql.Table;
import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedColumnRef;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedComputedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedCreateTableAsSelectStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedInsertRow;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedInsertStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedMergeStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedMergeWhen;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedOutputColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedSubqueryExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedUpdateItem;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedUpdateStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.Visitor;
import com.google.zetasql.resolvedast.ResolvedSubqueryExprEnums.SubqueryType;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ColumnLevelLineage {

  public static class ColumnEntity {
    public final String table;
    public final String name;

    public ColumnEntity(String table, String name) {
      this.table = table;
      this.name = name;
    }

    public static ColumnEntity forResolvedColumn(ResolvedColumn resolvedColumn) {
      return new ColumnEntity(resolvedColumn.getTableName(), resolvedColumn.getName());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof ColumnEntity)) {
        return false;
      }

      ColumnEntity other = (ColumnEntity) o;
      return table.equals(other.table)
          && name.equalsIgnoreCase(other.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(table, name.toLowerCase());
    }
  }

  public static class ColumnLineage {
    public final ColumnEntity target;
    public final Set<ColumnEntity> parents;

    public ColumnLineage(ColumnEntity target, Set<ColumnEntity> parents) {
      this.target = target;
      this.parents = parents;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof ColumnLineage)) {
        return false;
      }

      ColumnLineage other = (ColumnLineage) o;
      return target.equals(other.target)
          && parents.equals(other.parents);
    }

    @Override
    public int hashCode() {
      return Objects.hash(target, parents);
    }
  }

  private static <T> List<T> findDescendantSubtreesWithKind(ResolvedNode node, Class<T> cls) {
    ArrayList<T> result = new ArrayList<>();

    node.accept(new Visitor() {
      @Override
      protected void defaultVisit(ResolvedNode node) {
        if (cls.isAssignableFrom(node.getClass())) {
          result.add(cls.cast(node));
        }
        super.defaultVisit(node);
      }
    });

    return result;
  }

  private static Map<String, ResolvedComputedColumn> getComputedColumnsMap(ResolvedNode node) {
    List<ResolvedComputedColumn> computedColumnList =
        findDescendantSubtreesWithKind(node, ResolvedComputedColumn.class);

    return computedColumnList
        .stream()
        .collect(Collectors.toMap(
            computedColumn -> computedColumn.getColumn().shortDebugString(),
            Function.identity()
        ));
  }

  private static List<ResolvedColumnRef> extractParentsFromExpression(ResolvedExpr expression) {
    // TODO: Handle different types of expressions. E.g. special cases for functions,
    //  subqueries, etc. Probably use a visitor.

    if (
        expression instanceof ResolvedSubqueryExpr
        && ((ResolvedSubqueryExpr) expression).getSubqueryType().equals(SubqueryType.SCALAR)
    ) {
      // TODO: Implement
    }

    return findDescendantSubtreesWithKind(expression, ResolvedColumnRef.class);
  }

  private static List<ResolvedColumn> findTerminalParentsForColumn(
      ResolvedColumn column, Map<String, ResolvedComputedColumn> computedColumns) {

    ArrayList<ResolvedColumn> result = new ArrayList<>();
    Queue<ResolvedColumn> resolutionQueue = new ArrayDeque<>(List.of(column));

    while (resolutionQueue.peek() != null) {
      ResolvedColumn next = resolutionQueue.remove();
      ResolvedComputedColumn computationForColumn = computedColumns.get(next.shortDebugString());

      if (computationForColumn == null) {
        // If it is not computed, then it is terminal
        result.add(next);
      } else {
        List<ResolvedColumnRef> parentsReferenced =
            extractParentsFromExpression(computationForColumn.getExpr());
        parentsReferenced.forEach(parent -> resolutionQueue.add(parent.getColumn()));
      }
    }

    return result;
  }

  private static List<ResolvedColumn> findTerminalParentsForExpression(
      ResolvedExpr expression, Map<String, ResolvedComputedColumn> computedColumns) {
    List<ResolvedColumnRef> parentsReferenced = extractParentsFromExpression(expression);

    return parentsReferenced.stream()
        .map(ResolvedColumnRef::getColumn)
        .flatMap(parent -> findTerminalParentsForColumn(parent, computedColumns).stream())
        .collect(Collectors.toList());
  }

  private static Set<ColumnLineage> extractColumnLevelLineage(
      ResolvedCreateTableAsSelectStmt createTableAsSelectStmt) {

    String tablePath = String.join(".", createTableAsSelectStmt.getNamePath());

    List<ResolvedOutputColumn> outputColumns = createTableAsSelectStmt.getOutputColumnList();

    Map<String, ResolvedComputedColumn> computedColumnsMap =
        getComputedColumnsMap(createTableAsSelectStmt);

    Map<String, List<ResolvedColumn>> outputColumnToParentColumns = outputColumns.stream()
        .collect(Collectors.toMap(
            ResolvedOutputColumn::getName,
            outputColumn ->
                findTerminalParentsForColumn(outputColumn.getColumn(), computedColumnsMap)
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

    Map<String, ResolvedComputedColumn> computedColumnsMap = getComputedColumnsMap(query);

    Set<ColumnLineage> result = new HashSet<>(insertedColumns.size());

    for (int i = 0; i < insertedColumns.size(); i++) {
      ResolvedColumn insertedColumn = insertedColumns.get(i);
      ResolvedColumn matchingColumnInQuery = query.getColumnList().get(i);
      List<ResolvedColumn> parents = findTerminalParentsForColumn(
          matchingColumnInQuery, computedColumnsMap);

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
      Map<String, ResolvedComputedColumn> computedColumns,
      ResolvedUpdateItem updateItem) {

    ResolvedExpr target = updateItem.getTarget();
    ResolvedExpr updateExpression = updateItem.getSetValue().getValue();

    if (!(target instanceof ResolvedColumnRef)) {
      // TODO: Do we need to handle other types of inserts?
      //  See: https://github.com/google/zetasql/blob/5133c6e373a3f67f7f40b0619a2913c3fcab8171/zetasql/resolved_ast/gen_resolved_ast.py#L5564
      return Optional.empty();
    }

    ResolvedColumnRef targetColumnRef = (ResolvedColumnRef) target;
    List<ResolvedColumn> parents =
        findTerminalParentsForExpression(updateExpression, computedColumns);

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
    Map<String, ResolvedComputedColumn> computedColumnsMap = getComputedColumnsMap(updateStmt);
    List<ResolvedUpdateItem> updateItems = updateStmt.getUpdateItemList();

    return updateItems.stream()
        .map(updateItem -> extractColumnLevelLineage(targetTable, computedColumnsMap, updateItem))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toSet());
  }

  private static Set<ColumnLineage> extractColumnLevelLineage(
      Table targetTable,
      Map<String, ResolvedComputedColumn> computedColumns,
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
        List<ResolvedColumn> parents = findTerminalParentsForExpression(rowColumn, computedColumns);

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
          .map(updateItem -> extractColumnLevelLineage(targetTable, computedColumns, updateItem))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toSet());
    }

    return Set.of();
  }

  private static Set<ColumnLineage> extractColumnLevelLineage(ResolvedMergeStmt mergeStmt) {
    Table targetTable = mergeStmt.getTableScan().getTable();
    Map<String, ResolvedComputedColumn> computedColumnsMap = getComputedColumnsMap(mergeStmt);

    return mergeStmt.getWhenClauseList()
        .stream()
        .map(mergeWhen -> extractColumnLevelLineage(targetTable, computedColumnsMap, mergeWhen))
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

    throw new IllegalArgumentException(
        "Cannot extract column lineage from statement of type " + statement.getClass().getName());
  }

}
