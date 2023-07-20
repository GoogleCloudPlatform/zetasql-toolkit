package com.google.zetasql.toolkit.tools.lineage;

import com.google.zetasql.resolvedast.ResolvedColumn;
import java.util.Objects;

public class ColumnEntity {

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
