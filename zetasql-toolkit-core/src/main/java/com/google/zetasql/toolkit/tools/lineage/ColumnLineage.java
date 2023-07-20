package com.google.zetasql.toolkit.tools.lineage;

import java.util.Objects;
import java.util.Set;

public class ColumnLineage {

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
