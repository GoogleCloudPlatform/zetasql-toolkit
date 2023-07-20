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
