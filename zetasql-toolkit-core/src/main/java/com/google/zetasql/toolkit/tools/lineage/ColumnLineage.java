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
