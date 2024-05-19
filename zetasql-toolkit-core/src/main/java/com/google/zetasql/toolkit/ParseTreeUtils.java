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

import com.google.zetasql.parser.ASTNode;
import com.google.zetasql.parser.ASTNodes.ASTIdentifier;
import com.google.zetasql.parser.ASTNodes.ASTPathExpression;
import com.google.zetasql.parser.ParseTreeVisitor;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ParseTreeUtils {

  public static <T> List<T> findDescendantSubtreesWithKind(ASTNode tree, Class<T> kind) {
    List<T> result = new ArrayList<>();
    tree.accept(
        new ParseTreeVisitor() {
          @Override
          protected void defaultVisit(ASTNode node) {
            if (kind.isAssignableFrom(node.getClass())) {
              result.add(kind.cast(node));
            }
            super.defaultVisit(node);
          }
        });

    return result;
  }

  public static String pathExpressionToString(ASTPathExpression pathExpression) {
    return pathExpression.getNames().stream()
        .map(ASTIdentifier::getIdString)
        .collect(Collectors.joining("."));
  }
}
