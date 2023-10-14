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
import com.google.zetasql.Function;
import com.google.zetasql.StructType;
import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAggregateFunctionCall;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAnalyticFunctionCall;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedCast;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedColumnRef;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedFunctionCall;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedFunctionCallBase;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedGetStructField;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedMakeStruct;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedSubqueryExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWithExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.Visitor;
import com.google.zetasql.resolvedast.ResolvedSubqueryExprEnums.SubqueryType;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Implements finding the direct {@link ResolvedColumn} parent of a {@link ResolvedExpr}.
 */
class ExpressionParentFinder extends Visitor {
  private final Stack<ResolvedColumn> result = new Stack<>();
  public static List<ResolvedColumn> findDirectParentsForExpression(ResolvedExpr expression) {
    ExpressionParentFinder extractor = new ExpressionParentFinder();
    expression.accept(extractor);
    return extractor.result;
  }

  public void visit(ResolvedColumnRef columnRef) {
    result.push(columnRef.getColumn());
  }

  public void visit(ResolvedWithExpr withExpr) {
    withExpr.getExpr().accept(this);
  }

  public void visit(ResolvedSubqueryExpr subqueryExpr) {
    List<SubqueryType> scalarOrArray = ImmutableList.of(SubqueryType.SCALAR, SubqueryType.ARRAY);
    if (scalarOrArray.contains(subqueryExpr.getSubqueryType())) {
      ResolvedScan subquery = subqueryExpr.getSubquery();
      ResolvedColumn subqueryOutputColumn = subquery.getColumnList().get(0);
      result.push(subqueryOutputColumn);
    }
  }

  public void visitResolvedFunctionCallBase(ResolvedFunctionCallBase functionCallBase) {
    Function function = functionCallBase.getFunction();
    List<ResolvedExpr> arguments = functionCallBase.getArgumentList();
    int numberOfArguments = arguments.size();

    List<ResolvedExpr> expressionsToVisit;

    switch (function.getName().toLowerCase()) {
      case "$case_no_value":
        // Must keep all odd arguments (the WHEN expressions), plus the last one (the ELSE expr)
        expressionsToVisit = IntStream.range(0, numberOfArguments)
            .filter(i -> i % 2 == 1 || i == numberOfArguments - 1)
            .mapToObj(arguments::get)
            .collect(Collectors.toList());
        break;
      case "$case_with_value":
        // Must keep all even arguments (the WHEN expressions) but the first one (the CASE value),
        // plus the last one (the ELSE expr)
        expressionsToVisit = IntStream.range(0, numberOfArguments)
            .filter(i -> (i != 0 && i % 2 == 0) || i == numberOfArguments - 1)
            .mapToObj(arguments::get)
            .collect(Collectors.toList());
        break;
      case "if":
        // Remove the first argument (the condition)
        expressionsToVisit = arguments.subList(1, arguments.size());
        break;
      case "nullif":
        // Keep only the first argument (the value)
        expressionsToVisit = ImmutableList.of(arguments.get(0));
        break;
      default:
        expressionsToVisit = arguments;
    }

    expressionsToVisit.forEach(expression -> expression.accept(this));
  }

  public void visit(ResolvedFunctionCall functionCall) {
    visitResolvedFunctionCallBase(functionCall);
  }

  public void visit(ResolvedAggregateFunctionCall aggregateFunctionCall) {
    visitResolvedFunctionCallBase(aggregateFunctionCall);
  }

  public void visit(ResolvedAnalyticFunctionCall analyticFunctionCall) {
    visitResolvedFunctionCallBase(analyticFunctionCall);
  }

  public void visit(ResolvedMakeStruct makeStruct) {
    makeStruct.getFieldList().forEach(fieldExpr -> fieldExpr.accept(this));
  }

  public void visit(ResolvedGetStructField getStructField) {
    ResolvedExpr structExpression = getStructField.getExpr();
    StructType structExpressionType = structExpression.getType().asStruct();
    int accessedFieldIndex = (int) getStructField.getFieldIdx();
    String accessedFieldName = structExpressionType.getField(accessedFieldIndex).getName();

    if (structExpression instanceof ResolvedMakeStruct) {
      // If the user made a STRUCT and immediately accessed a field in it, only consider the parent
      // columns of the corresponding field.
      ResolvedMakeStruct makeStructExpression = (ResolvedMakeStruct) structExpression;
      StructType makeStructExpressionType = makeStructExpression.getType().asStruct();
      int numberOfFields = makeStructExpressionType.getFieldCount();

      int fieldIndex = IntStream.range(0, numberOfFields)
          .filter(index ->
              makeStructExpressionType.getField(index).getName().equals(accessedFieldName))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException(
              "Field " + accessedFieldName + " does not exist in STRUCT of type: "
                  + makeStructExpressionType));
      ResolvedExpr fieldExpression = makeStructExpression.getFieldList().get(fieldIndex);

      List<ResolvedColumn> parentColumns = ExpressionParentFinder.findDirectParentsForExpression(fieldExpression);
      parentColumns.forEach(result::push);
    } else {
      structExpression.accept(this);

      ResolvedColumn structColumn = result.pop();
      ResolvedColumn parentColumn = new ResolvedColumn(
          structColumn.getId(),
          structColumn.getTableName(),
          structColumn.getName() + "." + accessedFieldName,
          getStructField.getType());
      this.result.push(parentColumn);
    }
  }

  public void visit(ResolvedCast cast) {
    cast.getExpr().accept(this);
  }

  private ExpressionParentFinder() {}

}
