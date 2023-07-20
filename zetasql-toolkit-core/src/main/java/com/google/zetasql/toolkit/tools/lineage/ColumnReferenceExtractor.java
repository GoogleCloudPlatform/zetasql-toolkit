package com.google.zetasql.toolkit.tools.lineage;

import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedColumnRef;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.Visitor;
import java.util.ArrayList;
import java.util.List;

class ColumnReferenceExtractor extends Visitor {
  // TODO: Handle different types of expressions. E.g. special cases for functions,
  //  subqueries, etc.

  private final ArrayList<ResolvedColumn> result = new ArrayList<>();

  public static List<ResolvedColumn> extractFromExpression(ResolvedExpr expression) {
    ColumnReferenceExtractor extractor = new ColumnReferenceExtractor();
    expression.accept(extractor);
    return extractor.result;
  }

  public void visit(ResolvedColumnRef columnRef) {
    result.add(columnRef.getColumn());
  }

  private ColumnReferenceExtractor() {}

}
