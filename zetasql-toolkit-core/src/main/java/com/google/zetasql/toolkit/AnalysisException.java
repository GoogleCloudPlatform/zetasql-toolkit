package com.google.zetasql.toolkit;

import com.google.zetasql.SqlException;

/**
 * Exception thrown by the {@link ZetaSQLToolkitAnalyzer} when analysis fails.
 */
public class AnalysisException extends RuntimeException {

  public AnalysisException(String message) {
    super(message);
  }

  public AnalysisException(SqlException cause) {
    super(cause.getMessage(), cause);
  }

}
