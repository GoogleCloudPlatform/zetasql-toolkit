package com.google.zetasql.toolkit.examples;

import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.toolkit.AnalyzedStatement;
import com.google.zetasql.toolkit.ZetaSQLToolkitAnalyzer;
import com.google.zetasql.toolkit.catalog.bigquery.BigQueryCatalog;
import com.google.zetasql.toolkit.options.BigQueryLanguageOptions;
import java.util.Iterator;

/**
 * Example showcasing how we can analyze wildcard table reference using addAllTablesUsedInQuery.
 * This example queries data from all tables in the `noaa_gsod` dataset that begin with the string
 * `gsod202`.
 */
public class AnalyzeBigQueryWildcardReference {
  public static void main(String[] args) {
    String query = "SELECT date, temp, visib, wdsp FROM `bigquery-public-data.noaa_gsod.gsod202*`;";

    BigQueryCatalog catalog = BigQueryCatalog.usingBigQueryAPI("bigquery-public-data");

    AnalyzerOptions options = new AnalyzerOptions();
    options.setLanguageOptions(BigQueryLanguageOptions.get());

    // Will add bigquery-public-data.noaa_gsod.gsod2020, bigquery-public-data.noaa_gsod.gsod2021,
    // bigquery-public-data.noaa_gsod.gsod2022, etc to the catalog
    catalog.addAllTablesUsedInQuery(query, options);

    ZetaSQLToolkitAnalyzer analyzer = new ZetaSQLToolkitAnalyzer(options);
    Iterator<AnalyzedStatement> statementIterator = analyzer.analyzeStatements(query, catalog);

    statementIterator.forEachRemaining(
        statement -> statement.getResolvedStatement().ifPresent(System.out::println));
  }
}
