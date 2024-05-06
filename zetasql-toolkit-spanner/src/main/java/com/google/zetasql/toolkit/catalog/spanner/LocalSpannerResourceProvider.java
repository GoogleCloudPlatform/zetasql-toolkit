package com.google.zetasql.toolkit.catalog.spanner;

import com.google.zetasql.SimpleTable;
import com.google.zetasql.toolkit.catalog.io.CatalogResources;
import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link SpannerResourceProvider} implementation that fetches tables from a local {@link
 * CatalogResources} object.
 */
public class LocalSpannerResourceProvider implements SpannerResourceProvider {

  private final CatalogResources catalogResources;

  public LocalSpannerResourceProvider(CatalogResources catalogResources) {
    this.catalogResources = catalogResources;
  }

  @Override
  public List<SimpleTable> getTables(List<String> tableNames) {
    return catalogResources.getTables().stream()
        .filter(table -> tableNames.contains(table.getName()))
        .collect(Collectors.toList());
  }

  @Override
  public List<SimpleTable> getAllTablesInDatabase() {
    return catalogResources.getTables();
  }
}
