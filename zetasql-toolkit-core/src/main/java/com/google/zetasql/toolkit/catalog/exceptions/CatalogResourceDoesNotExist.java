package com.google.zetasql.toolkit.catalog.exceptions;

public class CatalogResourceDoesNotExist extends CatalogException {

  private final String resourceName;

  public CatalogResourceDoesNotExist(String resourceName) {
    super("Catalog resource already exists: " + resourceName);
    this.resourceName = resourceName;
  }

  public CatalogResourceDoesNotExist(String resourceName, String message) {
    super(message);
    this.resourceName = resourceName;
  }

  public CatalogResourceDoesNotExist(String resourceName, String message, Throwable cause) {
    super(message, cause);
    this.resourceName = resourceName;
  }

  public String getResourceName() {
    return resourceName;
  }

}
