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

package com.google.zetasql.toolkit.examples;

import com.google.common.collect.ImmutableList;
import com.google.zetasql.toolkit.catalog.spanner.SpannerCatalog;

public class AddResourcesToSpannerCatalog {

  public static void main(String[] args) {
    SpannerCatalog catalog = SpannerCatalog.usingSpannerClient("projectId", "instance", "database");

    // Add a table or a set of tables by name
    // Views are considered tables as well, so they can be added this way to the catalog
    catalog.addTable("bigquery-public-data.samples.wikipedia");
    catalog.addTables(
        ImmutableList.of(
            "bigquery-public-data.samples.wikipedia",
            "bigquery-public-data.samples.github_nested"));

    // Add all tables in the database
    // Views are considered tables as well, so they will be added to the catalog too
    catalog.addAllTablesInDatabase();
  }
}
