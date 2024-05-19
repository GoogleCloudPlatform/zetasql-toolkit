package com.google.zetasql.toolkit.examples;

import com.google.common.collect.ImmutableMap;
import com.google.zetasql.SqlFormatter;
import com.google.zetasql.toolkit.tools.migration.ShardedTableMigrator;
import java.text.ParseException;

public class ShardedTableMigration {

  public static void main(String[] args) throws ParseException {
    String query =
        "SELECT * FROM `project.dataset.table_202405*` "
            + "WHERE _TABLE_SUFFIX > '01' AND _TABLE_SUFFIX < '10';";
    String projectId = "project";
    ImmutableMap<String, String> tablesToPartitionColumns =
        ImmutableMap.<String, String>builder()
            .put("project.dataset.table", "PARTITION_COLUMN_NAME")
            .build();
    String result = ShardedTableMigrator.migrate(query, projectId, tablesToPartitionColumns);
    String formatted = new SqlFormatter().lenientFormatSql(result);
    System.out.println(formatted);
  }
}
