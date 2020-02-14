package com.doit.schemamigration.Parsers;

import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.ValueInSingleWindow;

public final class JsonToDestinationTable {
  public static TableDestination getTableName(
    final ValueInSingleWindow<String> jsonString,
    final String tableNameAttr,
    final String failOverTableName) {
    final String tableDest = JsonToTableRow.convert(jsonString.getValue()).getOrDefault(tableNameAttr, failOverTableName).toString();

    return new TableDestination(tableDest, "Automatically created");
  }
}
