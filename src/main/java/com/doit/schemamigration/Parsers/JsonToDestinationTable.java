package com.doit.schemamigration.Parsers;

import static java.util.Objects.requireNonNull;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.ValueInSingleWindow;

public final class JsonToDestinationTable {
  public static TableDestination getTableName(
      final ValueInSingleWindow<TableRow> tableRow,
      final String tableNameAttr,
      final String failOverTableName) {
    final String tableDest =
        requireNonNull(tableRow.getValue())
            .getOrDefault(tableNameAttr, failOverTableName)
            .toString();

    return new TableDestination(tableDest, "Automatically created");
  }
}
