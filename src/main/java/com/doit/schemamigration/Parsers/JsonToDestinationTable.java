package com.doit.schemamigration.Parsers;

import static java.util.Objects.requireNonNull;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.ValueInSingleWindow;

public final class JsonToDestinationTable {
  public static TableDestination getTableName(
      final ValueInSingleWindow<TableRow> tableRow,
      final String projectName,
      final String datasetName,
      final String tableNameAttr) {
    final String tableDest =
        requireNonNull(tableRow.getValue())
            .getOrDefault(tableNameAttr, "FAILED.TO.PROVIDE.VALID.TABLENAME")
            .toString();

    return new TableDestination(
        String.format("%s:%s.%s", projectName, datasetName, tableDest), "Automatically created");
  }
}
