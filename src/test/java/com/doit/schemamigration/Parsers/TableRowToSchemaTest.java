package com.doit.schemamigration.Parsers;

import static com.doit.schemamigration.Parsers.TableRowToSchema.convertToSchema;
import static com.google.cloud.bigquery.LegacySQLTypeName.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.Schema;
import org.junit.Test;

public class TableRowToSchemaTest {
  @Test
  public void toSchemaTestSimple() {
    final TableRow tableRow = new TableRow();
    tableRow.put("int", 1);
    tableRow.put("string", "a");
    tableRow.put("int64", 3L);
    tableRow.put("date", "2020-01-01 00:00:00");
    tableRow.put("double", 3.0);

    final Schema schema = convertToSchema(tableRow);
    assertThat(schema.getFields().get(0).getType(), is(equalTo(INTEGER)));
    assertThat(schema.getFields().get(1).getType(), is(equalTo(STRING)));
    assertThat(schema.getFields().get(2).getType(), is(equalTo(INTEGER)));
    assertThat(schema.getFields().get(3).getType(), is(equalTo(DATETIME)));
    assertThat(schema.getFields().get(4).getType(), is(equalTo(FLOAT)));
  }

  @Test
  public void toSchemaFailBack() {
    final TableRow tableRow = new TableRow();
    tableRow.put("int", Object.class);
    final Schema schema = convertToSchema(tableRow);
    assertThat(schema.getFields().get(0).getType(), is(equalTo(STRING)));
  }
}
