package com.doit.schemamigration.Parsers;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import com.google.api.services.bigquery.model.TableRow;
import org.junit.Test;

public class JsonToTableRowTest {
  @Test
  public void convertFromStringTest() {
    assertThat(JsonToTableRow.convertFromString("test"), is(equalTo(new TableRow())));
  }
}
