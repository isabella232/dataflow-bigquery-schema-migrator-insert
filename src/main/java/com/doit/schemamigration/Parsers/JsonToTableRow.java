package com.doit.schemamigration.Parsers;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;

public final class JsonToTableRow {
  public static TableRow convert(final String json) {
    final Gson gson = new Gson();
    return gson.fromJson(json, TableRow.class);
  }
}
