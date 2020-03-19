package com.doit.schemamigration.Parsers;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public final class JsonToTableRow {
  public static TableRow convertFromString(final String json) {
    final Gson gson = new Gson();
    try {
      return gson.fromJson(json, TableRow.class);
    } catch (JsonSyntaxException | NullPointerException e) {
      return new TableRow();
    }
  }
}
