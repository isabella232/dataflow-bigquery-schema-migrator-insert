package com.doit.schemamigration.Parsers;

import com.google.api.services.bigquery.model.TableRow;
import com.owlike.genson.Genson;
import com.owlike.genson.JsonBindingException;
import com.owlike.genson.stream.JsonStreamException;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JsonToTableRow {
  static final Logger logger = LoggerFactory.getLogger(JsonToTableRow.class);

  public static TableRow convertFromString(final String json) {
    final TableRow out = new TableRow();
    final Genson genson = new Genson();
    try {
      final HashMap<String, Object> convertedObject = genson.deserialize(json, HashMap.class);
      out.putAll(convertedObject);
    } catch (JsonBindingException | JsonStreamException | NullPointerException e) {
      logger.debug("Failed to parse message:\n {}\nException thrown: {}", json, e);
    }
    return out;
  }
}
