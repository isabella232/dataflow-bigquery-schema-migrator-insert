package com.doit.schemamigration.Parsers;

import static com.google.cloud.bigquery.StandardSQLTypeName.ARRAY;
import static com.google.cloud.bigquery.StandardSQLTypeName.STRUCT;
import static java.util.stream.Collectors.toList;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TableRowToSchema {
  static final Logger logger = LoggerFactory.getLogger(TableRowToSchema.class);

  public static Schema convertToSchema(final TableRow tableRow) {
    final List<Field> fields =
        tableRow.entrySet().stream().map(item -> convertToField(item)).collect(toList());
    return Schema.of(fields);
  }

  public static Field convertToField(Map.Entry<String, Object> item) {
    final StandardSQLTypeName typeName = findType(item.getValue());
    switch (typeName) {
      case ARRAY:
        return null;
      case STRUCT:
        return null;
      default:
        return Field.of(item.getKey(), typeName);
    }
  }

  static StandardSQLTypeName findType(final Object object) {
    if (object instanceof HashMap) {
      return STRUCT;
    }
    if (object instanceof Collection) {
      return ARRAY;
    }
    return findSimpleType(object);
  }

  static StandardSQLTypeName findSimpleType(final Object object) {
    // Numeric Types
    if (object instanceof Integer || object instanceof Long) {
      return StandardSQLTypeName.INT64;
    }
    if (object instanceof Byte || object instanceof Byte[]) {
      return StandardSQLTypeName.BYTES;
    }
    if (object instanceof Double || object instanceof Float) {
      return StandardSQLTypeName.FLOAT64;
    }
    // Date/time check
    try {
      dateTimeFormatter.parseDateTime(object.toString());
      return StandardSQLTypeName.DATETIME;
    } catch (IllegalArgumentException ignored) {
    }
    return StandardSQLTypeName.STRING;
  }

  /**
   * Formats BigQuery seconds-since-epoch into String matching JSON export. Thread-safe and
   * immutable.
   */
  public static final DateTimeFormatter dateTimeFormatter =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();
}
