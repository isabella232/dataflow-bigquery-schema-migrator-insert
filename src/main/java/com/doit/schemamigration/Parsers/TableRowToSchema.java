package com.doit.schemamigration.Parsers;

import static java.util.stream.Collectors.toList;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public final class TableRowToSchema {
  public static Schema convertToSchema(final TableRow tableRow) {
    final List<Field> fields =
        tableRow
            .entrySet()
            .stream()
            .map(item -> Field.of(item.getKey(), findType(item.getValue())))
            .collect(toList());
    return Schema.of(fields);
  }

  static StandardSQLTypeName findType(final Object object) {
    if (object instanceof HashMap) {
      throw new IllegalStateException("Type Record not implemented Yet");
    }
    if (object instanceof Collection) {
      throw new IllegalStateException("Type Repeated not implemented Yet");
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
  private static final DateTimeFormatter dateTimeFormatter =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();
}
