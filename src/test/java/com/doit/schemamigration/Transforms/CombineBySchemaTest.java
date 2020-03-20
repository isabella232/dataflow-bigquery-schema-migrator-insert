package com.doit.schemamigration.Transforms;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.util.List;
import org.junit.Test;

public class CombineBySchemaTest {
  @Test
  public void combineTest() {
    final CombineBySchema combineBySchema = new CombineBySchema();
    final Schema schema1 = Schema.of(Field.of("test1", StandardSQLTypeName.STRING));
    final Schema schema2 = Schema.of(Field.of("test2", StandardSQLTypeName.INT64));
    final Schema schema3 = Schema.of(Field.of("test3", StandardSQLTypeName.DATETIME));
    final List<Schema> schemas = asList(schema1, schema2, schema3);
    final Schema result = combineBySchema.apply(schemas);
    final List<Field> fields =
        schemas.stream().map(schema -> schema.getFields().get(0)).collect(toList());
    final Schema expectedResult = Schema.of(fields);
    assertThat(result, is(equalTo(expectedResult)));
  }
}
