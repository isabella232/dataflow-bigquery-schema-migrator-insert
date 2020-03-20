package com.doit.schemamigration.Transforms;

import static com.doit.schemamigration.Transforms.MergeWithTableSchema.mergeSchemas;

import com.google.cloud.bigquery.Schema;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class CombineBySchema implements SerializableFunction<Iterable<Schema>, Schema> {
  @Override
  public Schema apply(Iterable<Schema> input) {
    return StreamSupport.stream(input.spliterator(), false)
        .reduce(Schema.of(), (Schema a, Schema b) -> Schema.of(mergeSchemas(b, a)));
  }
}
