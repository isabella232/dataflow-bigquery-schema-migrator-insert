package com.doit.schemamigration.Transforms;

import static com.doit.schemamigration.Parsers.TableRowToSchema.convertToSchema;

import com.google.cloud.bigquery.Schema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class KeyByDestTable extends DoFn<BigQueryInsertError, KV<String, Schema>> {
  @ProcessElement
  public void processElement(
      @Element BigQueryInsertError error, OutputReceiver<KV<String, Schema>> out) {
    // Use OutputReceiver.output to emit the output element.
    out.output(KV.of(error.getTable().getTableId(), convertToSchema(error.getRow())));
  }
}
