package com.doit.schemamigration.Transforms;

import static java.util.Arrays.asList;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertErrorCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

public class FailureAndRetryMechanism
    extends PTransform<PCollection<BigQueryInsertError>, PCollection<String>> {
  static final TableSchema failOverSchema =
      new TableSchema()
          .setFields(
              asList(
                  new TableFieldSchema().setName("bad_data").setType("STRING"),
                  new TableFieldSchema().setName("table_dest").setType("STRING"),
                  new TableFieldSchema().setName("error").setType("STRING")));
  static final TupleTag<BigQueryInsertError> retry = new TupleTag<>();
  static final TupleTag<BigQueryInsertError> dump = new TupleTag<>();
  static final TupleTag<BigQueryInsertError> all = new TupleTag<>();
  final String failOverTable;
  final String retryAttemptJsonField;
  final Integer numberOfAllowedAttempts;
  final BigQueryIO.Write<BigQueryInsertError> write;

  public FailureAndRetryMechanism(
      final String failOverTable,
      final String retryAttemptJsonField,
      final Integer numberOfAllowedAttempts,
      final BigQueryIO.Write<BigQueryInsertError> write) {
    this.retryAttemptJsonField = retryAttemptJsonField;
    this.numberOfAllowedAttempts = numberOfAllowedAttempts;
    this.failOverTable = failOverTable;
    this.write = write;
  }

  @Override
  public PCollection<String> expand(PCollection<BigQueryInsertError> input) {
    final PCollectionTuple failures =
        input.apply(
            "Split out data not going to be retried",
            ParDo.of(new TagData()).withOutputTags(all, TupleTagList.of(asList(retry, dump))));

    failures
        .get(dump)
        .setCoder(BigQueryInsertErrorCoder.of())
        .apply(
            "Write to fail over table BigQuery",
            write
                .to(failOverTable)
                .withFormatFunction(
                    (BigQueryInsertError error) -> {
                      final TableRow tableRow = new TableRow();
                      tableRow.set("bad_data", error.getRow());
                      tableRow.set("table_dest", error.getTable().getTableId());
                      tableRow.set("error", error.getError().toString());
                      return tableRow;
                    })
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withSchema(failOverSchema)
                .withWriteDisposition(WRITE_APPEND));

    return failures
        .get(retry)
        .setCoder(BigQueryInsertErrorCoder.of())
        .apply(
            "Get Table Data",
            MapElements.into(TypeDescriptor.of(String.class))
                .via(
                    (BigQueryInsertError item) -> {
                      final TableRow tableRow = item.getRow();
                      tableRow.setFactory(Utils.getDefaultJsonFactory());
                      return tableRow.toString();
                    }));
  }

  class TagData extends DoFn<BigQueryInsertError, BigQueryInsertError> {
    @ProcessElement
    public void processElement(final ProcessContext c) {
      final BigQueryInsertError element = c.element();
      final int retryAttemptNumber =
          Integer.parseInt(element.getRow().getOrDefault(retryAttemptJsonField, 0).toString());
      if (retryAttemptNumber > numberOfAllowedAttempts) {
        c.output(dump, element);
      } else {
        c.output(retry, element);
      }
    }
  }
}
