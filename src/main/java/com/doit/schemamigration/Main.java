package com.doit.schemamigration;

import com.doit.schemamigration.Parsers.JsonToDestinationTable;
import com.doit.schemamigration.Parsers.JsonToTableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy.retryTransientErrors;

class Main {
  public static void main(String[] args) {
    final PipelineHelperOptions options = PipelineOptionsFactory
      .fromArgs(args)
      .withValidation()
      .as(PipelineHelperOptions.class);
    options.setStreaming(true);

    final Pipeline pipeline = Pipeline.create(options);
    // This ensures dataflow and encode and decode BigQueryInsertErrors
    final CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(TypeDescriptor.of(BigQueryInsertError.class), BigQueryInsertErrorCoder.of());

    final PCollection<BigQueryInsertError> failedInserts = pipeline
      .apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
      .apply(
        "Attempt to Write to BigQuery",
        BigQueryIO.<String>write()
          .to((input) -> JsonToDestinationTable.getTableName(input, options.getTargetTableName(), options.getFailOverTableName()))
          .withFormatFunction(
            (String msg) -> JsonToTableRow.convert(msg))
          .withCreateDisposition(CREATE_IF_NEEDED)
          .withWriteDisposition(WRITE_APPEND)
          .withExtendedErrorInfo()
          .withFailedInsertRetryPolicy(retryTransientErrors()))
      .getFailedInsertsWithErr();

    pipeline.run();
  }
}
