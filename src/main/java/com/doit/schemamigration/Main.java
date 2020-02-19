package com.doit.schemamigration;

import static com.doit.schemamigration.Parsers.JsonToTableRow.convert;
import static com.doit.schemamigration.Parsers.TableRowToSchema.convertToSchema;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_RELAXATION;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy.retryTransientErrors;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.readStrings;

import com.doit.schemamigration.Parsers.JsonToDestinationTable;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.Schema;
import java.util.EnumSet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;

class Main {
  public static void main(String[] args) {
    final PipelineHelperOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineHelperOptions.class);
    options.setStreaming(true);

    final Pipeline pipeline = Pipeline.create(options);
    // This ensures dataflow can encode and decode BigQueryInsertErrors
    final CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        TypeDescriptor.of(BigQueryInsertError.class), BigQueryInsertErrorCoder.of());

    final PCollection<TableRow> pubSubData =
        pubSubToJson(pipeline, readStrings().fromTopic(options.getInputTopic()));

    final PCollection<BigQueryInsertError> failedInserts =
        pubSubData
            .apply(
                "Attempt to Write to BigQuery",
                BigQueryIO.writeTableRows()
                    .to(
                        (input) ->
                            JsonToDestinationTable.getTableName(
                                input,
                                options.getTargetTableName(),
                                options.getFailOverTableName()))
                    .withCreateDisposition(CREATE_IF_NEEDED)
                    .withWriteDisposition(WRITE_APPEND)
                    .withSchemaUpdateOptions(
                        EnumSet.of(ALLOW_FIELD_ADDITION, ALLOW_FIELD_RELAXATION))
                    .withExtendedErrorInfo()
                    .withFailedInsertRetryPolicy(retryTransientErrors()))
            .getFailedInsertsWithErr();

    failedInserts
        .apply("Key by table", ParDo.of(new KeyByDestTable()))
        .apply(
            "Gather up schema changes",
            Window.<KV<String, Schema>>into(
                    FixedWindows.of(Duration.standardMinutes(options.getWindowSize())))
                .withAllowedLateness(Duration.ZERO))
        .apply("Combine by Schema", Combine.perKey(new CombineBySchema()));

    pipeline.run();
  }

  static class CombineBySchema implements SerializableFunction<Iterable<Schema>, Schema> {
    @Override
    public Schema apply(Iterable<Schema> input) {
      return StreamSupport.stream(input.spliterator(), false)
          .reduce(
              Schema.of(),
              (Schema a, Schema b) -> {
                a.getFields().addAll(b.getFields());
                return a;
              });
    }
  }

  static class KeyByDestTable extends DoFn<BigQueryInsertError, KV<String, Schema>> {
    @ProcessElement
    public void processElement(
        @Element BigQueryInsertError error, OutputReceiver<KV<String, Schema>> out) {
      // Use OutputReceiver.output to emit the output element.
      out.output(KV.of(error.getTable().toString(), convertToSchema(error.getRow())));
    }
  }

  public static PCollection<TableRow> pubSubToJson(
      final Pipeline pipeline, PubsubIO.Read<String> stringRead) {
    return pipeline
        .apply("Read PubSub Messages", stringRead)
        .apply(
            "Convert data to json",
            FlatMapElements.into(TypeDescriptor.of(TableRow.class))
                .via(
                    (String ele) ->
                        Stream.of(convert(ele))
                            .filter(tableRow -> !tableRow.isEmpty())
                            .collect(toList())));
  }
}
