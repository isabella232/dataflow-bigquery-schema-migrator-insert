package com.doit.schemamigration;

import static com.google.cloud.bigquery.BigQueryOptions.DefaultBigQueryFactory;
import static com.google.cloud.bigquery.BigQueryOptions.newBuilder;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_RELAXATION;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy.neverRetry;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.readStrings;

import com.doit.schemamigration.Parsers.JsonToDestinationTable;
import com.doit.schemamigration.Transforms.*;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Schema;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;

class Main {
  public static void main(String[] args) {
    final PipelineHelperOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineHelperOptions.class);
    options.setStreaming(true);
    final String projectName = options.getProject();
    final String topicName = options.getInputTopic();
    final String subscription =
        String.format("projects/%s/subscriptions/%s", projectName, topicName);
    final String topic = String.format("projects/%s/topics/%s", projectName, topicName);
    final String processedTimeJsonField = options.getJsonAttributeForProccess();
    final String retryAttemptJsonField = options.getJsonAttributeForRetry();
    final Integer numberOf = options.getNumberOfAllowedAttempts();
    final Duration windowSize = Duration.standardMinutes(options.getWindowSize());

    // Ensure Dataset exists in bigquery
    final String datasetName = options.getTargetDataset();
    final BigQuery bigQuery =
        new DefaultBigQueryFactory().create(newBuilder().setProjectId(projectName).build());

    if (bigQuery.getDataset(datasetName) == null) {
      bigQuery.create(DatasetInfo.of(datasetName));
    }

    final String targetTableAtt = options.getJsonAttributeForTargetTableName();
    final String failOverTable =
        String.format("%s:%s.%s", projectName, datasetName, options.getFailOverTableName());
    final List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName(targetTableAtt).setType("STRING"));
    final TableSchema dummySchema = new TableSchema().setFields(fields);

    final Pipeline pipeline = Pipeline.create(options);
    // This ensures dataflow can encode and decode BigQueryInsertErrors
    final CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        TypeDescriptor.of(BigQueryInsertError.class), BigQueryInsertErrorCoder.of());

    final PCollection<TableRow> pubSubData =
        pipeline
            .apply("Read PubSub Messages", readStrings().fromSubscription(subscription))
            .apply(
                "Transform to Tablerow",
                new PubSubToJSON(processedTimeJsonField, retryAttemptJsonField));

    final PCollection<BigQueryInsertError> failedInserts =
        pubSubData
            .apply(
                "Attempt to Write to BigQuery",
                BigQueryIO.writeTableRows()
                    .to(
                        (input) ->
                            JsonToDestinationTable.getTableName(
                                input, projectName, datasetName, targetTableAtt))
                    .withCreateDisposition(CREATE_IF_NEEDED)
                    .withSchema(dummySchema)
                    .withWriteDisposition(WRITE_APPEND)
                    .withSchemaUpdateOptions(
                        EnumSet.of(ALLOW_FIELD_ADDITION, ALLOW_FIELD_RELAXATION))
                    .withExtendedErrorInfo()
                    .withFailedInsertRetryPolicy(neverRetry()))
            .getFailedInsertsWithErr();

    failedInserts
        .apply("Key by table", ParDo.of(new KeyByDestTable()))
        .apply(
            "Gather up schema changes",
            Window.<KV<String, Schema>>into(FixedWindows.of(windowSize))
                .withAllowedLateness(Duration.standardSeconds(1L))
                .discardingFiredPanes()
                .triggering(
                    Repeatedly.forever(
                        AfterFirst.of(
                            // Don't try to hard bruh
                            AfterPane.elementCountAtLeast(1000000),
                            AfterProcessingTime.pastFirstElementInPane().plusDelayOf(windowSize)))))
        .apply("Combine by Schema", Combine.perKey(new CombineBySchema()))
        .apply(
            "Merge with target table schema", ParDo.of(new MergeSchema(projectName, datasetName)));

    // Send failed rows back to pubsub
    failedInserts
        .apply(
            "Fail or Retry",
            new FailureAndRetryMechanism(
                failOverTable,
                retryAttemptJsonField,
                processedTimeJsonField,
                windowSize,
                numberOf,
                BigQueryIO.write()))
        .apply("Send Back to pubsub", PubsubIO.writeStrings().to(topic));

    pipeline.run();
  }
}
