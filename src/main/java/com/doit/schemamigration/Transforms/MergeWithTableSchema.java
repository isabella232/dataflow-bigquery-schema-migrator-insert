package com.doit.schemamigration.Transforms;

import static com.google.cloud.bigquery.BigQueryOptions.newBuilder;

import com.google.cloud.bigquery.*;
import java.util.ArrayList;
import org.apache.beam.repackaged.core.org.antlr.v4.runtime.misc.OrderedHashSet;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeWithTableSchema
    extends PTransform<PCollection<KV<String, Schema>>, PCollection<KV<String, Schema>>> {
  final String projectName;
  final String datasetName;
  public static BigQuery bigQuery;
  final Logger logger = LoggerFactory.getLogger(MergeWithTableSchema.class);

  public MergeWithTableSchema(final String projectName, final String datasetName) {
    this.projectName = projectName;
    this.datasetName = datasetName;
    this.bigQuery =
        new BigQueryOptions.DefaultBigQueryFactory()
            .create(newBuilder().setProjectId(projectName).build());
  }

  @Override
  public PCollection<KV<String, Schema>> expand(final PCollection<KV<String, Schema>> input) {
    return input.apply("Merge Each Schema with pre-existing table", ParDo.of(new MergeSchema()));
  }

  class MergeSchema extends DoFn<KV<String, Schema>, KV<String, Schema>> {
    final Logger logger = LoggerFactory.getLogger(MergeSchema.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
      final KV<String, Schema> tableAndSchema = c.element();
      final TableId tableId = TableId.of(datasetName, tableAndSchema.getKey());
      final Table targetTable = bigQuery.getTable(tableId);
      logger.info(String.format("Target tale is: %s", targetTable));
      if (targetTable != null) {
        updateTargetTableSchema(tableAndSchema.getValue(), targetTable);
      } else {
        bigQuery.create(
            TableInfo.of(tableId, StandardTableDefinition.of(tableAndSchema.getValue())));
      }
      c.output(tableAndSchema);
    }
  }

  public void updateTargetTableSchema(final Schema tableAndSchema, final Table targetTable) {
    final Schema targetTableSchema = targetTable.getDefinition().getSchema();
    final OrderedHashSet<Field> fields = new OrderedHashSet<>();
    fields.addAll(tableAndSchema.getFields());
    fields.addAll(targetTableSchema != null ? targetTableSchema.getFields() : new ArrayList<>());

    if (targetTableSchema != null && fields.size() > targetTableSchema.getFields().size()) {
      logger.info("New schema created");
      logger.info(String.format("New schema is: %s", fields.toString()));
      targetTable
          .toBuilder()
          .setTableId(targetTable.getTableId())
          .setDefinition(StandardTableDefinition.of(Schema.of(fields)))
          .build();
      try {
        targetTable.update();
      } catch (BigQueryException e) {
        logger.error("Failed to add column");
        throw new IllegalStateException(e);
      }
      return;
    }
    logger.warn("New schema not created");
  }
}
