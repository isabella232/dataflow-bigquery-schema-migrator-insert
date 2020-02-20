package com.doit.schemamigration;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.Validation.Required;

public interface PipelineHelperOptions extends PipelineOptions, StreamingOptions, GcpOptions {
  @Description("The Cloud Pub/Sub topic to read from.")
  @Required
  String getInputTopic();

  void setInputTopic(final String value);

  @Description("The name of the target Dataset")
  @Required
  String getTargetDataset();

  void setTargetDataset(final String value);

  @Description("The name of the json attribute which will contain the full target table name.")
  @Required
  String getJsonAttributeForTargetTableName();

  void setJsonAttributeForTargetTableName(final String value);

  @Description("The name of json attribute for processed time to be stored in")
  @Default.String("processed_timestamp")
  String getJsonAttributeForProccess();

  void setJsonAttributeForProccess(final String value);

  @Description("The name of the full failure table.")
  @Required
  String getFailOverTableName();

  void setFailOverTableName(final String value);

  @Description("Time to consolidate Schema errors")
  @Default.Integer(1)
  Integer getWindowSize();

  void setWindowSize(final Integer value);
}
