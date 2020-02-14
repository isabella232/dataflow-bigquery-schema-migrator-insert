package com.doit.schemamigration;

import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.Validation.Required;

public interface PipelineHelperOptions extends PipelineOptions, StreamingOptions {
  @Description("The Cloud Pub/Sub topic to read from.")
  @Required
  String getInputTopic();

  void setInputTopic(final String value);

  @Description("The name of the json attribute which will contain the full target table name.")
  @Required
  String getTargetTableName();

  void setTargetTableName(final String value);

  @Description("The name of the full failure table.")
  @Required
  String getFailOverTableName();

  void setFailOverTableName(final String value);

  @Description("Time to consolidate Schema errors")
  @Default.Integer(3)
  Integer getWindowSize();

  void setWindowSize(final Integer value);
}
