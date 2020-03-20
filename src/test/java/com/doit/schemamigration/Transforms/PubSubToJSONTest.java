package com.doit.schemamigration.Transforms;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class PubSubToJSONTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Test
  public void stringToTableRow() {
    final PubSubToJSON pubSubToJSON = new PubSubToJSON("time", "retry");
    final PCollection<String> testJSON =
        pipeline.apply("Create fake json data", Create.of("{\"a\":\"a\",\"time\":\"time\"}"));
    final PCollection<TableRow> result = pubSubToJSON.expand(testJSON);

    final TableRow expectedResult = new TableRow();
    expectedResult.set("a", "a");
    expectedResult.set("time", "time");
    expectedResult.set("retry", 1);
    PAssert.that(result).containsInAnyOrder(expectedResult);
    pipeline.run();
  }
}
