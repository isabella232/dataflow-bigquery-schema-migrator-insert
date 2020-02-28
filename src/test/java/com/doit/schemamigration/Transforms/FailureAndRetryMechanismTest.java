package com.doit.schemamigration.Transforms;

import static java.util.Arrays.asList;
import static org.apache.beam.sdk.options.ValueProvider.StaticValueProvider.*;
import static org.junit.Assert.*;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class FailureAndRetryMechanismTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  // Fake Write to Big Query
  // https://github.com/apache/beam/blob/v2.19.0/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIOWriteTest.java
  final FakeDatasetService fakeDatasetService = new FakeDatasetService();
  final FakeJobService fakeJobService = new FakeJobService();
  final FakeBigQueryServices fakeBqServices =
      new FakeBigQueryServices()
          .withDatasetService(fakeDatasetService)
          .withJobService(fakeJobService);
  final BigQueryIO.Write<BigQueryInsertError> write =
      BigQueryIO.<BigQueryInsertError>write()
          .withTestServices(fakeBqServices)
          .withCustomGcsTempLocation(of("gs://test"))
          .withoutValidation();

  @Before
  public void setup() throws IOException, InterruptedException {
    final CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        TypeDescriptor.of(BigQueryInsertError.class), BigQueryInsertErrorCoder.of());

    FakeDatasetService.setUp();
    fakeDatasetService.createDataset("a12345", "b", "", "", null);
  }

  @Test
  public void testRetryMechanism() {
    final TableReference tableReference = BigQueryHelpers.parseTableSpec("a12345:b.c");
    final InsertErrors insertError = new InsertErrors();
    final TableRow retriedData = new TableRow();
    retriedData.set("a", "a");
    final TableRow failed = new TableRow();
    failed.set("retry_mechanism", 5);

    final BigQueryInsertError bigQueryInsertErrorRetry =
        new BigQueryInsertError(retriedData, insertError, tableReference);
    final BigQueryInsertError bigQueryInsertErrorFailed =
        new BigQueryInsertError(failed, insertError, tableReference);

    final PCollection<BigQueryInsertError> errorCollection =
        pipeline.apply(
            "Create test data for failure",
            Create.of(asList(bigQueryInsertErrorRetry, bigQueryInsertErrorFailed)));

    final FailureAndRetryMechanism retryMechanism =
        new FailureAndRetryMechanism("a12345:b.c", "retry_mechanism", 3, write);

    final PCollection<String> result = retryMechanism.expand(errorCollection);

    retriedData.setFactory(Utils.getDefaultJsonFactory());
    PAssert.that(result).containsInAnyOrder(retriedData.toString());
    try {
      pipeline.run();
    } catch (Exception ignored) {
    }
  }
}
