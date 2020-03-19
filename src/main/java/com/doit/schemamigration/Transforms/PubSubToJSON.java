package com.doit.schemamigration.Transforms;

import static com.doit.schemamigration.Parsers.JsonToTableRow.convertFromString;
import static com.doit.schemamigration.Parsers.TableRowToSchema.dateTimeFormatter;
import static java.util.stream.Collectors.toList;

import com.google.api.services.bigquery.model.TableRow;
import java.util.stream.Stream;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;

public class PubSubToJSON extends PTransform<PCollection<String>, PCollection<TableRow>> {
  private final String processedTimeJsonField;
  private final String retryAttemptJsonField;

  public PubSubToJSON(final String processedTimeJsonField, final String retryAttemptJsonField) {
    this.processedTimeJsonField = processedTimeJsonField;
    this.retryAttemptJsonField = retryAttemptJsonField;
  }

  @Override
  public PCollection<TableRow> expand(PCollection<String> input) {
    return input.apply(
        "Convert data to json and add process timestamp",
        FlatMapElements.into(TypeDescriptor.of(TableRow.class))
            .via(
                (String ele) ->
                    Stream.of(convertFromString(ele))
                        .filter(tableRow -> !tableRow.isEmpty())
                        .peek(
                            tableRow -> {
                              tableRow.putIfAbsent(
                                  processedTimeJsonField, dateTimeFormatter.print(Instant.now()));
                              tableRow.set(
                                  retryAttemptJsonField,
                                  Integer.parseInt(
                                          tableRow
                                              .getOrDefault(retryAttemptJsonField, 0)
                                              .toString())
                                      + 1);
                            })
                        .collect(toList())));
  }
}
