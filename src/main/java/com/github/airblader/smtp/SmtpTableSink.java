package com.github.airblader.smtp;

import lombok.RequiredArgsConstructor;
import lombok.var;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;

@RequiredArgsConstructor
public class SmtpTableSink implements DynamicTableSink {
  private final TableSchema schema;
  private final SmtpSinkOptions connectorOptions;

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    return ChangelogMode.insertOnly();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    var fieldNames = schema.getFieldNames();

    var sinkFunction = new SmtpSinkFunction(connectorOptions, fieldNames);
    return SinkFunctionProvider.of(sinkFunction);
  }

  @Override
  public DynamicTableSink copy() {
    return new SmtpTableSink(schema, connectorOptions);
  }

  @Override
  public String asSummaryString() {
    return "SMTP Table Sink";
  }
}
