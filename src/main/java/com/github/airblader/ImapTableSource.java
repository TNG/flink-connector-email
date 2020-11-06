package com.github.airblader;

import lombok.RequiredArgsConstructor;
import lombok.var;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;

@RequiredArgsConstructor
public class ImapTableSource implements ScanTableSource {
  private final TableSchema schema;
  private final ImapConnectorOptions connectorOptions;

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    var fieldNames = schema.getFieldNames();

    var sourceFunction = new ImapSourceFunction(connectorOptions, fieldNames);
    return SourceFunctionProvider.of(sourceFunction, false);
  }

  @Override
  public DynamicTableSource copy() {
    return new ImapTableSource(schema, connectorOptions);
  }

  @Override
  public String asSummaryString() {
    return "IMAP Table Source";
  }
}
