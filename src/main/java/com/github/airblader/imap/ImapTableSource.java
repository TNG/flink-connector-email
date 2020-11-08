package com.github.airblader.imap;

import java.util.Arrays;
import lombok.RequiredArgsConstructor;
import lombok.var;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.types.RowKind;

@RequiredArgsConstructor
public class ImapTableSource implements ScanTableSource {
  private final TableSchema schema;
  private final ConnectorOptions connectorOptions;

  @Override
  public ChangelogMode getChangelogMode() {
    var changeModeBuilder = ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT);
    if (connectorOptions.isDeletions()) {
      changeModeBuilder.addContainedKind(RowKind.DELETE);
    }

    return changeModeBuilder.build();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    var fieldNames = Arrays.asList(schema.getFieldNames());
    var rowType = schema.toRowDataType();

    var sourceFunction = new ImapSourceFunction(connectorOptions, rowType, fieldNames);
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
