package com.github.airblader.imap.table;

import java.util.Arrays;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.RowKind;

public class ImapTableSource implements ScanTableSource, SupportsProjectionPushDown {
  private final ImapSourceOptions connectorOptions;
  private TableSchema schema;

  public ImapTableSource(ImapSourceOptions connectorOptions, TableSchema schema) {
    this.connectorOptions = connectorOptions;
    this.schema = schema;
  }

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

    var sourceFunction = new ImapSourceFunction(connectorOptions, fieldNames, rowType);
    return SourceFunctionProvider.of(sourceFunction, false);
  }

  @Override
  public void applyProjection(int[][] projectedFields) {
    this.schema = TableSchemaUtils.projectSchema(schema, projectedFields);
  }

  @Override
  public boolean supportsNestedProjection() {
    return false;
  }

  @Override
  public DynamicTableSource copy() {
    return new ImapTableSource(connectorOptions, schema);
  }

  @Override
  public String asSummaryString() {
    return "IMAP Table Source";
  }
}
