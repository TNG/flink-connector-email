package com.github.airblader.imap.table;

import static com.github.airblader.ConfigOptionsLibrary.*;
import static com.github.airblader.ConfigUtils.validateOptionOrEnv;

import com.github.airblader.imap.ScanMode;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

public class ImapTableSourceFactory implements DynamicTableSourceFactory {
  @Override
  public String factoryIdentifier() {
    return "imap";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return new HashSet<>();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return TABLE_OPTIONS;
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    var factoryHelper = FactoryUtil.createTableFactoryHelper(this, context);

    factoryHelper.validate();
    validateOptionOrEnv(factoryHelper.getOptions(), HOST, ENV_HOST);

    ScanMode scanMode;
    try {
      scanMode = ScanMode.from(factoryHelper.getOptions().get(MODE));
    } catch (IllegalArgumentException e) {
      throw new ValidationException("Invalid value for " + MODE.key(), e);
    }

    var schema = context.getCatalogTable().getSchema();
    var connectorOptions =
        ImapSourceOptions.builder()
            .host(factoryHelper.getOptions().get(HOST))
            .envHost(factoryHelper.getOptions().get(ENV_HOST))
            .port(factoryHelper.getOptions().get(PORT))
            .envPort(factoryHelper.getOptions().get(ENV_PORT))
            .user(factoryHelper.getOptions().get(USER))
            .envUser(factoryHelper.getOptions().get(ENV_USER))
            .password(factoryHelper.getOptions().get(PASSWORD))
            .envPassword(factoryHelper.getOptions().get(ENV_PASSWORD))
            .ssl(factoryHelper.getOptions().get(SSL))
            .folder(factoryHelper.getOptions().get(FOLDER))
            .mode(scanMode)
            .scanFromUID(factoryHelper.getOptions().get(SCAN_FROM_UID))
            .connectionTimeout(factoryHelper.getOptions().get(CONNECTION_TIMEOUT))
            .batchSize(factoryHelper.getOptions().get(BATCH_SIZE))
            .idle(factoryHelper.getOptions().get(IDLE))
            .heartbeat(factoryHelper.getOptions().get(HEARTBEAT))
            .heartbeatInterval(factoryHelper.getOptions().get(HEARTBEAT_INTERVAL))
            .interval(factoryHelper.getOptions().get(INTERVAL))
            .deletions(factoryHelper.getOptions().get(DELETIONS))
            .addressFormat(factoryHelper.getOptions().get(ADDRESS_FORMAT))
            .build();

    return new ImapTableSource(connectorOptions, schema);
  }
}
