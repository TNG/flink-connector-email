package com.github.airblader.imap;

import static com.github.airblader.ConfigUtils.getValueWithEnvOverride;
import static com.github.airblader.ConfigUtils.validateOptionOrEnv;
import static com.github.airblader.ConnectorConfigOptions.*;

import java.util.HashSet;
import java.util.Set;
import lombok.var;
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
    var options = new HashSet<ConfigOption<?>>();
    options.add(HOST);
    options.add(ENV_HOST);
    options.add(PORT);
    options.add(ENV_PORT);
    options.add(USER);
    options.add(ENV_USER);
    options.add(PASSWORD);
    options.add(ENV_PASSWORD);
    options.add(SSL);
    options.add(FOLDER);
    options.add(MODE);
    options.add(CONNECTION_TIMEOUT);
    options.add(IDLE);
    options.add(HEARTBEAT);
    options.add(HEARTBEAT_INTERVAL);
    options.add(INTERVAL);
    options.add(DELETIONS);

    return options;
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    var factoryHelper = FactoryUtil.createTableFactoryHelper(this, context);

    factoryHelper.validate();
    validateOptionOrEnv(factoryHelper, HOST, ENV_HOST);

    ScanMode scanMode;
    try {
      scanMode = ScanMode.from(factoryHelper.getOptions().get(MODE));
    } catch (IllegalArgumentException e) {
      throw new ValidationException("Invalid value for " + MODE.key(), e);
    }

    var schema = context.getCatalogTable().getSchema();
    var connectorOptions =
        ConnectorOptions.builder()
            .host(getValueWithEnvOverride(factoryHelper, ENV_HOST, HOST))
            .port(getValueWithEnvOverride(factoryHelper, ENV_PORT, PORT, Integer::valueOf))
            .user(getValueWithEnvOverride(factoryHelper, ENV_USER, USER))
            .password(getValueWithEnvOverride(factoryHelper, ENV_PASSWORD, PASSWORD))
            .ssl(factoryHelper.getOptions().get(SSL))
            .folder(factoryHelper.getOptions().get(FOLDER))
            .mode(scanMode)
            .connectionTimeout(factoryHelper.getOptions().get(CONNECTION_TIMEOUT))
            .idle(factoryHelper.getOptions().get(IDLE))
            .heartbeat(factoryHelper.getOptions().get(HEARTBEAT))
            .heartbeatInterval(factoryHelper.getOptions().get(HEARTBEAT_INTERVAL))
            .interval(factoryHelper.getOptions().get(INTERVAL))
            .deletions(factoryHelper.getOptions().get(DELETIONS))
            .build();

    return new ImapTableSource(connectorOptions, schema);
  }
}
