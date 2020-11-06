package com.github.airblader;

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
    var options = new HashSet<ConfigOption<?>>();
    options.add(HOST);

    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    var options = new HashSet<ConfigOption<?>>();
    options.add(PORT);
    options.add(USER);
    options.add(PASSWORD);
    options.add(SSL);
    options.add(FOLDER);
    options.add(MODE);
    options.add(IDLE);
    options.add(INTERVAL);
    options.add(DELETIONS);

    return options;
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    var factoryHelper = FactoryUtil.createTableFactoryHelper(this, context);

    factoryHelper.validate();
    ScanMode scanMode;
    try {
      scanMode = ScanMode.from(factoryHelper.getOptions().get(MODE));
    } catch (IllegalArgumentException e) {
      throw new ValidationException("Invalid value for " + MODE.key(), e);
    }

    var schema = context.getCatalogTable().getSchema();
    var connectorOptions =
        ConnectorOptions.builder()
            .host(factoryHelper.getOptions().get(HOST))
            .port(factoryHelper.getOptions().get(PORT))
            .user(factoryHelper.getOptions().get(USER))
            .password(factoryHelper.getOptions().get(PASSWORD))
            .ssl(factoryHelper.getOptions().get(SSL))
            .folder(factoryHelper.getOptions().get(FOLDER))
            .mode(scanMode)
            .idle(factoryHelper.getOptions().get(IDLE))
            .interval(factoryHelper.getOptions().get(INTERVAL))
            .deletions(factoryHelper.getOptions().get(DELETIONS))
            .build();

    return new ImapTableSource(schema, connectorOptions);
  }
}
