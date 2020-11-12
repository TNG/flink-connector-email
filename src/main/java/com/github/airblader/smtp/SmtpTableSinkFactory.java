package com.github.airblader.smtp;

import static com.github.airblader.ConfigUtils.validateOptionOrEnv;
import static com.github.airblader.ConnectorConfigOptions.*;

import java.util.HashSet;
import java.util.Set;
import lombok.var;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

public class SmtpTableSinkFactory implements DynamicTableSinkFactory {
  @Override
  public String factoryIdentifier() {
    return "smtp";
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

    return options;
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    var factoryHelper = FactoryUtil.createTableFactoryHelper(this, context);

    factoryHelper.validate();
    validateOptionOrEnv(factoryHelper, HOST, ENV_HOST);

    var schema = context.getCatalogTable().getSchema();
    var connectorOptions =
        SmtpSinkOptions.builder()
            .host(factoryHelper.getOptions().get(HOST))
            .envHost(factoryHelper.getOptions().get(ENV_HOST))
            .port(factoryHelper.getOptions().get(PORT))
            .envPort(factoryHelper.getOptions().get(ENV_PORT))
            .user(factoryHelper.getOptions().get(USER))
            .envUser(factoryHelper.getOptions().get(ENV_USER))
            .password(factoryHelper.getOptions().get(PASSWORD))
            .envPassword(factoryHelper.getOptions().get(ENV_PASSWORD))
            .ssl(factoryHelper.getOptions().get(SSL))
            .build();

    return new SmtpTableSink(schema, connectorOptions);
  }
}
