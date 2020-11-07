package com.github.airblader;

import static com.github.airblader.ConnectorConfigOptions.*;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
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
    options.add(IDLE);
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
            .idle(factoryHelper.getOptions().get(IDLE))
            .interval(factoryHelper.getOptions().get(INTERVAL))
            .deletions(factoryHelper.getOptions().get(DELETIONS))
            .build();

    return new ImapTableSource(schema, connectorOptions);
  }

  private <T> void validateOptionOrEnv(
      FactoryUtil.TableFactoryHelper factoryHelper,
      ConfigOption<T> option,
      ConfigOption<String> envOption) {
    var value = factoryHelper.getOptions().get(option);
    var envValue = factoryHelper.getOptions().get(envOption);
    if (value == null && envValue == null) {
      throw new ValidationException(
          String.format("One of '%s' or '%s' must be set.", option.key(), envOption.key()));
    }
  }

  private String getValueWithEnvOverride(
      FactoryUtil.TableFactoryHelper factoryHelper,
      ConfigOption<String> envOption,
      ConfigOption<String> option) {
    return getValueWithEnvOverride(factoryHelper, envOption, option, Function.identity());
  }

  private <T> T getValueWithEnvOverride(
      FactoryUtil.TableFactoryHelper factoryHelper,
      ConfigOption<String> envOption,
      ConfigOption<T> option,
      Function<String, T> parserFn) {
    var envName = factoryHelper.getOptions().get(envOption);
    if (envName != null) {
      var envValue = System.getProperty(envName);
      if (envValue != null) {
        return parserFn.apply(envValue);
      }
    }

    return factoryHelper.getOptions().get(option);
  }
}
