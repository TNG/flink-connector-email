package com.github.airblader;

import lombok.var;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

public class ImapTableSourceFactory implements DynamicTableSourceFactory {
    private static final ConfigOption<String> HOST = ConfigOptions.key("host")
        .stringType()
        .noDefaultValue()
        .withDescription("Host for the IMAP server");
    private static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
        .intType()
        .noDefaultValue()
        .withDescription("Port for the IMAP server");
    private static final ConfigOption<String> USER = ConfigOptions.key("user")
        .stringType()
        .noDefaultValue()
        .withDescription("Username to authenticate with");
    private static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
        .stringType()
        .noDefaultValue()
        .withDescription("Password to authenticate with");
    private static final ConfigOption<Boolean> SSL = ConfigOptions.key("ssl")
        .booleanType()
        .defaultValue(true)
        .withDescription("Use SSL");
    private static final ConfigOption<String> FOLDER = ConfigOptions.key("folder")
        .stringType()
        .defaultValue("Inbox")
        .withDescription("Folder for which to list messages");
    private static final ConfigOption<String> MODE = ConfigOptions.key("scan.startup.mode")
        .stringType()
        .defaultValue("latest")
        .withDescription("'all' = Initially fetch all emails in the folder, 'latest' = Only observe changes in the folder");

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
        var connectorOptions = ImapConnectorOptions.builder()
            .host(factoryHelper.getOptions().get(HOST))
            .port(factoryHelper.getOptions().get(PORT))
            .user(factoryHelper.getOptions().get(USER))
            .password(factoryHelper.getOptions().get(PASSWORD))
            .ssl(factoryHelper.getOptions().get(SSL))
            .folder(factoryHelper.getOptions().get(FOLDER))
            .mode(scanMode)
            .build();

        return new ImapTableSource(schema, connectorOptions);
    }
}
