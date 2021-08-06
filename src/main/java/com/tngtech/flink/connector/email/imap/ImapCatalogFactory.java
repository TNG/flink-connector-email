package com.tngtech.flink.connector.email.imap;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static com.tngtech.flink.connector.email.imap.ImapConfigOptions.*;

@Internal
public class ImapCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "imap";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USER);
        options.add(PASSWORD);
        options.add(PORT);
        options.add(SSL);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper factoryHelper =
            FactoryUtil.createCatalogFactoryHelper(this, context);
        factoryHelper.validate();

        ReadableConfig options = factoryHelper.getOptions();
        return new ImapCatalog(context.getName(), ImapCatalogOptions.fromOptions(options));
    }
}
