package com.tngtech.flink.connector.email.imap;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

import static com.tngtech.flink.connector.email.imap.ImapConfigOptions.*;

@Internal
public class ImapSourceFactory implements DynamicTableSourceFactory {
    private static final String IDENTIFIER = "imap";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST);
        options.add(USER);
        options.add(PASSWORD);
        options.add(SSL);
        options.add(FOLDER);
        options.add(MODE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FORMAT);

        options.add(PORT);
        options.add(CONNECTION_TIMEOUT);
        options.add(INTERVAL);
        options.add(HEARTBEAT_INTERVAL);
        options.add(BATCH_SIZE);
        options.add(OFFSET);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper
            factoryHelper = FactoryUtil.createTableFactoryHelper(this, context);

        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = factoryHelper
            .discoverDecodingFormat(DeserializationFormatFactory.class, FORMAT);

        factoryHelper.validate();

        final DataType rowType =
            context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        final ImapSourceOptions options = ImapSourceOptions.fromOptions(factoryHelper.getOptions());

        return new ImapTableSource(rowType, decodingFormat, options);
    }

}
