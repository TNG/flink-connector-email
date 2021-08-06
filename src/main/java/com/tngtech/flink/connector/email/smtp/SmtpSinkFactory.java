package com.tngtech.flink.connector.email.smtp;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

import static com.tngtech.flink.connector.email.smtp.SmtpConfigOptions.*;

@Internal
public class SmtpSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "smtp";

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
        options.add(FORMAT);

        options.add(USER);
        options.add(PASSWORD);
        options.add(PORT);
        options.add(SSL);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper
            factoryHelper = FactoryUtil.createTableFactoryHelper(this, context);

        final EncodingFormat<SerializationSchema<RowData>> encodingFormat =
            factoryHelper.discoverEncodingFormat(SerializationFormatFactory.class, FORMAT);

        factoryHelper.validate();

        final DataType rowType =
            context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        final SmtpSinkOptions options = SmtpSinkOptions.fromOptions(factoryHelper.getOptions());

        return new SmtpTableSink(rowType, encodingFormat, options);
    }
}
