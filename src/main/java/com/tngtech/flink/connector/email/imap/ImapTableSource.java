package com.tngtech.flink.connector.email.imap;

import com.tngtech.flink.connector.email.imap.ImapConfigOptions.StartupMode;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
class ImapTableSource implements ScanTableSource, SupportsReadingMetadata,
    SupportsProjectionPushDown {

    private DataType rowType;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final ImapSourceOptions options;
    private List<ReadableMetadata> metadataKeys = new ArrayList<>();

    public ImapTableSource(DataType rowType,
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
        ImapSourceOptions options) {

        this.rowType = checkNotNull(rowType);
        this.decodingFormat = decodingFormat;
        this.options = options;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        // If the produced row type is empty, no physical columns have been declared, and we don't
        // need to deserialize anything.
        final boolean readsContent = !rowType.getChildren().isEmpty();

        final DeserializationSchema<RowData> deserializer = readsContent
            ? decodingFormat.createRuntimeDecoder(context, rowType)
            : null;

        final ImapSource source = new ImapSource(deserializer, options, metadataKeys);
        final boolean bounded = options.getMode() == StartupMode.CURRENT;
        return SourceFunctionProvider.of(source, bounded);
    }

    @Override
    public DynamicTableSource copy() {
        final ImapTableSource source = new ImapTableSource(rowType, decodingFormat, options);
        source.metadataKeys = new ArrayList<>(metadataKeys);
        return source;
    }

    @Override
    public String asSummaryString() {
        return getClass().getSimpleName();
    }

    // ---------------------------------------------------------------------------------------------
    // Abilities
    // ---------------------------------------------------------------------------------------------


    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        rowType = DataTypeUtils.projectRow(rowType, projectedFields);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Arrays.stream(ReadableMetadata.values())
            .collect(Collectors.toMap(ReadableMetadata::getKey, ReadableMetadata::getType));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys.stream()
            .map(ReadableMetadata::ofKey)
            .collect(Collectors.toList());
    }
}
