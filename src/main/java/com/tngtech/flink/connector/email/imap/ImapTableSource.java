package com.tngtech.flink.connector.email.imap;

import lombok.RequiredArgsConstructor;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Internal
@RequiredArgsConstructor
class ImapTableSource implements ScanTableSource, SupportsReadingMetadata {

    private final DataType rowType;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final ImapSourceOptions options;
    private final List<ReadableMetadata> metadataKeys = new ArrayList<>();

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
        return SourceFunctionProvider.of(source, false);
    }

    @Override
    public DynamicTableSource copy() {
        final ImapTableSource source = new ImapTableSource(rowType, decodingFormat, options);
        source.metadataKeys.addAll(metadataKeys);
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
    public Map<String, DataType> listReadableMetadata() {
        return Arrays.stream(ReadableMetadata.values())
            .collect(Collectors.toMap(ReadableMetadata::getKey, ReadableMetadata::getType));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        metadataKeys.stream()
            .map(ReadableMetadata::ofKey)
            .forEach(this.metadataKeys::add);
    }
}
