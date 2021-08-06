package com.tngtech.flink.connector.email.smtp;

import lombok.RequiredArgsConstructor;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Internal
@RequiredArgsConstructor
class SmtpTableSink implements DynamicTableSink, SupportsWritingMetadata {

    private final DataType rowType;
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private final SmtpSinkOptions options;
    private final List<WritableMetadata> metadataKeys = new ArrayList<>();

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // If the produced row type is empty, no physical columns have been declared and we don't
        // need to serialize anything.
        final boolean writesContent = !rowType.getChildren().isEmpty();

        final SerializationSchema<RowData> serializer = writesContent
            ? encodingFormat.createRuntimeEncoder(context, rowType)
            : null;

        final SmtpSink sink = new SmtpSink(rowType, serializer, options, metadataKeys);
        return SinkFunctionProvider.of(sink);
    }

    @Override
    public DynamicTableSink copy() {
        final SmtpTableSink sink = new SmtpTableSink(rowType, encodingFormat, options);
        sink.metadataKeys.addAll(metadataKeys);
        return sink;
    }

    @Override
    public String asSummaryString() {
        return getClass().getSimpleName();
    }

    // ---------------------------------------------------------------------------------------------
    // Abilities
    // ---------------------------------------------------------------------------------------------

    @Override
    public Map<String, DataType> listWritableMetadata() {
        return Arrays.stream(WritableMetadata.values())
            .collect(Collectors.toMap(WritableMetadata::getKey, WritableMetadata::getType));
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        metadataKeys.stream()
            .map(WritableMetadata::ofKey)
            .forEach(this.metadataKeys::add);
    }
}
