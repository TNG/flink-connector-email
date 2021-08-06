package com.tngtech.flink.connector.email.imap;

import com.sun.mail.imap.IMAPFolder;
import com.tngtech.flink.connector.email.common.SessionProperties;
import com.tngtech.flink.connector.email.imap.ImapConfigOptions.StartupMode;
import com.tngtech.flink.connector.email.imap.ReadableMetadata.Context;
import jakarta.mail.*;
import jakarta.mail.event.MessageCountAdapter;
import jakarta.mail.event.MessageCountEvent;
import lombok.RequiredArgsConstructor;
import org.apache.commons.io.IOUtils;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.UserCodeClassLoader;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

@PublicEvolving
@RequiredArgsConstructor
public class ImapSource extends RichSourceFunction<RowData> {

    private final @Nullable DeserializationSchema<RowData> contentDeserializer;
    private final ImapSourceOptions options;
    private final List<ReadableMetadata> metadataKeys;

    private transient volatile boolean running;

    private transient Store store;
    private transient IMAPFolder folder;
    private transient Heartbeat heartbeat;

    private volatile boolean supportsIdle = true;

    private FetchProfile fetchProfile;

    @Override
    public void open(Configuration parameters) throws Exception {
        fetchProfile = getFetchProfile();
        connect();

        if (contentDeserializer != null) {
            contentDeserializer.open(new DeserializationSchema.InitializationContext() {
                @Override
                public MetricGroup getMetricGroup() {
                    return getRuntimeContext().getMetricGroup();
                }

                @Override
                public UserCodeClassLoader getUserCodeClassLoader() {
                    return (UserCodeClassLoader) Thread.currentThread().getContextClassLoader();
                }
            });
        }
    }

    @Override
    public void close() {
        try {
            if (folder != null) {
                folder.close(false);
            }

            if (store != null) {
                store.close();
            }
        } catch (MessagingException ignored) {
        }
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        running = true;

        if (options.getMode() == StartupMode.ALL) {
            fetchExistingMessages(ctx);
        }

        folder.addMessageCountListener(new MessageCountAdapter() {
            @Override
            public void messagesAdded(MessageCountEvent event) {
                collectMessages(ctx, event.getMessages());
            }
        });

        enterWaitLoop();
    }

    @Override
    public void cancel() {
        running = false;
        stopIdleHeartbeat();
    }

    // ---------------------------------------------------------------------------------------------

    private void connect() throws ImapSourceException {
        final Session session = Session.getInstance(getImapProperties(options));

        try {
            store = session.getStore();
        } catch (NoSuchProviderException e) {
            throw ImapSourceException.propagate(e);
        }

        try {
            if (options.usesAuthentication()) {
                store.connect(options.getUser(), options.getPassword());
            } else {
                store.connect("", "");
            }
        } catch (MessagingException e) {
            throw ImapSourceException.propagate(e);
        }

        try {
            final Folder genericFolder = store.getFolder(options.getFolder());
            folder = (IMAPFolder) genericFolder;
        } catch (MessagingException e) {
            throw ImapSourceException.propagate(e);
        } catch (ClassCastException e) {
            throw new ImapSourceException(
                "Folder " + folder.getName() + " is not an " + IMAPFolder.class.getSimpleName(), e);
        }

        openFolder();

        final boolean folderExists;
        try {
            folderExists = folder.exists();
        } catch (MessagingException e) {
            throw ImapSourceException.propagate(e);
        }

        if (!folderExists) {
            throw new ImapSourceException("Folder " + folder.getName() + " does not exist.");
        }
    }

    private void openFolder() {
        try {
            if (!folder.isOpen()) {
                folder.open(Folder.READ_ONLY);
            }
        } catch (MessagingException e) {
            throw ImapSourceException.propagate(e);
        }
    }

    private FetchProfile getFetchProfile() {
        final FetchProfile fetchProfile = new FetchProfile();
        fetchProfile.add(FetchProfile.Item.ENVELOPE);

        if (contentDeserializer != null || metadataKeys.contains(ReadableMetadata.CONTENT_TYPE)) {
            fetchProfile.add(FetchProfile.Item.CONTENT_INFO);
        }

        if (metadataKeys.contains(ReadableMetadata.SIZE)) {
            fetchProfile.add(FetchProfile.Item.SIZE);
        }

        if (metadataKeys.contains(ReadableMetadata.SEEN)
            || metadataKeys.contains(ReadableMetadata.DRAFT)
            || metadataKeys.contains(ReadableMetadata.ANSWERED)) {
            fetchProfile.add(FetchProfile.Item.FLAGS);
        }

        return fetchProfile;
    }

    private void stopIdleHeartbeat() {
        if (heartbeat != null && heartbeat.isAlive()) {
            heartbeat.interrupt();
        }
    }

    // ---------------------------------------------------------------------------------------------

    private void enterWaitLoop() {
        heartbeat = new Heartbeat(folder, options.getHeartbeatInterval());
        heartbeat.setDaemon(true);
        heartbeat.start();

        long nextReadTimeMs = System.currentTimeMillis();
        while (running) {
            if (supportsIdle) {
                try {
                    folder.idle();
                } catch (MessagingException ignored) {
                    supportsIdle = false;
                    stopIdleHeartbeat();
                } catch (IllegalStateException e) {
                    openFolder();
                }
            } else {
                try {
                    // Trigger some IMAP request to force the server to send a notification
                    folder.getMessageCount();
                } catch (MessagingException e) {
                    throw ImapSourceException.propagate(e);
                }

                nextReadTimeMs += options.getInterval().toMillis();
                try {
                    Thread.sleep(Math.max(0, nextReadTimeMs - System.currentTimeMillis()));
                } catch (InterruptedException e) {
                    throw new ImapSourceException("Error while sleeping", e);
                }
            }
        }
    }

    private void fetchExistingMessages(SourceContext<RowData> ctx) throws MessagingException {
        int currentNum = 1;

        if (options.getOffset() != null) {
            final Message startMessage = folder.getMessageByUID(options.getOffset());
            currentNum = startMessage.getMessageNumber();
        }

        // We need to loop to ensure we're not missing any messages coming in while we're processing
        // these.
        // See https://eclipse-ee4j.github.io/mail/FAQ#addlistener.
        while (running) {
            final int numberOfMessages = folder.getMessageCount();
            if (currentNum > numberOfMessages) {
                break;
            }

            final int batchEnd =
                currentNum + Math.min(numberOfMessages - currentNum, options.getBatchSize());

            collectMessages(ctx, folder.getMessages(currentNum, batchEnd));
            currentNum = batchEnd + 1;
        }
    }

    private void collectMessages(SourceContext<RowData> ctx, Message[] messages) {
        try {
            folder.fetch(messages, fetchProfile);
        } catch (MessagingException e) {
            throw ImapSourceException.propagate(e);
        }

        synchronized (ctx.getCheckpointLock()) {
            for (Message message : messages) {
                try {
                    collectMessage(ctx, message);
                } catch (Exception e) {
                    throw ImapSourceException.propagate(e);
                }
            }
        }

        ctx.markAsTemporarilyIdle();
    }

    private void collectMessage(SourceContext<RowData> ctx, Message message) throws Exception {
        final Context converterContext = Context.of(folder, message);
        final GenericRowData metadataRow = new GenericRowData(metadataKeys.size());

        for (int i = 0; i < metadataKeys.size(); i++) {
            final ReadableMetadata metadata = metadataKeys.get(i);
            final ReadableMetadata.Converter converter = metadata.getConverter();
            final Object obj = toInternalType(metadata, converter.convert(converterContext));
            metadataRow.setField(i, obj);
        }

        final RowData outputRow;
        if (contentDeserializer != null) {
            final byte[] content = IOUtils.toByteArray(message.getInputStream());
            final RowData deserializedRow = contentDeserializer.deserialize(content);

            final RowData physicalRow = deserializedRow == null
                ? GenericRowData.of(RowKind.INSERT, null)
                : deserializedRow;

            outputRow = new JoinedRowData(physicalRow, metadataRow);
        } else {
            outputRow = metadataRow;
        }

        ctx.collect(outputRow);
    }

    private @Nullable Object toInternalType(ReadableMetadata metadata, @Nullable Object value) {
        if (value == null) {
            return null;
        }

        switch (metadata) {
            case SUBJECT:
            case CONTENT_TYPE:
            case FROM_FIRST:
            case TO_FIRST:
                return StringData.fromString((String) value);
            case SENT:
            case RECEIVED:
                return TimestampData.fromInstant((Instant) value);
            default:
                return value;
        }
    }

    // ---------------------------------------------------------------------------------------------

    private static Properties getImapProperties(ImapSourceOptions options) {
        final SessionProperties sessionProperties = new SessionProperties(options);

        sessionProperties.addProtocolProperty("connectiontimeout",
            String.valueOf(options.getConnectionTimeout().toMillis()));
        sessionProperties.addProtocolProperty("partialfetch", "false");
        sessionProperties.addProtocolProperty("peek", "true");

        return sessionProperties.getProperties();
    }
}
