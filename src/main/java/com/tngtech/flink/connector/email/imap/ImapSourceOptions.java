package com.tngtech.flink.connector.email.imap;

import com.tngtech.flink.connector.email.common.ConnectorOptions;
import com.tngtech.flink.connector.email.common.Protocol;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;

import static com.tngtech.flink.connector.email.imap.ImapConfigOptions.*;

@PublicEvolving
@Data
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public class ImapSourceOptions extends ConnectorOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String folder;

    private final StartupMode mode;
    private final int batchSize;
    private final @Nullable Long offset;

    private final Duration connectionTimeout;
    private final Duration interval;
    private final Duration heartbeatInterval;

    // ---------------------------------------------------------------------------------------------

    public static ImapSourceOptions fromOptions(ReadableConfig options) {
        return ImapSourceOptions.builder()
            .host(options.get(HOST))
            .port(options.get(PORT))
            .user(options.get(USER))
            .password(options.get(PASSWORD))
            .protocol(options.get(SSL) ? Protocol.IMAPS : Protocol.IMAP)
            .folder(options.get(FOLDER))
            .mode(options.get(MODE))
            .batchSize(options.get(BATCH_SIZE))
            .offset(options.get(OFFSET))
            .connectionTimeout(options.get(CONNECTION_TIMEOUT))
            .interval(options.get(INTERVAL))
            .heartbeatInterval(options.get(HEARTBEAT_INTERVAL))
            .build();
    }

}
