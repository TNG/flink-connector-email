package com.tngtech.flink.connector.email.imap;

import com.tngtech.flink.connector.email.common.ConnectorOptions;
import com.tngtech.flink.connector.email.common.Protocol;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.tngtech.flink.connector.email.imap.ImapConfigOptions.*;

@PublicEvolving
@Data
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public class ImapCatalogOptions extends ConnectorOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    // ---------------------------------------------------------------------------------------------

    public static ImapCatalogOptions fromOptions(ReadableConfig options) {
        return ImapCatalogOptions.builder()
            .host(options.get(HOST))
            .port(options.get(PORT))
            .user(options.get(USER))
            .password(options.get(PASSWORD))
            .protocol(options.get(SSL) ? Protocol.IMAPS : Protocol.IMAP)
            .build();
    }

    public Map<String, String> toOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put(HOST.key(), getHost());
        if (getPort() != null) {
            options.put(PORT.key(), String.valueOf(getPort()));
        }
        if (getUser() != null) {
            options.put(USER.key(), getUser());
        }
        if (getPassword() != null) {
            options.put(PASSWORD.key(), getPassword());
        }
        options.put(SSL.key(), String.valueOf(getProtocol() == Protocol.IMAPS));

        return options;
    }
}
