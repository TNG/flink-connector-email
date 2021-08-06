package com.tngtech.flink.connector.email.common;

import lombok.Getter;
import org.apache.flink.annotation.Internal;

import java.util.Properties;

@Internal
public class SessionProperties {

    private final Protocol protocol;

    @Getter
    private final Properties properties = new Properties();

    public SessionProperties(ConnectorOptions options) {
        this.protocol = options.getProtocol();

        addProperty("mail.store.protocol", protocol.getName());
        addProtocolProperty("auth", String.valueOf(options.usesAuthentication()));
        addProtocolProperty("host", options.getHost());

        if (options.getPort() != null) {
            addProtocolProperty("port", String.valueOf(options.getPort()));
        }

        if (options.getProtocol().isSsl()) {
            addProtocolProperty("ssl.enable", "true");
            addProtocolProperty("starttls.enable", "true");
        }
    }

    public void addProperty(String key, String value) {
        properties.put(key, value);
    }

    public void addProtocolProperty(String key, String value) {
        properties.put(String.format("mail.%s.%s", protocol.getName(), key), value);
    }
}
