package com.tngtech.flink.connector.email.common;

import lombok.Data;
import lombok.experimental.SuperBuilder;
import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;
import java.io.Serializable;

@Internal
@Data
@SuperBuilder(toBuilder = true)
public class ConnectorOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String host;
    private final @Nullable Long port;
    private final @Nullable String user;
    private final @Nullable String password;
    private final Protocol protocol;

    public boolean usesAuthentication() {
        return password != null;
    }

}
