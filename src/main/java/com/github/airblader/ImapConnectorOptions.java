package com.github.airblader;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class ImapConnectorOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String host;
    private final Integer port;
    private final String user;
    private final String password;
    private final boolean ssl;
    private final String folder;
    private final ScanMode mode;
}
