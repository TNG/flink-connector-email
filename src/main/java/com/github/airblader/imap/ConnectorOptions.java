package com.github.airblader.imap;

import java.io.Serializable;
import java.time.Duration;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ConnectorOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String host;
  private final Integer port;
  private final String user;
  private final String password;
  private final boolean ssl;
  private final String folder;
  private final ScanMode mode;
  private final Duration connectionTimeout;
  private final boolean idle;
  private final boolean heartbeat;
  private final Duration heartbeatInterval;
  private final Duration interval;
  private final boolean deletions;
  private final AddressFormat addressFormat;
}
