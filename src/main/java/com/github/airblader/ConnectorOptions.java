package com.github.airblader;

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
  private final boolean idle;
  private final Duration interval;
}
