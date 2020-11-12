package com.github.airblader.imap;

import static com.github.airblader.ConfigUtils.getEffectiveProperty;
import static org.apache.flink.util.TimeUtils.formatWithHighestUnit;

import com.github.airblader.ConnectorConfigOptions;
import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class ImapCatalogOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String envHost;
  private final String host;
  private final String envPort;
  private final Integer port;
  private final String envUser;
  private final String user;
  private final String envPassword;
  private final String password;
  private final boolean ssl;
  private final ScanMode mode;
  private final Duration connectionTimeout;
  private final boolean idle;
  private final boolean heartbeat;
  private final Duration heartbeatInterval;
  private final Duration interval;
  private final boolean deletions;
  private final AddressFormat addressFormat;

  public String getEffectiveHost() {
    return getEffectiveProperty(envHost, host);
  }

  public Integer getEffectivePort() {
    return getEffectiveProperty(envPort, port, Integer::parseInt);
  }

  public String getEffectiveUser() {
    return getEffectiveProperty(envUser, user);
  }

  public String getEffectivePassword() {
    return getEffectiveProperty(envPassword, password);
  }

  public Map<String, String> toProperties() {
    Map<String, String> properties = new HashMap<>();
    if (envHost != null) {
      properties.put(ConnectorConfigOptions.ENV_HOST.key(), envHost);
    }
    if (host != null) {
      properties.put(ConnectorConfigOptions.HOST.key(), host);
    }
    if (envPort != null) {
      properties.put(ConnectorConfigOptions.ENV_PORT.key(), envPort);
    }
    if (port != null) {
      properties.put(ConnectorConfigOptions.PORT.key(), port.toString());
    }
    if (envUser != null) {
      properties.put(ConnectorConfigOptions.ENV_USER.key(), envUser);
    }
    if (user != null) {
      properties.put(ConnectorConfigOptions.USER.key(), user);
    }
    if (envPassword != null) {
      properties.put(ConnectorConfigOptions.ENV_PASSWORD.key(), envPassword);
    }
    if (password != null) {
      properties.put(ConnectorConfigOptions.PASSWORD.key(), password);
    }
    properties.put(ConnectorConfigOptions.SSL.key(), String.valueOf(ssl));
    properties.put(ConnectorConfigOptions.MODE.key(), mode.getValue());
    properties.put(
        ConnectorConfigOptions.CONNECTION_TIMEOUT.key(), formatWithHighestUnit(connectionTimeout));
    properties.put(ConnectorConfigOptions.IDLE.key(), String.valueOf(idle));
    properties.put(ConnectorConfigOptions.HEARTBEAT.key(), String.valueOf(heartbeat));
    properties.put(
        ConnectorConfigOptions.HEARTBEAT_INTERVAL.key(), formatWithHighestUnit(heartbeatInterval));
    properties.put(ConnectorConfigOptions.INTERVAL.key(), formatWithHighestUnit(interval));
    properties.put(ConnectorConfigOptions.DELETIONS.key(), String.valueOf(deletions));
    properties.put(ConnectorConfigOptions.ADDRESS_FORMAT.key(), addressFormat.getValue());

    return properties;
  }
}
