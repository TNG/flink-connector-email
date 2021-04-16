package com.github.airblader.imap.catalog;

import static com.github.airblader.ConfigUtils.getEffectiveProperty;
import static org.apache.flink.util.TimeUtils.formatWithHighestUnit;

import com.github.airblader.ConfigOptionsLibrary;
import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder(toBuilder = true)
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
  private final Duration connectionTimeout;

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
      properties.put(ConfigOptionsLibrary.ENV_HOST.key(), envHost);
    }
    if (host != null) {
      properties.put(ConfigOptionsLibrary.HOST.key(), host);
    }
    if (envPort != null) {
      properties.put(ConfigOptionsLibrary.ENV_PORT.key(), envPort);
    }
    if (port != null) {
      properties.put(ConfigOptionsLibrary.PORT.key(), port.toString());
    }
    if (envUser != null) {
      properties.put(ConfigOptionsLibrary.ENV_USER.key(), envUser);
    }
    if (user != null) {
      properties.put(ConfigOptionsLibrary.USER.key(), user);
    }
    if (envPassword != null) {
      properties.put(ConfigOptionsLibrary.ENV_PASSWORD.key(), envPassword);
    }
    if (password != null) {
      properties.put(ConfigOptionsLibrary.PASSWORD.key(), password);
    }
    properties.put(ConfigOptionsLibrary.SSL.key(), String.valueOf(ssl));
    if (connectionTimeout != null) {
      properties.put(
          ConfigOptionsLibrary.CONNECTION_TIMEOUT.key(), formatWithHighestUnit(connectionTimeout));
    }

    return properties;
  }
}
