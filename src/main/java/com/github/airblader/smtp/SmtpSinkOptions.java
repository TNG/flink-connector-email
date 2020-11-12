package com.github.airblader.smtp;

import static com.github.airblader.ConfigUtils.getEffectiveProperty;

import java.io.Serializable;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SmtpSinkOptions implements Serializable {
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
}
