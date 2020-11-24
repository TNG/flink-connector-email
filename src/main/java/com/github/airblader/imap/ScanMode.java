package com.github.airblader.imap;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum ScanMode {
  ALL("all"),
  LATEST("latest"),
  UID("uid");

  private final String value;

  public String getValue() {
    return value;
  }

  public static ScanMode from(String value) {
    if (value == null) {
      throw new IllegalArgumentException("Mode must not be null");
    }

    for (ScanMode scanMode : values()) {
      if (value.equals(scanMode.getValue())) {
        return scanMode;
      }
    }

    throw new IllegalArgumentException("Unknown mode: " + value);
  }

  public boolean isOneOf(ScanMode... scanModes) {
    for (ScanMode scanMode : scanModes) {
      if (this == scanMode) {
        return true;
      }
    }

    return false;
  }
}
