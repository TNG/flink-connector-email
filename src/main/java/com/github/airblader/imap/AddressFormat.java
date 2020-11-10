package com.github.airblader.imap;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum AddressFormat {
  DEFAULT("default"),
  SIMPLE("simple");

  private final String value;

  public String getValue() {
    return value;
  }

  public static AddressFormat from(String value) {
    if (value == null) {
      throw new IllegalArgumentException("AddressFormat must not be null");
    }

    for (AddressFormat addressFormat : values()) {
      if (value.equals(addressFormat.getValue())) {
        return addressFormat;
      }
    }

    throw new IllegalArgumentException("Unknown format: " + value);
  }
}
