package com.github.airblader;

public enum ScanMode {
    ALL("all"), LATEST("latest");

    private final String value;

    ScanMode(String value) {
        this.value = value;
    }

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
}
