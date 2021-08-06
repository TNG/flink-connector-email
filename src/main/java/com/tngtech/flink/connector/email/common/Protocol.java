package com.tngtech.flink.connector.email.common;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.flink.annotation.Internal;

@Internal
@RequiredArgsConstructor
public enum Protocol {
    IMAP("imap", false),
    IMAPS("imaps", true),
    SMTP("smtp", false),
    SMTPS("smtps", true);

    @Getter
    private final String name;

    @Getter
    private final boolean ssl;
}
