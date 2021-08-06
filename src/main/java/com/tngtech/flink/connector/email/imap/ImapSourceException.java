package com.tngtech.flink.connector.email.imap;

import org.apache.flink.annotation.PublicEvolving;

@PublicEvolving
public final class ImapSourceException extends RuntimeException {

    public ImapSourceException(String s) {
        super(s);
    }

    public ImapSourceException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public static ImapSourceException propagate(Exception e) {
        throw new ImapSourceException(e.getMessage(), e);
    }
}
