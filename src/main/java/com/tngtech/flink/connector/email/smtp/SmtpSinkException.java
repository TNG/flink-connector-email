package com.tngtech.flink.connector.email.smtp;

import org.apache.flink.annotation.PublicEvolving;

@PublicEvolving
public final class SmtpSinkException extends RuntimeException {

    public SmtpSinkException(String s) {
        super(s);
    }

    public SmtpSinkException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public static SmtpSinkException propagate(Exception e) {
        throw new SmtpSinkException(e.getMessage(), e);
    }

}
