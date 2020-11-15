package com.github.airblader.imap.table;

import jakarta.mail.MessagingException;

public class ImapSourceException extends RuntimeException {
  public ImapSourceException(String s) {
    super(s);
  }

  public ImapSourceException(String s, Throwable throwable) {
    super(s, throwable);
  }

  public static ImapSourceException propagate(MessagingException e) {
    throw new ImapSourceException(e.getMessage(), e);
  }
}
