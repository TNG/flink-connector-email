package com.github.airblader.imap.table;

public class ImapSourceException extends RuntimeException {
  public ImapSourceException(String s) {
    super(s);
  }

  public ImapSourceException(String s, Throwable throwable) {
    super(s, throwable);
  }
}
