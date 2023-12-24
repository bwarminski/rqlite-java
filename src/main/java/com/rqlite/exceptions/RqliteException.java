package com.rqlite.exceptions;

public class RqliteException extends Exception {
  public RqliteException() {
  }

  public RqliteException(String message) {
    super(message);
  }

  public RqliteException(String message, Throwable cause) {
    super(message, cause);
  }

  public RqliteException(Throwable cause) {
    super(cause);
  }

  public RqliteException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
