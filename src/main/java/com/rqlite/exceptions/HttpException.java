package com.rqlite.exceptions;

import java.io.IOException;

public class HttpException extends RqliteException {
  private final IOException ioException;
  private final String databaseError;

  public HttpException(IOException ioException, String databaseError) {
    super();
    this.ioException = ioException;
    this.databaseError = databaseError;
  }

  public IOException getIoException() {
    return ioException;
  }

  public String getDatabaseError() {
    return databaseError;
  }

  public static HttpException forIOException(IOException e) {
    return new HttpException(e, null);
  }

  public static HttpException forDatabaseError(String databaseError) {
    return new HttpException(null, databaseError);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("HttpException{");
    if (ioException != null) {
      sb.append("ioException=").append(ioException.getMessage());
    }
    if (databaseError != null) {
      if (ioException != null) {sb.append(", ");}
      sb.append("databaseError='").append(databaseError).append('\'');
    }
    sb.append('}');
    return sb.toString();
  }

}
