package com.rqlite.exceptions;

import java.util.List;

public class MultiHttpException extends RqliteException {
  private final List<HttpException> exceptions;

  public MultiHttpException(List<HttpException> exceptions) {
    this.exceptions = exceptions;
  }
}
