package com.rqlite.exceptions;

import java.util.List;

public class MultiHttpException extends RqliteException {
  private final List<HttpException> exceptions;

  public MultiHttpException(List<HttpException> exceptions) {
    this.exceptions = exceptions;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("MultiHttpException{");
    if (exceptions != null && !exceptions.isEmpty()) {
      sb.append("exceptions=[");
      for (int i = 0; i < exceptions.size(); i++) {
        sb.append(exceptions.get(i).toString());
        if (i < exceptions.size() - 1) {
          sb.append(", ");
        }
      }
      sb.append("]");
    }
    sb.append('}');
    return sb.toString();
  }

}
