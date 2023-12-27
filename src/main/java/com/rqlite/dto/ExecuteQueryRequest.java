package com.rqlite.dto;

import java.util.List;

public class ExecuteQueryRequest implements StatementRequest {
  private boolean timings;
  private List<Statement> statements;
  private boolean transaction;
  private boolean noRewriteRandom;
  private long timeout;

  private ExecuteQueryRequest(Builder builder) {
    setTimings(builder.timings);
    setStatements(builder.statements);
    setTransaction(builder.transaction);
    setNoRewriteRandom(builder.noRewriteRandom);
    setTimeout(builder.timeout);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(ExecuteQueryRequest copy) {
    Builder builder = new Builder();
    builder.timings = copy.getTimings();
    builder.statements = copy.getStatements();
    builder.transaction = copy.getTransaction();
    builder.noRewriteRandom = copy.getNoRewriteRandom();
    builder.timeout = copy.getTimeout();
    return builder;
  }

  public boolean getTimings() {
    return timings;
  }

  public ExecuteQueryRequest setTimings(boolean timings) {
    this.timings = timings;
    return this;
  }

  public List<Statement> getStatements() {
    return statements;
  }

  public ExecuteQueryRequest setStatements(List<Statement> statements) {
    this.statements = statements;
    return this;
  }

  public boolean getTransaction() {
    return transaction;
  }

  public ExecuteQueryRequest setTransaction(boolean transaction) {
    this.transaction = transaction;
    return this;
  }

  public boolean getNoRewriteRandom() {
    return noRewriteRandom;
  }

  public ExecuteQueryRequest setNoRewriteRandom(boolean noRewriteRandom) {
    this.noRewriteRandom = noRewriteRandom;
    return this;
  }

  public long getTimeout() {
    return timeout;
  }

  public ExecuteQueryRequest setTimeout(long timeout) {
    this.timeout = timeout;
    return this;
  }


  public static final class Builder {
    private boolean timings;
    private List<Statement> statements;
    private boolean transaction;
    private boolean noRewriteRandom;
    private long timeout;

    private Builder() {
    }

    public Builder setTimings(boolean val) {
      timings = val;
      return this;
    }

    public Builder setStatements(List<Statement> val) {
      statements = val;
      return this;
    }

    public Builder setTransaction(boolean val) {
      transaction = val;
      return this;
    }

    public Builder setNoRewriteRandom(boolean val) {
      noRewriteRandom = val;
      return this;
    }

    public Builder setTimeout(long val) {
      timeout = val;
      return this;
    }

    public ExecuteQueryRequest build() {
      return new ExecuteQueryRequest(this);
    }
  }
}
