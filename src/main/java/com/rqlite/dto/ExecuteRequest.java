package com.rqlite.dto;

import java.util.ArrayList;
import java.util.List;

public class ExecuteRequest implements StatementRequest {
  private boolean timings;
  private boolean transaction;
  private List<Statement> statements;
  private boolean wait;
  private boolean noRewriteRandom;
  private long timeout;

  // TODO: do we care about redirect mode?

  private ExecuteRequest(Builder builder) {
    setTimings(builder.timings);
    setTransaction(builder.transaction);
    setStatements(builder.statements);
    setWait(builder.wait);
    setNoRewriteRandom(builder.noRewriteRandom);
    setTimeout(builder.timeout);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(ExecuteRequest copy) {
    Builder builder = new Builder();
    builder.timings = copy.getTimings();
    builder.transaction = copy.getTransaction();
    builder.statements = copy.getStatements();
    builder.wait = copy.getWait();
    builder.noRewriteRandom = copy.getNoRewriteRandom();
    builder.timeout = copy.getTimeout();
    return builder;
  }

  public boolean getTimings() {
    return timings;
  }

  public ExecuteRequest setTimings(boolean timings) {
    this.timings = timings;
    return this;
  }

  public boolean getTransaction() {
    return transaction;
  }

  public ExecuteRequest setTransaction(boolean transaction) {
    this.transaction = transaction;
    return this;
  }

  public List<Statement> getStatements() {
    return statements;
  }

  public ExecuteRequest setStatements(List<Statement> statements) {
    this.statements = statements;
    return this;
  }

  public boolean getWait() {
    return wait;
  }

  public ExecuteRequest setWait(boolean wait) {
    this.wait = wait;
    return this;
  }

  public boolean getNoRewriteRandom() {
    return noRewriteRandom;
  }

  public ExecuteRequest setNoRewriteRandom(boolean noRewriteRandom) {
    this.noRewriteRandom = noRewriteRandom;
    return this;
  }

  public long getTimeout() {
    return timeout;
  }

  public ExecuteRequest setTimeout(long timeout) {
    this.timeout = timeout;
    return this;
  }

  public static final class Builder {
    private boolean timings;
    private boolean transaction;
    private List<Statement> statements;
    private boolean wait;
    private boolean noRewriteRandom;
    private long timeout;

    private Builder() {
      statements = new ArrayList<>();
    }

    public Builder setTimings(boolean val) {
      timings = val;
      return this;
    }

    public Builder setTransaction(boolean val) {
      transaction = val;
      return this;
    }

    public Builder setStatements(List<Statement> val) {
      statements = val;
      return this;
    }

    public Builder setWait(boolean val) {
      wait = val;
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

    public ExecuteRequest build() {
      return new ExecuteRequest(this);
    }
  }
}
