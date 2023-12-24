package com.rqlite.dto;

import java.util.ArrayList;
import java.util.List;

import com.rqlite.Rqlite.ReadConsistencyLevel;

public class QueryRequest implements StatementRequest {
  private boolean timings;
  private ReadConsistencyLevel level;
  private long freshness;
  private List<Statement> statements;
  private boolean transaction;
  private boolean noRewriteRandom;
  private long timeout;

  private QueryRequest(Builder builder) {
    setTimings(builder.timings);
    setLevel(builder.level);
    setFreshness(builder.freshness);
    setStatements(builder.statements);
    setTransaction(builder.transaction);
    setNoRewriteRandom(builder.noRewriteRandom);
    setTimeout(builder.timeout);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(QueryRequest copy) {
    Builder builder = new Builder();
    builder.timings = copy.getTimings();
    builder.level = copy.getLevel();
    builder.freshness = copy.getFreshness();
    builder.statements = copy.getStatements();
    builder.transaction = copy.getTransaction();
    builder.noRewriteRandom = copy.getNoRewriteRandom();
    builder.timeout = copy.getTimeout();
    return builder;
  }

  public boolean getTimings() {
    return timings;
  }

  public QueryRequest setTimings(boolean timings) {
    this.timings = timings;
    return this;
  }

  public ReadConsistencyLevel getLevel() {
    return level;
  }

  public QueryRequest setLevel(ReadConsistencyLevel level) {
    this.level = level;
    return this;
  }

  public long getFreshness() {
    return freshness;
  }

  public QueryRequest setFreshness(long freshness) {
    this.freshness = freshness;
    return this;
  }

  public List<Statement> getStatements() {
    return statements;
  }

  public QueryRequest setStatements(List<Statement> statements) {
    this.statements = statements;
    return this;
  }

  public boolean getTransaction() {
    return transaction;
  }

  public QueryRequest setTransaction(boolean transaction) {
    this.transaction = transaction;
    return this;
  }

  public boolean getNoRewriteRandom() {
    return noRewriteRandom;
  }

  public QueryRequest setNoRewriteRandom(boolean noRewriteRandom) {
    this.noRewriteRandom = noRewriteRandom;
    return this;
  }

  public long getTimeout() {
    return timeout;
  }

  public QueryRequest setTimeout(long timeout) {
    this.timeout = timeout;
    return this;
  }


  public static final class Builder {
    private boolean timings;
    private ReadConsistencyLevel level;
    private long freshness;
    private List<Statement> statements;
    private boolean transaction;
    private boolean noRewriteRandom;
    private long timeout;

    private Builder() {
      statements = new ArrayList<>();
    }

    public Builder setTimings(boolean val) {
      timings = val;
      return this;
    }

    public Builder setLevel(ReadConsistencyLevel val) {
      level = val;
      return this;
    }

    public Builder setFreshness(long val) {
      freshness = val;
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

    public QueryRequest build() {
      return new QueryRequest(this);
    }
  }
}
