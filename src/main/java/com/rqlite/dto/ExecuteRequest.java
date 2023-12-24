package com.rqlite.dto;

import java.util.ArrayList;
import java.util.List;

public class ExecuteRequest {
  private boolean timings;
  private boolean transaction;
  private List<Statement> statements;

  public ExecuteRequest(boolean timings, boolean transaction, List<Statement> statements) {
    this.timings = timings;
    this.transaction = transaction;
    this.statements = statements;
  }

  private ExecuteRequest(Builder builder) {
    setTimings(builder.timings);
    setTransaction(builder.transaction);
    setStatements(builder.statements);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(ExecuteRequest copy) {
    Builder builder = new Builder();
    builder.timings = copy.getTimings();
    builder.transaction = copy.getTransaction();
    builder.statements = copy.getStatements();
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


  public static final class Builder {
    private boolean timings;
    private boolean transaction;
    private List<Statement> statements;

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

    public ExecuteRequest build() {
      return new ExecuteRequest(this);
    }
  }
}
