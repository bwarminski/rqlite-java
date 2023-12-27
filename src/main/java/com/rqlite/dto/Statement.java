package com.rqlite.dto;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Statement {
  private String sql;
  private List<Parameter> parameters;

  public Statement(String sql, List<Parameter> parameters) {
    this.sql = sql;
    this.parameters = parameters;
  }

  private Statement(Builder builder) {
    setSql(builder.sql);
    setParameters(builder.parameters);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(Statement copy) {
    Builder builder = new Builder();
    builder.sql = copy.getSql();
    builder.parameters = copy.getParameters();
    return builder;
  }

  public String getSql() {
    return sql;
  }

  public Statement setSql(String sql) {
    this.sql = sql;
    return this;
  }

  public List<Parameter> getParameters() {
    return parameters;
  }

  public Statement setParameters(List<Parameter> parameters) {
    this.parameters = parameters;
    return this;
  }



  public static class Parameter {
    Optional<String> name;
    Object value;

    public static Parameter unnamed(Object value) {
      return new Parameter(Optional.empty(), value);
    }
    public Parameter(Optional<String> name, Object value) {
      this.name = name;
      this.value = value;
    }

    public Optional<String> getName() {
      return name;
    }

    public Parameter setName(Optional<String> name) {
      this.name = name;
      return this;
    }

    public Object getValue() {
      return value;
    }

    public Parameter setValue(Object value) {
      this.value = value;
      return this;
    }

    private Parameter(Builder builder) {
      name = builder.name;
      value = builder.value;
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public static Builder newBuilder(Parameter copy) {
      Builder builder = new Builder();
      builder.name = copy.getName();
      builder.value = copy.getValue();
      return builder;
    }


    public static final class Builder {
      private Optional<String> name;
      private Object value;

      private Builder() {
        name = Optional.empty();
      }

      public Builder setName(Optional<String> val) {
        name = val;
        return this;
      }

      public Builder setValue(Object val) {
        value = val;
        return this;
      }

      public Parameter build() {
        return new Parameter(this);
      }
    }
  }

  public static final class Builder {
    private String sql;
    private List<Parameter> parameters;

    private Builder() {
      parameters = new ArrayList<>();
      sql = "";
    }

    public Builder setSql(String val) {
      sql = val;
      return this;
    }

    public Builder setParameters(List<Parameter> val) {
      parameters = val;
      return this;
    }

    public Statement build() {
      return new Statement(this);
    }
  }
}
