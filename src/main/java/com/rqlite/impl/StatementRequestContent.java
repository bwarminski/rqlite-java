package com.rqlite.impl;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.api.client.http.AbstractHttpContent;
import com.rqlite.dto.Statement;
import com.rqlite.dto.Statement.Parameter;
import com.rqlite.dto.StatementRequest;

public class StatementRequestContent extends AbstractHttpContent {
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  private final StatementRequest request;
  public StatementRequestContent(StatementRequest request) {
    super("application/json");
    this.request = request;
  }
  @Override
  public void writeTo(OutputStream out) throws IOException {
    JsonGenerator json = JSON_FACTORY.createGenerator(out);
    json.writeStartArray();
    boolean anyParameters = request.getStatements().stream().anyMatch((s) -> !s.getParameters().isEmpty());
    if (anyParameters) {
      for (Statement s : request.getStatements()) {
        json.writeStartArray();
        json.writeString(s.getSql());
        if (s.getParameters().stream().anyMatch((p) -> p.getName().isPresent())) {
          json.writeStartObject();
          for (Parameter p : s.getParameters()) {
            if (p.getName().isEmpty()) {
              throw new IOException("Unable to serialize, one or more parameters have no name");
            }
            json.writeObjectField(p.getName().get(), p.getValue());
          }
          json.writeEndObject();
        } else {
          for (Parameter p: s.getParameters()) {
            json.writeObject(p.getValue());
          }
        }

        json.writeEndArray();
      }
    } else {
      for (Statement s : request.getStatements()) {
        json.writeString(s.getSql());
      }
    }

    json.writeEndArray();
    json.close();
  }
}
