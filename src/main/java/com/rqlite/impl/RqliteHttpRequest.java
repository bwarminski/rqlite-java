package com.rqlite.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.rqlite.dto.Peer;
import com.rqlite.exceptions.HttpException;
import com.rqlite.exceptions.MultiHttpException;
import com.rqlite.exceptions.RqliteException;

/**
 * Encapsulates the logic of executing a request, following redirects and optionally retrying against
 * peers
 */
public class RqliteHttpRequest {
  private final List<Peer> peers;
  protected final HttpRequest request;

  public RqliteHttpRequest(List<Peer> peers,  HttpRequest request) {
    this.peers = peers;
    this.request = request;
  }

  public HttpResponse execute() throws RqliteException {
    if (peers.isEmpty()) {
      throw new RqliteException("No peers available to query");
    }
    request.setThrowExceptionOnExecuteError(false); // TODO, move to factory

    List<HttpException> exceptions = new ArrayList<>();

    // TODO: Original implementation includes a deadline, loops and performs backoff
    for (Peer peer: peers) {
      try {
        GenericUrl url = request.getUrl();
        url.setHost(peer.host);
        url.setPort(peer.port);
        HttpResponse response = request.execute();
        if (!response.isSuccessStatusCode()) {
          String responseBody = response.parseAsString();
          exceptions.add(HttpException.forDatabaseError("Got response code: " + response.getStatusCode() + " message: " + responseBody));
          continue;
        }
        return response;
      } catch (IOException e) {
        exceptions.add(HttpException.forIOException(e));
      }
    }
    throw new MultiHttpException(exceptions);
  }

  public String getUrl() {
    return request.getUrl().build();
  }

  public String getMethod() {
    return request.getRequestMethod();
  }

  public String getBody() throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    this.request.getContent().writeTo(stream);
    return stream.toString();
  }
}
