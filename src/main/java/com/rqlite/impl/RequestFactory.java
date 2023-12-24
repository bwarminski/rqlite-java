package com.rqlite.impl;

import java.io.IOException;
import java.util.List;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.rqlite.dto.ExecuteRequest;
import com.rqlite.dto.Peer;

public class RequestFactory {
    static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    static final JsonFactory JSON_FACTORY = new JacksonFactory();

    private HttpRequestFactory requestFactory;

    private String proto;
    private String host;
    private Integer port;

    private GenericUrl executeUrl;
    private GenericUrl queryUrl;
    private GenericUrl statusUrl;

    private List<Peer> peers;

    public RequestFactory(final String proto, final String host, final Integer port) {
        this.proto = proto;
        this.host = host;
        this.port = port;
        this.peers = List.of(new Peer(host, port));

        this.executeUrl = new GenericUrl(String.format("%s://%s:%d/db/execute", this.proto, this.host, this.port));
        this.queryUrl = new GenericUrl(String.format("%s://%s:%d/db/query", this.proto, this.host, this.port));
        this.statusUrl = new GenericUrl(String.format("%s://%s:%d/status", this.proto, this.host, this.port));

        this.requestFactory = HTTP_TRANSPORT.createRequestFactory(new HttpRequestInitializer() {
            public void initialize(HttpRequest request) {
                request.setParser(new JsonObjectParser(JSON_FACTORY));
            }
        });
    }

    public RqliteHttpRequest buildExecuteRequest(ExecuteRequest request, boolean queue) throws IOException {
        GenericUrl url = this.executeUrl.clone();
        if (request.getTimings()) {
            url.set("timings","true");
        }
        if (request.getTransaction()) {
            url.set("transaction", "true");
        }
        if (queue) {
            url.set("queue","true");
        }
        if (request.getWait()) {
            url.set("wait", "true");
        }
        if (request.getNoRewriteRandom()) {
            url.set("norwrandom", "true");
        }
        if (request.getTimeout() > 0) {
            url.set("timeout", request.getTimeout());
        }
        HttpRequest httpRequest = this.requestFactory.buildPostRequest(url, new StatementRequestContent(request));

        return new RqliteHttpRequest(peers, httpRequest);
    }

    public RqliteHttpRequest buildQueryRequest(com.rqlite.dto.QueryRequest request) throws IOException {
        GenericUrl url = this.queryUrl.clone();
        if (request.getTimings()) {
            url.set("timings","true");
        }
        if (request.getTransaction()) {
            url.set("transaction", "true");
        }
        if (request.getNoRewriteRandom()) {
            url.set("norwrandom", "true");
        }
        if (request.getTimeout() > 0) {
            url.set("timeout", request.getTimeout());
        }
        if (request.getLevel() != null) {
            url.set("level", request.getLevel().toString().toLowerCase());
        }

        HttpRequest httpRequest = this.requestFactory.buildPostRequest(url, new StatementRequestContent(request));

        return new RqliteHttpRequest(peers, httpRequest);
    }

    public PingRequest buildPingRequest() throws IOException {
        HttpRequest request = this.requestFactory.buildGetRequest(this.statusUrl);
        return new PingRequest(request);
    }

    @Override
    public String toString() {
        return "RequestFactory{" +
                "requestFactory=" + requestFactory +
                ", proto='" + proto + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", executeUrl=" + executeUrl +
                ", queryUrl=" + queryUrl +
                ", statusUrl=" + statusUrl +
                '}';
    }
}
