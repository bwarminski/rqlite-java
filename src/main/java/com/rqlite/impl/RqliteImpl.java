package com.rqlite.impl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.rqlite.Rqlite;
import com.rqlite.dto.ExecuteRequest;
import com.rqlite.dto.ExecuteResults;
import com.rqlite.dto.ParameterizedStatement;
import com.rqlite.dto.Pong;
import com.rqlite.dto.QueryRequest;
import com.rqlite.dto.QueryResults;
import com.rqlite.dto.Statement;
import com.rqlite.exceptions.RqliteException;

public class RqliteImpl implements Rqlite {

    static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    static final JsonFactory JSON_FACTORY = new JacksonFactory();

    private RequestFactory requestFactory;

    private List<RqliteNode> peers; // only initialized if evaluating a config file
    private int timeoutDelay = 8000;

    Map<RqliteNode, RequestFactory> nodeRequestFactoryMap = new HashMap<>();

    public RqliteImpl(final String proto, final String host, final Integer port) {
        this.requestFactory = new RequestFactory(proto, host, port);
    }

    public RqliteImpl(final String configPath) {
        loadPeersFromConfig(configPath);
        this.requestFactory = new RequestFactory(peers.get(0).proto, peers.get(0).host, peers.get(0).port);
    }

    public void setTimeoutDelay(int delay) {
        this.timeoutDelay = delay;
    }

    private void loadPeersFromConfig(String configPath){
        this.peers = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(configPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                peers.add(new RqliteNode(values[0], values[1], Integer.valueOf(values[2])));
            }
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public QueryResults Query(String[] stmts, boolean tx, ReadConsistencyLevel lvl) throws RqliteException {
        QueryRequest request = QueryRequest.newBuilder()
            .setStatements(Arrays.stream(stmts).map((s) -> Statement.newBuilder().setSql(s).build()).toList())
            .setTransaction(tx)
            .setLevel(lvl)
            .build();
        return Query(request);
    }
    @Override
    public QueryResults Query(ParameterizedStatement[] stmts, boolean tx, ReadConsistencyLevel lvl) throws RqliteException {
        QueryRequest request = QueryRequest.newBuilder()
            .setStatements(Arrays.stream(stmts)
                .map((s) -> Statement.newBuilder()
                    .setSql(s.query)
                    .setParameters(Arrays.stream(s.arguments)
                        .map((a) -> Statement.Parameter.newBuilder()
                            .setValue(a)
                            .build())
                        .toList())
                    .build())
                .toList())
            .setTransaction(tx)
            .setLevel(lvl)
            .build();
        return Query(request);
    }

    @Override
    public QueryResults Query(QueryRequest query) throws RqliteException {
        try {
            RqliteHttpRequest request = this.requestFactory.buildQueryRequest(query);
            HttpResponse response = request.execute();
            return response.parseAs(QueryResults.class);
        } catch (IOException e) {
            throw new RqliteException(e);
        }
    }

    public QueryResults Query(String s, ReadConsistencyLevel lvl) throws RqliteException {
        return this.Query(new String[] { s }, false, lvl);
    }

    @Override
    public QueryResults Query(ParameterizedStatement q, ReadConsistencyLevel lvl) throws RqliteException {
        return this.Query(new ParameterizedStatement[] { q }, false, lvl);
    }

    public ExecuteResults Execute(String[] stmts, boolean tx) throws RqliteException {
        ExecuteRequest request = ExecuteRequest.newBuilder()
            .setTimings(true)
            .setStatements(Arrays.stream(stmts)
                .map((s) -> Statement.newBuilder().setSql(s).build())
                .toList())
            .build();
        return Execute(request, false);
    }

    @Override
    public ExecuteResults Execute(ParameterizedStatement[] stmts, boolean tx) throws RqliteException {
        ExecuteRequest request = ExecuteRequest.newBuilder()
            .setTimings(true)
            .setTransaction(tx)
            .setStatements(Arrays.stream(stmts)
                .map((s) -> Statement.newBuilder()
                    .setSql(s.query)
                    .setParameters(Arrays.stream(s.arguments)
                        .map((a) -> Statement.Parameter.newBuilder()
                            .setValue(a)
                            .build())
                        .toList())
                    .build())
                .toList())
            .build();
        return Execute(request, false);
    }

    @Override
    public ExecuteResults Execute(ExecuteRequest execute, boolean queued) throws RqliteException {
        try {
            RqliteHttpRequest request = this.requestFactory.buildExecuteRequest(execute, queued);
            HttpResponse response = request.execute();
            return response.parseAs(ExecuteResults.class);
        } catch (IOException e) {
            throw new RqliteException(e);
        }
    }

    public ExecuteResults Execute(String s) throws RqliteException {
        return this.Execute(new String[] { s }, false);
    }

    @Override
    public ExecuteResults Execute(ParameterizedStatement q) throws RqliteException {
        return this.Execute(new ParameterizedStatement[]{ q }, false);
    }

    public Pong Ping() throws RqliteException {
        try {
            return this.requestFactory.buildPingRequest().execute();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }
}
