package com.rqlite.impl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.api.client.http.HttpResponse;
import com.rqlite.Rqlite;
import com.rqlite.dto.ExecuteQueryRequest;
import com.rqlite.dto.ExecuteQueryRequestResults;
import com.rqlite.dto.ExecuteRequest;
import com.rqlite.dto.ExecuteResults;
import com.rqlite.dto.Pong;
import com.rqlite.dto.QueryRequest;
import com.rqlite.dto.QueryResults;
import com.rqlite.dto.Statement;
import com.rqlite.exceptions.RqliteException;

public class RqliteImpl implements Rqlite {
    private RequestFactory requestFactory;

    private List<RqliteNode> peers; // only initialized if evaluating a config file

    public RqliteImpl(final String proto, final String host, final Integer port) {
        this.requestFactory = new RequestFactory(proto, host, port);
    }

    public RqliteImpl(final String configPath) {
        loadPeersFromConfig(configPath);
        this.requestFactory = new RequestFactory(peers.get(0).proto, peers.get(0).host, peers.get(0).port);
    }

    private void loadPeersFromConfig(String configPath){
        this.peers = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(configPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                peers.add(new RqliteNode(values[0], values[1], Integer.valueOf(values[2])));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
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
    public QueryResults Query(Statement[] stmts, boolean tx, ReadConsistencyLevel lvl) throws RqliteException {
        QueryRequest request = QueryRequest.newBuilder()
            .setStatements(List.of(stmts))
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
    public QueryResults Query(Statement q, ReadConsistencyLevel lvl) throws RqliteException {
        return this.Query(new Statement[] { q }, false, lvl);
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
    public ExecuteResults Execute(Statement[] stmts, boolean tx) throws RqliteException {
        ExecuteRequest request = ExecuteRequest.newBuilder()
            .setTimings(true)
            .setTransaction(tx)
            .setStatements(List.of(stmts))
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

    @Override
    public ExecuteQueryRequestResults Request(ExecuteQueryRequest request) throws RqliteException {
        try {
            RqliteHttpRequest httpRequest = this.requestFactory.buildExecuteQueryRequest(request);
            HttpResponse response = httpRequest.execute();
            return response.parseAs(ExecuteQueryRequestResults.class);
        } catch (IOException e) {
            throw new RqliteException(e);
        }
    }

    @Override
    public ExecuteQueryRequestResults Request(String q) throws RqliteException {
        ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
            .setTimings(true)
            .setStatements(List.of(Statement.newBuilder().setSql(q).build()))
            .build();
        return Request(request);
    }

    @Override
    public ExecuteQueryRequestResults Request(Statement q) throws RqliteException {
        ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
            .setTimings(true)
            .setStatements(List.of(q))
            .build();
        return Request(request);
    }

    @Override
    public ExecuteQueryRequestResults Request(String[] q, boolean tx) throws RqliteException {
        ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
            .setTimings(true)
            .setTransaction(tx)
            .setStatements(Arrays.stream(q).map((s) -> Statement.newBuilder().setSql(s).build()).toList())
            .build();
        return Request(request);
    }

    @Override
    public ExecuteQueryRequestResults Request(Statement[] q, boolean tx) throws RqliteException {
        ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
            .setTimings(true)
            .setTransaction(tx)
            .setStatements(List.of(q))
            .build();
        return Request(request);
    }

    public ExecuteResults Execute(String s) throws RqliteException {
        return this.Execute(new String[] { s }, false);
    }

    @Override
    public ExecuteResults Execute(Statement q) throws RqliteException {
        return this.Execute(new Statement[]{ q }, false);
    }

    public Pong Ping() throws RqliteException {
        try {
            return this.requestFactory.buildPingRequest().execute();
        } catch (IOException e) {
            throw new RqliteException(e);
        }
    }
}
