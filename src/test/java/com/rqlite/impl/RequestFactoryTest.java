package com.rqlite.impl;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.rqlite.Rqlite.ReadConsistencyLevel;
import com.rqlite.dto.ExecuteRequest;
import com.rqlite.dto.QueryRequest;
import com.rqlite.dto.Statement;

public class RequestFactoryTest {
    @Test
    public void testRequestFactoryQuery() throws IOException {
        RequestFactory factory = new RequestFactory("http", "localhost", 4001);
        QueryRequest.Builder builder = QueryRequest.newBuilder();
        RqliteHttpRequest request = factory.buildQueryRequest(builder.build());
        Assert.assertEquals("http://localhost:4001/db/query", request.getUrl());
        Assert.assertEquals("POST", request.getMethod());
        Assert.assertEquals("[]", request.getBody());

        builder.setTransaction(true);
        request = factory.buildQueryRequest(builder.build());
        Assert.assertEquals("http://localhost:4001/db/query?transaction=true", request.getUrl());

        builder.setTransaction(false);
        request = factory.buildQueryRequest(builder.build());
        Assert.assertEquals("http://localhost:4001/db/query", request.getUrl());

        builder.setLevel(ReadConsistencyLevel.STRONG);
        request = factory.buildQueryRequest(builder.build());
        Assert.assertEquals("http://localhost:4001/db/query?level=strong", request.getUrl());

        builder.setLevel(ReadConsistencyLevel.WEAK);
        request = factory.buildQueryRequest(builder.build());
        Assert.assertEquals("http://localhost:4001/db/query?level=weak", request.getUrl());

        builder.setLevel(ReadConsistencyLevel.NONE);
        request = factory.buildQueryRequest(builder.build());
        Assert.assertEquals("http://localhost:4001/db/query?level=none", request.getUrl());
    }

    @Test
    public void testRequestFactorQueryStatement() throws IOException {
        RequestFactory factory = new RequestFactory("http", "localhost", 4001);
        RqliteHttpRequest request = factory.buildQueryRequest(QueryRequest.newBuilder().setStatements(List.of(Statement.newBuilder().setSql("SELECT * FROM foo" ).build())).build());
        Assert.assertEquals("http://localhost:4001/db/query", request.getUrl());
        Assert.assertEquals("POST", request.getMethod());
        Assert.assertEquals("[\"SELECT * FROM foo\"]", request.getBody());
    }

    @Test
    public void testRequestFactorQueryStatementMulti() throws IOException {
        RequestFactory factory = new RequestFactory("http", "localhost", 4001);
        RqliteHttpRequest request = factory.buildQueryRequest(QueryRequest.newBuilder()
            .setStatements(List.of(
                Statement.newBuilder().setSql("SELECT * FROM foo").build(),
                Statement.newBuilder().setSql("SELECT * FROM bar").build()
            )).build());
        Assert.assertEquals("http://localhost:4001/db/query", request.getUrl());
        Assert.assertEquals("POST", request.getMethod());
        Assert.assertEquals("[\"SELECT * FROM foo\",\"SELECT * FROM bar\"]", request.getBody());
    }

    @Test
    public void testRequestFactorExecute() throws IOException {
        RequestFactory factory = new RequestFactory("http", "localhost", 4001);
        ExecuteRequest.Builder builder = ExecuteRequest.newBuilder();
        RqliteHttpRequest request = factory.buildExecuteRequest(builder.build(), false);
        Assert.assertEquals("http://localhost:4001/db/execute", request.getUrl());
        Assert.assertEquals("POST", request.getMethod());
        Assert.assertEquals("[]", request.getBody());

        builder.setTransaction(true);
        request = factory.buildExecuteRequest(builder.build(), false);
        Assert.assertEquals("http://localhost:4001/db/execute?transaction=true", request.getUrl());

        builder.setTransaction(false);
        request = factory.buildExecuteRequest(builder.build(), false);
        Assert.assertEquals("http://localhost:4001/db/execute", request.getUrl());
    }

    @Test
    public void testRequestFactorExecuteStatementMulti() throws IOException {
        RequestFactory factory = new RequestFactory("http", "localhost", 4001);
        RqliteHttpRequest request = factory.buildExecuteRequest(
            ExecuteRequest.newBuilder().setStatements(List.of(
                Statement.newBuilder().setSql("INSERT INTO foo(name) VALUES(1)").build(),
                Statement.newBuilder().setSql("INSERT INTO foo(name) VALUES(2)").build()
            )).build(), false);
        Assert.assertEquals("http://localhost:4001/db/execute", request.getUrl());
        Assert.assertEquals("POST", request.getMethod());
        Assert.assertEquals("[\"INSERT INTO foo(name) VALUES(1)\",\"INSERT INTO foo(name) VALUES(2)\"]",
                request.getBody());
    }

}
