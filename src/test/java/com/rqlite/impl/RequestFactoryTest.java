package com.rqlite.impl;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.rqlite.Rqlite;
import com.rqlite.dto.ExecuteRequest;
import com.rqlite.dto.Statement;

public class RequestFactoryTest {
    @Test
    public void testRequestFactoryQuery() throws IOException {
        RequestFactory factory = new RequestFactory("http", "localhost", 4001);
        QueryRequest request = factory.buildQueryRequest(new String[] {});
        Assert.assertEquals("http://localhost:4001/db/query", request.getUrl());
        Assert.assertEquals("POST", request.getMethod());
        Assert.assertEquals("[]", request.getBody());

        request.enableTransaction(true);
        Assert.assertEquals("http://localhost:4001/db/query?transaction=true", request.getUrl());

        request.enableTransaction(false);
        Assert.assertEquals("http://localhost:4001/db/query", request.getUrl());

        request.setReadConsistencyLevel(Rqlite.ReadConsistencyLevel.STRONG);
        Assert.assertEquals("http://localhost:4001/db/query?level=strong", request.getUrl());

        request.setReadConsistencyLevel(Rqlite.ReadConsistencyLevel.WEAK);
        Assert.assertEquals("http://localhost:4001/db/query?level=weak", request.getUrl());

        request.setReadConsistencyLevel(Rqlite.ReadConsistencyLevel.NONE);
        Assert.assertEquals("http://localhost:4001/db/query?level=none", request.getUrl());
    }

    @Test
    public void testRequestFactorQueryStatement() throws IOException {
        RequestFactory factory = new RequestFactory("http", "localhost", 4001);
        QueryRequest request = factory.buildQueryRequest(new String[] { "SELECT * FROM foo" });
        Assert.assertEquals("http://localhost:4001/db/query", request.getUrl());
        Assert.assertEquals("POST", request.getMethod());
        Assert.assertEquals("[\"SELECT * FROM foo\"]", request.getBody());
    }

    @Test
    public void testRequestFactorQueryStatementMulti() throws IOException {
        RequestFactory factory = new RequestFactory("http", "localhost", 4001);
        QueryRequest request = factory.buildQueryRequest(new String[] { "SELECT * FROM foo", "SELECT * FROM bar" });
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
