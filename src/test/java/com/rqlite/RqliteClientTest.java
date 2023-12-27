package com.rqlite;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.rqlite.dto.ExecuteQueryRequestResults;
import com.rqlite.dto.ExecuteRequest;
import com.rqlite.dto.ExecuteResults;
import com.rqlite.dto.QueryResults;
import com.rqlite.dto.Statement;
import com.rqlite.dto.Statement.Parameter;
import com.rqlite.exceptions.NodeUnavailableException;

public class RqliteClientTest {

    public Rqlite rqlite;

    @Before
    public void setup(){
        rqlite = RqliteFactory.connect("http", "localhost", 4001);
    }

    @Test
    public void testRqliteClientSingle() throws NodeUnavailableException {
        ExecuteResults results = null;
        QueryResults rows = null;
        ExecuteQueryRequestResults requestResults = null;
        try {

            results = rqlite.Execute("CREATE TABLE foo (id integer not null primary key, name text)");
            Assert.assertNotNull(results);
            Assert.assertEquals(1, results.results.length);

            results = rqlite.Execute("INSERT INTO foo(name) VALUES(\"fiona\")");
            Assert.assertNotNull(results);
            Assert.assertEquals(1, results.results.length);
            Assert.assertEquals(1, results.results[0].lastInsertId);

            rows = rqlite.Query("SELECT * FROM foo", Rqlite.ReadConsistencyLevel.WEAK);
            Assert.assertNotNull(rows);
            Assert.assertEquals(1, rows.results.length);
            Assert.assertArrayEquals(new String[]{"id", "name"}, rows.results[0].columns);
            Assert.assertArrayEquals(new String[]{"integer", "text"}, rows.results[0].types);
            Assert.assertEquals(1, rows.results[0].values.length);
            Assert.assertArrayEquals(new Object[]{new BigDecimal(1), "fiona"}, rows.results[0].values[0]);

            results = rqlite.Execute("CREATE TABLE secret_agents (id integer not null primary key, name text, secret text)");
            Assert.assertNotNull(results);
            Assert.assertEquals(1, results.results.length);

            results = rqlite.Execute(new Statement("INSERT INTO secret_agents(id, name, secret) VALUES(?, ?, ?)",
                List.of(Parameter.unnamed(7), Parameter.unnamed("James Bond"), Parameter.unnamed("not-a-secret"))));
            Assert.assertNotNull(results);
            Assert.assertEquals(1, results.results.length);
            Assert.assertNull(results.results[0].error);
            Assert.assertEquals(7, results.results[0].lastInsertId);

            results = rqlite.Execute(ExecuteRequest.newBuilder().setStatements(List.of(Statement.newBuilder()
                .setSql("INSERT INTO secret_agents(id, name, secret) VALUES(:id, :name, :secret)")
                .setParameters(List.of(
                    Parameter.newBuilder().setName(Optional.of("id")).setValue(8).build(),
                    Parameter.newBuilder().setName(Optional.of("name")).setValue("bob").build(),
                    Parameter.newBuilder().setName(Optional.of("secret")).build()
                )).build())).build(), false);
            Assert.assertNotNull(results);
            Assert.assertEquals(1, results.results.length);
            Assert.assertNull(results.results[0].error);
            Assert.assertEquals(8, results.results[0].lastInsertId);

            requestResults = rqlite.Request("SELECT * FROM foo");
            Assert.assertNotNull(requestResults);
            Assert.assertEquals(1, requestResults.results.length);
            Assert.assertArrayEquals(new String[]{"id", "name"}, requestResults.results[0].columns);
            Assert.assertArrayEquals(new String[]{"integer", "text"}, requestResults.results[0].types);
            Assert.assertEquals(1, requestResults.results[0].values.length);
            Assert.assertArrayEquals(new Object[]{new BigDecimal(1), "fiona"}, requestResults.results[0].values[0]);
        } catch (NodeUnavailableException e) {
            Assert.fail("Failed because rqlite-java could not connect to the node.");
        } catch (com.rqlite.exceptions.RqliteException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRqliteClientMulti() {
        ExecuteResults results = null;
        QueryResults rows = null;
        ExecuteQueryRequestResults requestResults = null;

        try {
            results = rqlite.Execute("CREATE TABLE bar (id integer not null primary key, name text)");
            Assert.assertNotNull(results);
            Assert.assertEquals(1, results.results.length);

            String[] s = {"INSERT INTO bar(name) VALUES(\"fiona\")", "INSERT INTO bar(name) VALUES(\"declan\")"};
            results = rqlite.Execute(s, false);
            Assert.assertNotNull(results);
            Assert.assertEquals(2, results.results.length);
            Assert.assertEquals(1, results.results[0].lastInsertId);
            Assert.assertEquals(2, results.results[1].lastInsertId);

            String[] q = {"SELECT * FROM bar", "SELECT name FROM bar"};
            rows = rqlite.Query(q, false, Rqlite.ReadConsistencyLevel.WEAK);
            Assert.assertNotNull(rows);
            Assert.assertNotNull(rows);
            Assert.assertEquals(2, rows.results.length);

            String[] sq = {"INSERT INTO bar(name) VALUES(\"clifford\")", "INSERT INTO bar(name) VALUES(\"magoo\")", "SELECT * FROM bar", "SELECT name FROM bar"};
            requestResults = rqlite.Request(sq, true);
            Assert.assertNotNull(requestResults);
            Assert.assertEquals(4, requestResults.results.length);
            Assert.assertEquals(3, requestResults.results[0].lastInsertId);
            Assert.assertEquals(4, requestResults.results[1].lastInsertId);
            Assert.assertArrayEquals(new String[]{"id", "name"}, requestResults.results[2].columns);
            Assert.assertArrayEquals(new String[]{"integer", "text"}, requestResults.results[2].types);
            Assert.assertEquals(4, requestResults.results[2].values.length);
            Assert.assertArrayEquals(new Object[]{new BigDecimal(1), "fiona"}, requestResults.results[2].values[0]);
            Assert.assertArrayEquals(new Object[]{new BigDecimal(2), "declan"}, requestResults.results[2].values[1]);
            Assert.assertArrayEquals(new Object[]{new BigDecimal(3), "clifford"}, requestResults.results[2].values[2]);
            Assert.assertArrayEquals(new Object[]{new BigDecimal(4), "magoo"}, requestResults.results[2].values[3]);
            Assert.assertArrayEquals(new String[]{"name"}, requestResults.results[3].columns);
            Assert.assertArrayEquals(new String[]{"text"}, requestResults.results[3].types);
            Assert.assertEquals(4, requestResults.results[3].values.length);
            Assert.assertArrayEquals(new Object[]{"fiona"}, requestResults.results[3].values[0]);
            Assert.assertArrayEquals(new Object[]{"declan"}, requestResults.results[3].values[1]);
            Assert.assertArrayEquals(new Object[]{"clifford"}, requestResults.results[3].values[2]);
            Assert.assertArrayEquals(new Object[]{"magoo"}, requestResults.results[3].values[3]);

            // SELECT * FROM bar
            Assert.assertArrayEquals(new String[]{"id", "name"}, rows.results[0].columns);
            Assert.assertArrayEquals(new String[]{"integer", "text"}, rows.results[0].types);
            Assert.assertEquals(2, rows.results[0].values.length);
            Assert.assertArrayEquals(new Object[]{new BigDecimal(1), "fiona"}, rows.results[0].values[0]);
            Assert.assertArrayEquals(new Object[]{new BigDecimal(2), "declan"}, rows.results[0].values[1]);

            // SELECT name FROM bar
            Assert.assertArrayEquals(new String[]{"name"}, rows.results[1].columns);
            Assert.assertArrayEquals(new String[]{"text"}, rows.results[1].types);
            Assert.assertEquals(2, rows.results[1].values.length);
            Assert.assertArrayEquals(new Object[]{"fiona"}, rows.results[1].values[0]);
            Assert.assertArrayEquals(new Object[]{"declan"}, rows.results[1].values[1]);
        } catch (NodeUnavailableException e) {
            Assert.fail("Failed because rqlite-java could not connect to the node.");
        } catch (com.rqlite.exceptions.RqliteException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRqliteClientSyntax() {
        ExecuteResults results = null;
        QueryResults rows = null;
        try {
        results = rqlite.Execute("nonsense");
            Assert.assertNotNull(results);
            Assert.assertEquals(1, results.results.length);
            Assert.assertEquals(0, results.results[0].rowsAffected);
            Assert.assertEquals("near \"nonsense\": syntax error", results.results[0].error);

            rows = rqlite.Query("more nonsense", Rqlite.ReadConsistencyLevel.WEAK);
            Assert.assertNotNull(rows);
            Assert.assertEquals(1, rows.results.length);
            Assert.assertEquals("near \"more\": syntax error", rows.results[0].error);
        } catch (NodeUnavailableException e) {
            Assert.fail("Failed because rqlite-java could not connect to the node.");
        } catch (com.rqlite.exceptions.RqliteException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void after() throws Exception {
        Rqlite rqlite = RqliteFactory.connect("http", "localhost", 4001);
        rqlite.Execute("DROP TABLE foo");
        rqlite.Execute("DROP TABLE bar");
        rqlite.Execute("DROP TABLE secret_agents");
    }
}
