package com.rqlite;

import java.math.BigDecimal;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.rqlite.dto.ExecuteResults;
import com.rqlite.dto.ParameterizedStatement;
import com.rqlite.dto.QueryResults;

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

            results = rqlite.Execute(new ParameterizedStatement("INSERT INTO secret_agents(id, name, secret) VALUES(?, ?, ?)", new Object[]{7, "James Bond", "not-a-secret"}));
            Assert.assertNotNull(results);
            Assert.assertEquals(1, results.results.length);
            Assert.assertNull(results.results[0].error);
            Assert.assertEquals(7, results.results[0].lastInsertId);
        } catch (NodeUnavailableException e) {
            Assert.fail("Failed because rqlite-java could not connect to the node.");
        }
    }

    @Test
    public void testRqliteClientMulti() {
        ExecuteResults results = null;
        QueryResults rows = null;

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
