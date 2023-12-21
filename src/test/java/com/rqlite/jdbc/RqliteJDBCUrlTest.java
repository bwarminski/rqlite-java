package com.rqlite.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;

public class RqliteJDBCUrlTest {
  @Test
  public void testParseValidURL() throws SQLException {
    String url = "jdbc:rqlite://localhost:8086";
    RqliteJDBCUrl parsedUrl = RqliteJDBCUrl.parse(url, new Properties());

    assertEquals("http", parsedUrl.getProto());
    assertEquals("localhost", parsedUrl.getHost());
    assertEquals(8086, parsedUrl.getPort());
  }

  @Test(expected = SQLException.class)
  public void testParseMalformedURL() throws SQLException {
    String url = "jdbc:rqlite://localhost";
    RqliteJDBCUrl.parse(url, new Properties());
  }

  @Test(expected = SQLException.class)
  public void testParseMalformedPort() throws SQLException {
    String url = "jdbc:rqlite://localhost:123brett";
    RqliteJDBCUrl.parse(url, new Properties());
  }

  @Test
  public void testParseURLWithParameters() throws SQLException {
    String url = "jdbc:rqlite://localhost:8086?useSSL=true&test=3";
    RqliteJDBCUrl parsedUrl = RqliteJDBCUrl.parse(url, new Properties());

    assertEquals("https", parsedUrl.getProto());
    assertEquals("localhost", parsedUrl.getHost());
    assertEquals(8086, parsedUrl.getPort());
  }
}
