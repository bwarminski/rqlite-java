package com.rqlite.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class RqliteConnectionTest {

  @Test
  public void isValid() throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:rqlite://localhost:4001");
    assertThat(conn.isValid(0)).isTrue();
    conn.close();
    assertThat(conn.isValid(0)).isFalse();
  }

  @Test
  public void executeUpdateOnClosedDB() throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:rqlite://localhost:4001");
    Statement stat = conn.createStatement();
    conn.close();

    assertThatExceptionOfType(SQLException.class)
        .isThrownBy(() -> stat.executeUpdate("create table A(id, name)"));
  }

  @Test
  public void isClosed() throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:rqlite://localhost:4001");
    conn.close();
    assertThat(conn.isClosed()).isTrue();
  }

  @Test
  public void closeTest() throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:rqlite://localhost:4001");
    PreparedStatement prep = conn.prepareStatement("select null;");
    prep.executeQuery();
    conn.close();
    assertThatExceptionOfType(SQLException.class).isThrownBy(prep::clearParameters);
  }
}
