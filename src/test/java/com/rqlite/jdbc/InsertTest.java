package com.rqlite.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Test;

import com.rqlite.Rqlite;
import com.rqlite.RqliteFactory;

public class InsertTest {

  @After
  public void after() throws Exception {
    Rqlite rqlite = RqliteFactory.connect("http", "localhost", 4001);
    rqlite.Execute("DROP TABLE data");
    rqlite.Execute("DROP TABLE ResourceTags");
  }

  static class BD {
    String fullId;
    String type;

    public BD(String fullId, String type) {
      this.fullId = fullId;
      this.type = type;
    }

    public String getFullId() {
      return fullId;
    }

    public void setFullId(String fullId) {
      this.fullId = fullId;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public static byte[] serializeBD(BD item) {
      return new byte[0];
    }
  }


  @Test
  public void insertAndQuery() throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:rqlite://localhost:4001");
    Statement st = conn.createStatement();
    st.executeUpdate(
        "CREATE TABLE IF NOT EXISTS data (fid VARCHAR(255) PRIMARY KEY, type VARCHAR(64), data BLOB);");
    st.executeUpdate(
        "CREATE TABLE IF NOT EXISTS ResourcesTags (bd_fid VARCHAR(255), name VARCHAR(64), version INTEGER);");
    st.close();

    // Object Serialization
    PreparedStatement statAddBD = conn.prepareStatement("INSERT OR REPLACE INTO data values (?, ?, ?)");
    PreparedStatement statDelRT = conn.prepareStatement("DELETE FROM ResourcesTags WHERE bd_fid = ?");
    PreparedStatement statAddRT = conn.prepareStatement("INSERT INTO ResourcesTags values (?, ?, ?)");

    for (int i = 0; i < 10; i++) {
      BD item = new BD(Integer.toHexString(i), Integer.toString(i));

      // SQLite database insertion
      statAddBD.setString(1, item.getFullId());
      statAddBD.setString(2, item.getType());
      statAddBD.setBytes(3, BD.serializeBD(item));
      statAddBD.execute();

      // Then, its resources tags
      statDelRT.setString(1, item.getFullId());
      statDelRT.execute();

      statAddRT.setString(1, item.getFullId());

      for (int j = 0; j < 2; j++) {
        statAddRT.setString(2, "1");
        statAddRT.setLong(3, 1L);
        statAddRT.execute();
      }
    }

    statAddBD.close();
    statDelRT.close();
    statAddRT.close();

    //
    PreparedStatement stat;
    Long result = 0L;
    String query = "SELECT COUNT(fid) FROM data";

    stat = conn.prepareStatement(query);
    ResultSet rs = stat.executeQuery();

    rs.next();
    result = rs.getLong(1);
    assertThat(result).isEqualTo(10);
    rs.close();
    stat.close();

  }
}
