package com.rqlite.jdbc;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.rqlite.Rqlite;
import com.rqlite.Rqlite.ReadConsistencyLevel;

public class RqliteJDBCUrl {
  public static final String URL_PATTERN = "^jdbc:rqlite://([^:/]+):(\\d+)(\\?.*)?$";
  public static final ReadConsistencyLevel DEFAULT_CONSISTENCY = ReadConsistencyLevel.WEAK;

  private final String proto;
  private final String host;
  private final int port;
  private final Rqlite.ReadConsistencyLevel level;

  public static RqliteJDBCUrl parse(String url, Properties info) throws SQLException {
    // jdbc:rqlite://host:port?
    // useSSL=true for https
    // Later on we can do fancy stuff with trust stores for mutual TLS and allow multiple hosts


    Pattern pattern = Pattern.compile(URL_PATTERN);
    Matcher matcher = pattern.matcher(url);

    if (!matcher.find()) {
      throw new SQLException("Malformed URL Exception");
    }

    String host = matcher.group(1);
    String portStr = matcher.group(2);
    int port;
    try {
      port = Integer.parseInt(portStr);
    } catch (NumberFormatException e) {
      throw new SQLException("Malformed URL Exception");
    }

    boolean useSSL = false;
    ReadConsistencyLevel level = DEFAULT_CONSISTENCY;
    String queryParams = matcher.group(3);
    if (queryParams != null) {
      Map<String, String> params = parseQueryParams(queryParams);
      String sslValue = info.getProperty("useSSL");
      if (sslValue == null) {
        sslValue = params.get("useSSL");
      }
      if ("true".equalsIgnoreCase(sslValue)) {
        useSSL = true;
      }

      String levelValue = info.getProperty("level");
      if (levelValue == null) {
        levelValue = params.get("level");
      }
      if (levelValue != null) {
        Optional<ReadConsistencyLevel> maybeLevel = ReadConsistencyLevel.fromValue(levelValue);
        if (maybeLevel.isEmpty()) {
          throw new SQLException("Unknown rqlite consistency level " + levelValue);
        }
        level = maybeLevel.get();
      }
    }

    return new RqliteJDBCUrl(useSSL ? "https" : "http", host, port, level);
  }

  private static Map<String, String> parseQueryParams(String queryParams) {
    Map<String, String> params = new HashMap<>();
    String[] pairs = queryParams.substring(1).split("&"); // Remove '?' and split
    for (String pair : pairs) {
      int idx = pair.indexOf("=");
      params.put(pair.substring(0, idx), pair.substring(idx + 1));
    }
    return params;
  }

  public RqliteJDBCUrl(String proto, String host, int port, ReadConsistencyLevel level) {
    this.proto = proto;
    this.host = host;
    this.port = port;
    this.level = level;
  }

  public String getProto() {
    return proto;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public ReadConsistencyLevel getLevel() {
    return level;
  }
}
