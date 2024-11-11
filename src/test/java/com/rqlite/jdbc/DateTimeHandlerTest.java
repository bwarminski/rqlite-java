package com.rqlite.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.junit.Test;

public class DateTimeHandlerTest {

  @Test
  public void itWillPass() {
    assertThat(true).isTrue();
  }

  @Test
  public void parseDate() throws SQLException {
    String[] dateStrings = {
        "2006-01-02 15:04:05.999999999-07:00",
        "2006-01-02T15:04:05.999999999-07:00",
        "2006-01-02 15:04:05.999999999",
        "2006-01-02T15:04:05.999999999",
        "2006-01-02 15:04:05",
        "2006-01-02T15:04:05",
        "2006-01-02 15:04",
        "2006-01-02T15:04",
        "2006-01-02",
        "2006-01-02T15:04:05.077Z"
    };

    for (String dateString : dateStrings) {
      DateTimeHandler.parseDate(dateString);
    }
  }

  @Test
  public void parseTime() throws SQLException {
    String[] dateStrings = {
        "2006-01-02 15:04:05.999999999-07:00",
        "2006-01-02T15:04:05.999999999-07:00",
        "2006-01-02 15:04:05.999999999",
        "2006-01-02T15:04:05.999999999",
        "2006-01-02 15:04:05",
        "2006-01-02T15:04:05",
        "2006-01-02 15:04",
        "2006-01-02T15:04",
        "2006-01-02T15:04:05.077Z",
        "15:04",
        "15:04:05",
        "15:04:05-07:00",
        "15:04:05.999999999-07:00",
        "15:04:05.077Z"
    };

    for (String dateString : dateStrings) {
      DateTimeHandler.parseTime(dateString);
    }
  }

  @Test
  public void parseTimestamp() throws SQLException {
    String[] dateStrings = {
        "2006-01-02 15:04:05.999999999-07:00",
        "2006-01-02T15:04:05.999999999-07:00",
        "2006-01-02 15:04:05.999999999",
        "2006-01-02T15:04:05.999999999",
        "2006-01-02 15:04:05",
        "2006-01-02T15:04:05",
        "2006-01-02 15:04",
        "2006-01-02T15:04",
        "2006-01-02T15:04:05.077Z"
    };

    for (String dateString : dateStrings) {
      DateTimeHandler.parseTimestamp(dateString);
    }
  }

}
