package com.rqlite.jdbc;

import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public class DateTimeHandler {
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern(
      "yyyy-MM-dd['T'][' '][HH:mm[:ss[.SSSSSSSSS][.SSS]['Z']][XXX]"
  );

  private static final DateTimeFormatter FLEXIBLE_FORMATTER = DateTimeFormatter.ofPattern(
      "[yyyy-MM-dd['T'][' ']][HH:mm[:ss[.SSSSSSSSS][.SSS]['Z']][XXX]"
  );
  public static java.sql.Date parseDate(String sqlString) throws SQLException {
    TemporalAccessor ta = FORMATTER.parseBest(sqlString, LocalDate::from, LocalDateTime::from, OffsetDateTime::from);
    if (ta instanceof LocalDate) {
      return Date.valueOf((LocalDate) ta);
    } else if (ta instanceof LocalDateTime) {
      return Date.valueOf(((LocalDateTime) ta).toLocalDate());
    } else if (ta instanceof OffsetDateTime) {
      return Date.valueOf(((OffsetDateTime) ta).toLocalDate());
    }
    throw new SQLException("Unsupported date time format: " + sqlString);
  }

  public static java.sql.Time parseTime(String sqlString) throws SQLException {
    TemporalAccessor ta = FLEXIBLE_FORMATTER.parseBest(sqlString, LocalTime::from, OffsetTime::from);
    if (ta instanceof LocalTime) {
      return java.sql.Time.valueOf((LocalTime) ta);
    } else if (ta instanceof OffsetTime) {
      return java.sql.Time.valueOf(((OffsetTime) ta).toLocalTime());
    }
    throw new SQLException("Unsupported date time format: " + sqlString);
  }

  public static java.sql.Timestamp parseTimestamp(String sqlString) throws SQLException {
    TemporalAccessor ta = FORMATTER.parseBest(sqlString, LocalDateTime::from, OffsetDateTime::from);
    if (ta instanceof LocalDateTime) {
      return java.sql.Timestamp.valueOf((LocalDateTime) ta);
    } else if (ta instanceof OffsetDateTime) {
      return java.sql.Timestamp.from(((OffsetDateTime) ta).toInstant());
    }
    throw new SQLException("Unsupported date time format: " + sqlString);
  }
}
