package com.rqlite.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.rqlite.dto.QueryResults;

public class RqliteResultSetMetaData implements ResultSetMetaData {
  /**
   * Pattern used to extract the precision and scale from column meta returned by the JDBC driver.
   */
  protected static final Pattern COLUMN_PRECISION = Pattern.compile(".*?\\((.*?)\\)");
  private final QueryResults.Result result;
  private final RqliteResultSet resultSet;

  public RqliteResultSetMetaData(RqliteResultSet resultSet, QueryResults.Result result) {
    this.resultSet = resultSet;
    this.result = result;
  }


  /**
   * Returns the number of columns in this {@code ResultSet} object.
   *
   * @return the number of columns
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getColumnCount() throws SQLException {
    return result.columns.length;
  }

  /**
   * Indicates whether the designated column is automatically numbered.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException(RqliteResultSet.SQL_FEATURE_NOT_SUPPORTED);
  }

  /**
   * Indicates whether a column's case matters.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return true;
  }

  /**
   * Indicates whether the designated column can be used in a where clause.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean isSearchable(int column) throws SQLException {
    return true;
  }

  /**
   * Indicates whether the designated column is a cash value.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  /**
   * Indicates the nullability of values in the designated column.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return the nullability status of the given column; one of {@code columnNoNulls},
   * {@code columnNullable} or {@code columnNullableUnknown}
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int isNullable(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException(RqliteResultSet.SQL_FEATURE_NOT_SUPPORTED);
  }

  /**
   * Indicates whether values in the designated column are signed numbers.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean isSigned(int column) throws SQLException {
    if (column < 1 || column > result.types.length) {
      throw new SQLException("Invalid Column Index: " + column);
    }

    String typeName = result.types[column-1];
    // TODO: Handle affinities
    return "NUMERIC".equalsIgnoreCase(typeName) || "INTEGER".equalsIgnoreCase(typeName) || "REAL".equalsIgnoreCase(typeName);
  }

  /**
   * Indicates the designated column's normal maximum width in characters.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return the normal maximum number of characters allowed as the width
   * of the designated column
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return Integer.MAX_VALUE;
  }

  /**
   * Gets the designated column's suggested title for use in printouts and
   * displays. The suggested title is usually specified by the SQL {@code AS}
   * clause.  If a SQL {@code AS} is not specified, the value returned from
   * {@code getColumnLabel} will be the same as the value returned by the
   * {@code getColumnName} method.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return the suggested column title
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getColumnLabel(int column) throws SQLException {
    if (column < 1 || column > result.types.length) {
      throw new SQLException("Invalid Column Index: " + column);
    }

    return result.columns[column-1];
  }

  /**
   * Get the designated column's name.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return column name
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getColumnName(int column) throws SQLException {
    return getColumnLabel(column); // TODO: This isn't exactly right
  }

  /**
   * Get the designated column's table's schema.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return schema name or "" if not applicable
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getSchemaName(int column) throws SQLException {
    return "";
  }

  /**
   * Get the designated column's specified column size.
   * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
   * For datetime datatypes, this is the length in characters of the String representation (assuming the
   * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
   * this is the length in bytes. 0 is returned for data types where the
   * column size is not applicable.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return precision
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getPrecision(int column) throws SQLException {
    if (column < 1 || column > result.types.length) {
      throw new SQLException("Invalid Column Index: " + column);
    }

    String declType = result.types[column-1];
    if (declType != null) {
      Matcher matcher = COLUMN_PRECISION.matcher(declType);

      return matcher.find() ? Integer.parseInt(matcher.group(1).split(",")[0].trim()) : 0;
    }

    return 0;
  }

  /**
   * Gets the designated column's number of digits to right of the decimal point.
   * 0 is returned for data types where the scale is not applicable.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return scale
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getScale(int column) throws SQLException {
    if (column < 1 || column > result.types.length) {
      throw new SQLException("Invalid Column Index: " + column);
    }

    String declType = result.types[column-1];

    if (declType != null) {
      Matcher matcher = COLUMN_PRECISION.matcher(declType);

      if (matcher.find()) {
        String[] array = matcher.group(1).split(",");

        if (array.length == 2) {
          return Integer.parseInt(array[1].trim());
        }
      }
    }

    return 0;
  }

  /**
   * Gets the designated column's table name.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return table name or "" if not applicable
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getTableName(int column) throws SQLException {
    return ""; // TODO: Not quite right
  }

  /**
   * Gets the designated column's table's catalog name.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return the name of the catalog for the table in which the given column
   * appears or "" if not applicable
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getCatalogName(int column) throws SQLException {
    return getTableName(column);
  }

  /**
   * Retrieves the designated column's SQL type.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return SQL type from java.sql.Types
   * @throws SQLException if a database access error occurs
   * @see Types
   */
  @Override
  public int getColumnType(int column) throws SQLException {
    if (column < 1 || column > result.types.length) {
      throw new SQLException("Invalid Column Index: " + column);
    }

    String typeName = result.types[column-1];

    if ("BOOLEAN".equalsIgnoreCase(typeName)) {
      return Types.BOOLEAN;
    }

    if ("TINYINT".equalsIgnoreCase(typeName)) {
      return Types.TINYINT;
    }

    if ("SMALLINT".equalsIgnoreCase(typeName) || "INT2".equalsIgnoreCase(typeName)) {
      return Types.SMALLINT;
    }

    if ("BIGINT".equalsIgnoreCase(typeName)
        || "INT8".equalsIgnoreCase(typeName)
        || "UNSIGNED BIG INT".equalsIgnoreCase(typeName)) {
      return Types.BIGINT;
    }

    if ("DATE".equalsIgnoreCase(typeName) || "DATETIME".equalsIgnoreCase(typeName)) {
      return Types.DATE;
    }

    if ("TIMESTAMP".equalsIgnoreCase(typeName)) {
      return Types.TIMESTAMP;
    }

    if ("INT".equalsIgnoreCase(typeName)
        || "INTEGER".equalsIgnoreCase(typeName)
        || "MEDIUMINT".equalsIgnoreCase(typeName)) {
      long val = resultSet.getLong(column); // TODO: This is what sqlite jdbc does but it requires that the resultset be advanced once
      if (val > Integer.MAX_VALUE || val < Integer.MIN_VALUE) {
        return Types.BIGINT;
      } else {
        return Types.INTEGER;
      }
    }

    Matcher matcher = COLUMN_PRECISION.matcher(typeName);

    if (matcher.find()) {
      String innerType = matcher.group(0);
      if ("DECIMAL".equalsIgnoreCase(innerType)) {
        return Types.DECIMAL;
      }
      if ("CHARACTER".equalsIgnoreCase(innerType)
          || "NCHAR".equalsIgnoreCase(innerType)
          || "NATIVE CHARACTER".equalsIgnoreCase(innerType)
          || "CHAR".equalsIgnoreCase(innerType)) {
        return Types.CHAR;
      }
      if ("VARCHAR".equalsIgnoreCase(innerType)
          || "VARYING CHARACTER".equalsIgnoreCase(innerType)
          || "NVARCHAR".equalsIgnoreCase(innerType)
          || "TEXT".equalsIgnoreCase(innerType)) {
        return Types.VARCHAR;
      }
    }
    if ("DECIMAL".equalsIgnoreCase(typeName)) {
      return Types.DECIMAL;
    }

    if ("DOUBLE".equalsIgnoreCase(typeName) || "DOUBLE PRECISION".equalsIgnoreCase(typeName)) {
      return Types.DOUBLE;
    }

    if ("NUMERIC".equalsIgnoreCase(typeName)) {
      return Types.NUMERIC;
    }

    if ("REAL".equalsIgnoreCase(typeName)) {
      return Types.REAL;
    }

    if ("FLOAT".equalsIgnoreCase(typeName)) {
      return Types.FLOAT;
    }

    if ("CHARACTER".equalsIgnoreCase(typeName)
        || "NCHAR".equalsIgnoreCase(typeName)
        || "NATIVE CHARACTER".equalsIgnoreCase(typeName)
        || "CHAR".equalsIgnoreCase(typeName)) {
      return Types.CHAR;
    }

    if ("CLOB".equalsIgnoreCase(typeName)) {
      return Types.CLOB;
    }

    if ("DATE".equalsIgnoreCase(typeName) || "DATETIME".equalsIgnoreCase(typeName)) {
      return Types.DATE;
    }

    if ("TIMESTAMP".equalsIgnoreCase(typeName)) {
      return Types.TIMESTAMP;
    }

    if ("VARCHAR".equalsIgnoreCase(typeName)
        || "VARYING CHARACTER".equalsIgnoreCase(typeName)
        || "NVARCHAR".equalsIgnoreCase(typeName)
        || "TEXT".equalsIgnoreCase(typeName)) {
      return Types.VARCHAR;
    }

    if ("BINARY".equalsIgnoreCase(typeName)) {
      return Types.BINARY;
    }

    if ("BLOB".equalsIgnoreCase(typeName)) {
      return Types.BLOB;
    }

    return Types.NUMERIC;
  }

  /**
   * Retrieves the designated column's database-specific type name.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return type name used by the database. If the column type is
   * a user-defined type, then a fully-qualified type name is returned.
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getColumnTypeName(int column) throws SQLException {
    if (column < 1 || column > result.types.length) {
      throw new SQLException("Invalid Column Index: " + column);
    }

    String typeName = result.types[column - 1];

    if (typeName.contains("int")) {
      return "INTEGER";
    } else if (typeName.contains("char") || typeName.contains("clob") || typeName.contains("text")) {
      return "TEXT";
    } else if (typeName.contains("blob") || typeName.isEmpty()) {
      return "BLOB";
    } else if (typeName.contains("real") || typeName.contains("floa") || typeName.contains("doub")) {
      return "REAL";
    } else {
      return "NUMERIC";
    }
  }


  /**
   * Indicates whether the designated column is definitely not writable.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return false;
  }

  /**
   * Indicates whether it is possible for a write on the designated column to succeed.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean isWritable(int column) throws SQLException {
    return true;
  }

  /**
   * Indicates whether a write on the designated column will definitely succeed.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return true; // This is marked as FIXME in the sqlite driver
  }

  /**
   * <p>Returns the fully-qualified name of the Java class whose instances
   * are manufactured if the method {@code ResultSet.getObject}
   * is called to retrieve a value
   * from the column.  {@code ResultSet.getObject} may return a subclass of the
   * class returned by this method.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return the fully-qualified name of the class in the Java programming
   * language that would be used by the method
   * {@code ResultSet.getObject} to retrieve the value in the specified
   * column. This is the class name used for custom mapping.
   * @throws SQLException if a database access error occurs
   * @since 1.2
   */
  @Override
  public String getColumnClassName(int column) throws SQLException {
    if (column < 1 || column > result.types.length) {
      throw new SQLException("Invalid Column Index: " + column);
    }
    Object value = resultSet.getObject(column);
    if (value == null) {
      return "java.lang.Object";
    }
    return value.getClass().getName();
  }

  /**
   * Returns an object that implements the given interface to allow access to
   * non-standard methods, or standard methods not exposed by the proxy.
   * <p>
   * If the receiver implements the interface then the result is the receiver
   * or a proxy for the receiver. If the receiver is a wrapper
   * and the wrapped object implements the interface then the result is the
   * wrapped object or a proxy for the wrapped object. Otherwise return the
   * the result of calling {@code unwrap} recursively on the wrapped object
   * or a proxy for that result. If the receiver is not a
   * wrapper and does not implement the interface, then an {@code SQLException} is thrown.
   *
   * @param iface A Class defining an interface that the result must implement.
   * @return an object that implements the interface. May be a proxy for the actual implementing object.
   * @throws SQLException If no object found that implements the interface
   * @since 1.6
   */
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return iface.cast(this);
  }

  /**
   * Returns true if this either implements the interface argument or is directly or indirectly a wrapper
   * for an object that does. Returns false otherwise. If this implements the interface then return true,
   * else if this is a wrapper then return the result of recursively calling {@code isWrapperFor} on the wrapped
   * object. If this does not implement the interface and is not a wrapper, return false.
   * This method should be implemented as a low-cost operation compared to {@code unwrap} so that
   * callers can use this method to avoid expensive {@code unwrap} calls that may fail. If this method
   * returns true then calling {@code unwrap} with the same argument should succeed.
   *
   * @param iface a Class defining an interface.
   * @return true if this implements the interface or directly or indirectly wraps an object that does.
   * @throws SQLException if an error occurs while determining whether this is a wrapper
   *                      for an object with the given interface.
   * @since 1.6
   */
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }
}
