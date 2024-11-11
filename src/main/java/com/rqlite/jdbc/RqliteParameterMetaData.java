package com.rqlite.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import com.rqlite.dto.Statement.Parameter;

public class RqliteParameterMetaData implements ParameterMetaData {
  private final List<Parameter> parameters;

  public RqliteParameterMetaData(List<Parameter> parameters) {
    this.parameters = parameters;
  }

  /**
   * Retrieves the number of parameters in the {@code PreparedStatement}
   * object for which this {@code ParameterMetaData} object contains
   * information.
   *
   * @return the number of parameters
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public int getParameterCount() throws SQLException {
    return parameters.size();
  }

  /**
   * Retrieves whether null values are allowed in the designated parameter.
   *
   * @param param the first parameter is 1, the second is 2, ...
   * @return the nullability status of the given parameter; one of
   * {@code ParameterMetaData.parameterNoNulls},
   * {@code ParameterMetaData.parameterNullable}, or
   * {@code ParameterMetaData.parameterNullableUnknown}
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public int isNullable(int param) throws SQLException {
    checkBounds(param);
    return ParameterMetaData.parameterNullableUnknown;
  }

  /**
   * Retrieves whether values for the designated parameter can be signed numbers.
   *
   * @param param the first parameter is 1, the second is 2, ...
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public boolean isSigned(int param) throws SQLException {
    checkBounds(param);
    return true;
  }

  /**
   * Retrieves the designated parameter's specified column size.
   *
   * <P>The returned value represents the maximum column size for the given parameter.
   * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
   * For datetime datatypes, this is the length in characters of the String representation (assuming the
   * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
   * this is the length in bytes. 0 is returned for data types where the
   * column size is not applicable.
   *
   * @param param the first parameter is 1, the second is 2, ...
   * @return precision
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public int getPrecision(int param) throws SQLException {
    checkBounds(param);
    return 0;
  }

  /**
   * Retrieves the designated parameter's number of digits to right of the decimal point.
   * 0 is returned for data types where the scale is not applicable.
   *
   * @param param the first parameter is 1, the second is 2, ...
   * @return scale
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public int getScale(int param) throws SQLException {
    checkBounds(param);
    return 0;
  }

  /**
   * Retrieves the designated parameter's SQL type.
   *
   * @param param the first parameter is 1, the second is 2, ...
   * @return SQL type from {@code java.sql.Types}
   * @throws SQLException if a database access error occurs
   * @see Types
   * @since 1.4
   */
  @Override
  public int getParameterType(int param) throws SQLException {
    checkBounds(param);
    Object p = parameters.get(param-1).getValue();
    if (p instanceof Boolean) {
      return Types.BOOLEAN;
    }
    if (p instanceof Number) {
      return Types.NUMERIC;
    }
    return Types.VARCHAR;
  }

  /**
   * Retrieves the designated parameter's database-specific type name.
   *
   * @param param the first parameter is 1, the second is 2, ...
   * @return type the name used by the database. If the parameter type is
   * a user-defined type, then a fully-qualified type name is returned.
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public String getParameterTypeName(int param) throws SQLException {
    checkBounds(param);
    return "TEXT";
  }

  /**
   * Retrieves the fully-qualified name of the Java class whose instances
   * should be passed to the method {@code PreparedStatement.setObject}.
   *
   * @param param the first parameter is 1, the second is 2, ...
   * @return the fully-qualified name of the class in the Java programming
   * language that would be used by the method
   * {@code PreparedStatement.setObject} to set the value
   * in the specified parameter. This is the class name used
   * for custom mapping.
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public String getParameterClassName(int param) throws SQLException {
    checkBounds(param);
    Object p = parameters.get(param-1).getValue();
    if (p == null) {
      return "java.lang.String";
    }
    return p.getClass().toString();
  }

  /**
   * Retrieves the designated parameter's mode.
   *
   * @param param the first parameter is 1, the second is 2, ...
   * @return mode of the parameter; one of
   * {@code ParameterMetaData.parameterModeIn},
   * {@code ParameterMetaData.parameterModeOut}, or
   * {@code ParameterMetaData.parameterModeInOut}
   * {@code ParameterMetaData.parameterModeUnknown}.
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public int getParameterMode(int param) throws SQLException {
    checkBounds(param);
    return RqliteParameterMetaData.parameterModeUnknown;
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

  private void checkBounds(int paramNumber) throws SQLException {
    if (paramNumber < 1) {
      throw new SQLException("Parameter index of '" + paramNumber + "' is invalid.");
    }

    if (paramNumber > this.parameters.size()) {
      throw new SQLException("Parameter index of '" + paramNumber
          + "' is greater than number of parameters, which is '" + this.parameters.size() + "'.");
    }
  }
}