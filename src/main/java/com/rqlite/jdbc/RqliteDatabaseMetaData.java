package com.rqlite.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.PseudoColumnUsage;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.rqlite.jdbc.RqliteDatabaseMetaData.ImportedKeyFinder.ForeignKey;

public class RqliteDatabaseMetaData implements DatabaseMetaData {
  private final RqliteConnection connection;
  private PreparedStatement getProcedures = null;
  private PreparedStatement getProcedureColumns = null;
  private PreparedStatement getSchemas = null;
  private PreparedStatement getCatalogs = null;
  private PreparedStatement getTableTypes = null;
  private PreparedStatement getColumnPrivileges = null;
  private PreparedStatement getTablePrivileges = null;
  private PreparedStatement getBestRowIdentifier = null;
  private PreparedStatement getVersionColumns = null;
  private PreparedStatement getTypeInfo = null;
  private PreparedStatement getUDTs = null;
  private PreparedStatement getSuperTypes = null;
  private PreparedStatement getSuperTables = null;
  private PreparedStatement getAttributes = null;

  private static final Pattern TYPE_INTEGER = Pattern.compile(".*(INT|BOOL).*");
  private static final Pattern TYPE_VARCHAR = Pattern.compile(".*(CHAR|CLOB|TEXT|BLOB).*");
  private static final Pattern TYPE_FLOAT = Pattern.compile(".*(REAL|FLOA|DOUB|DEC|NUM).*");

  public RqliteDatabaseMetaData(RqliteConnection connection) {
    this.connection = connection;
  }


  /**
   * Retrieves whether the current user can call all the procedures
   * returned by the method {@code getProcedures}.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether the current user can use all the tables returned
   * by the method {@code getTables} in a {@code SELECT}
   * statement.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return true;
  }

  /**
   * Retrieves the URL for this DBMS.
   *
   * @return the URL for this DBMS or {@code null} if it cannot be
   * generated
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getURL() throws SQLException {
    return connection.getUrl().toString();
  }

  /**
   * Retrieves the user name as known to this database.
   *
   * @return the database user name
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getUserName() throws SQLException {
    return null;
  }

  /**
   * Retrieves whether this database is in read-only mode.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether {@code NULL} values are sorted high.
   * Sorted high means that {@code NULL} values
   * sort higher than any other value in a domain.  In an ascending order,
   * if this method returns {@code true},  {@code NULL} values
   * will appear at the end. By contrast, the method
   * {@code nullsAreSortedAtEnd} indicates whether {@code NULL} values
   * are sorted at the end regardless of sort order.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    return true;
  }

  /**
   * Retrieves whether {@code NULL} values are sorted low.
   * Sorted low means that {@code NULL} values
   * sort lower than any other value in a domain.  In an ascending order,
   * if this method returns {@code true},  {@code NULL} values
   * will appear at the beginning. By contrast, the method
   * {@code nullsAreSortedAtStart} indicates whether {@code NULL} values
   * are sorted at the beginning regardless of sort order.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether {@code NULL} values are sorted at the start regardless
   * of sort order.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    return true;
  }

  /**
   * Retrieves whether {@code NULL} values are sorted at the end regardless of
   * sort order.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    return false;
  }

  /**
   * Retrieves the name of this database product.
   *
   * @return database product name
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getDatabaseProductName() throws SQLException {
    return "SQLite";
  }

  /**
   * Retrieves the version number of this database product.
   *
   * @return database version number
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return "0"; // TODO: We can get this with the client
  }

  /**
   * Retrieves the name of this JDBC driver.
   *
   * @return JDBC driver name
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getDriverName() throws SQLException {
    return "Rqlite JDBC";
  }

  /**
   * Retrieves the version number of this JDBC driver as a {@code String}.
   *
   * @return JDBC driver version
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getDriverVersion() throws SQLException {
    return "UNKNOWN";
  }

  /**
   * Retrieves this JDBC driver's major version number.
   *
   * @return JDBC driver major version
   */
  @Override
  public int getDriverMajorVersion() {
    return 0;
  }

  /**
   * Retrieves this JDBC driver's minor version number.
   *
   * @return JDBC driver minor version number
   */
  @Override
  public int getDriverMinorVersion() {
    return 0;
  }

  /**
   * Retrieves whether this database stores tables in a local file.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean usesLocalFiles() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database uses a file for each table.
   *
   * @return {@code true} if this database uses a local file for each table;
   * {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database treats mixed case unquoted SQL identifiers as
   * case sensitive and as a result stores them in mixed case.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return true;
  }

  /**
   * Retrieves whether this database treats mixed case unquoted SQL identifiers as
   * case insensitive and stores them in upper case.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database treats mixed case unquoted SQL identifiers as
   * case insensitive and stores them in lower case.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database treats mixed case unquoted SQL identifiers as
   * case insensitive and stores them in mixed case.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return true;
  }

  /**
   * Retrieves whether this database treats mixed case quoted SQL identifiers as
   * case sensitive and as a result stores them in mixed case.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database treats mixed case quoted SQL identifiers as
   * case insensitive and stores them in upper case.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database treats mixed case quoted SQL identifiers as
   * case insensitive and stores them in lower case.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database treats mixed case quoted SQL identifiers as
   * case insensitive and stores them in mixed case.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  /**
   * Retrieves the string used to quote SQL identifiers.
   * This method returns a space " " if identifier quoting is not supported.
   *
   * @return the quoting string or a space if quoting is not supported
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return "\"";
  }

  /**
   * Retrieves a comma-separated list of all of this database's SQL keywords
   * that are NOT also SQL:2003 keywords.
   *
   * @return the list of this database's keywords that are not also
   * SQL:2003 keywords
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getSQLKeywords() throws SQLException {
    return "ABORT,ACTION,AFTER,ANALYZE,ATTACH,AUTOINCREMENT,BEFORE,"
        + "CASCADE,CONFLICT,DATABASE,DEFERRABLE,DEFERRED,DESC,DETACH,"
        + "EXCLUSIVE,EXPLAIN,FAIL,GLOB,IGNORE,INDEX,INDEXED,INITIALLY,INSTEAD,ISNULL,"
        + "KEY,LIMIT,NOTNULL,OFFSET,PLAN,PRAGMA,QUERY,"
        + "RAISE,REGEXP,REINDEX,RENAME,REPLACE,RESTRICT,"
        + "TEMP,TEMPORARY,TRANSACTION,VACUUM,VIEW,VIRTUAL";
  }

  /**
   * Retrieves a comma-separated list of math functions available with
   * this database.  These are the Open /Open CLI math function names used in
   * the JDBC function escape clause.
   *
   * @return the list of math functions supported by this database
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getNumericFunctions() throws SQLException {
    return "";
  }

  /**
   * Retrieves a comma-separated list of string functions available with
   * this database.  These are the  Open Group CLI string function names used
   * in the JDBC function escape clause.
   *
   * @return the list of string functions supported by this database
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getStringFunctions() throws SQLException {
    return "";
  }

  /**
   * Retrieves a comma-separated list of system functions available with
   * this database.  These are the  Open Group CLI system function names used
   * in the JDBC function escape clause.
   *
   * @return a list of system functions supported by this database
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getSystemFunctions() throws SQLException {
    return "";
  }

  /**
   * Retrieves a comma-separated list of the time and date functions available
   * with this database.
   *
   * @return the list of time and date functions supported by this database
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getTimeDateFunctions() throws SQLException {
    return "DATE,TIME,DATETIME,JULIANDAY,STRFTIME";
  }

  /**
   * Retrieves the string that can be used to escape wildcard characters.
   * This is the string that can be used to escape '_' or '%' in
   * the catalog search parameters that are a pattern (and therefore use one
   * of the wildcard characters).
   *
   * <P>The '_' character represents any single character;
   * the '%' character represents any sequence of zero or
   * more characters.
   *
   * @return the string used to escape wildcard characters
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getSearchStringEscape() throws SQLException {
    return "\\";
  }

  /**
   * Retrieves all the "extra" characters that can be used in unquoted
   * identifier names (those beyond a-z, A-Z, 0-9 and _).
   *
   * @return the string containing the extra characters
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getExtraNameCharacters() throws SQLException {
    return "";
  }

  /**
   * Retrieves whether this database supports {@code ALTER TABLE}
   * with add column.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports {@code ALTER TABLE}
   * with drop column.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports column aliasing.
   *
   * <P>If so, the SQL AS clause can be used to provide names for
   * computed columns or to provide alias names for columns as
   * required.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  /**
   * Retrieves whether this database supports concatenations between
   * {@code NULL} and non-{@code NULL} values being
   * {@code NULL}.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return true;
  }

  /**
   * Retrieves whether this database supports the JDBC scalar function
   * {@code CONVERT} for the conversion of one JDBC type to another.
   * The JDBC types are the generic SQL data types defined
   * in {@code java.sql.Types}.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsConvert() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports the JDBC scalar function
   * {@code CONVERT} for conversions between the JDBC types <i>fromType</i>
   * and <i>toType</i>.  The JDBC types are the generic SQL data types defined
   * in {@code java.sql.Types}.
   *
   * @param fromType the type to convert from; one of the type codes from
   *                 the class {@code java.sql.Types}
   * @param toType   the type to convert to; one of the type codes from
   *                 the class {@code java.sql.Types}
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @see Types
   */
  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports table correlation names.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether, when table correlation names are supported, they
   * are restricted to being different from the names of the tables.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports expressions in
   * {@code ORDER BY} lists.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return true;
  }

  /**
   * Retrieves whether this database supports using a column that is
   * not in the {@code SELECT} statement in an
   * {@code ORDER BY} clause.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports some form of
   * {@code GROUP BY} clause.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  /**
   * Retrieves whether this database supports using a column that is
   * not in the {@code SELECT} statement in a
   * {@code GROUP BY} clause.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports using columns not included in
   * the {@code SELECT} statement in a {@code GROUP BY} clause
   * provided that all of the columns in the {@code SELECT} statement
   * are included in the {@code GROUP BY} clause.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports specifying a
   * {@code LIKE} escape clause.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports getting multiple
   * {@code ResultSet} objects from a single call to the
   * method {@code execute}.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database allows having multiple
   * transactions open at once (on different connections).
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether columns in this database may be defined as non-nullable.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return true;
  }

  /**
   * Retrieves whether this database supports the ODBC Minimum SQL grammar.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return true;
  }

  /**
   * Retrieves whether this database supports the ODBC Core SQL grammar.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return true;
  }

  /**
   * Retrieves whether this database supports the ODBC Extended SQL grammar.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports the ANSI92 entry level SQL
   * grammar.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports the ANSI92 intermediate SQL grammar supported.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports the ANSI92 full SQL grammar supported.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports the SQL Integrity
   * Enhancement Facility.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports some form of outer join.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  /**
   * Retrieves whether this database supports full nested outer joins.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return true; // TODO: Potentially false if version < 3.39
  }

  /**
   * Retrieves whether this database provides limited support for outer
   * joins.  (This will be {@code true} if the method
   * {@code supportsFullOuterJoins} returns {@code true}).
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return true;
  }

  /**
   * Retrieves the database vendor's preferred term for "schema".
   *
   * @return the vendor term for "schema"
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getSchemaTerm() throws SQLException {
    return "schema";
  }

  /**
   * Retrieves the database vendor's preferred term for "procedure".
   *
   * @return the vendor term for "procedure"
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getProcedureTerm() throws SQLException {
    return "not_implemented";
  }

  /**
   * Retrieves the database vendor's preferred term for "catalog".
   *
   * @return the vendor term for "catalog"
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getCatalogTerm() throws SQLException {
    return "catalog";
  }

  /**
   * Retrieves whether a catalog appears at the start of a fully qualified
   * table name.  If not, the catalog appears at the end.
   *
   * @return {@code true} if the catalog name appears at the beginning
   * of a fully qualified table name; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean isCatalogAtStart() throws SQLException {
    return true;
  }

  /**
   * Retrieves the {@code String} that this database uses as the
   * separator between a catalog and table name.
   *
   * @return the separator string
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getCatalogSeparator() throws SQLException {
    return ".";
  }

  /**
   * Retrieves whether a schema name can be used in a data manipulation statement.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether a schema name can be used in a procedure call statement.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether a schema name can be used in a table definition statement.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether a schema name can be used in an index definition statement.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether a schema name can be used in a privilege definition statement.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether a catalog name can be used in a data manipulation statement.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether a catalog name can be used in a procedure call statement.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether a catalog name can be used in a table definition statement.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether a catalog name can be used in an index definition statement.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether a catalog name can be used in a privilege definition statement.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports positioned {@code DELETE}
   * statements.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports positioned {@code UPDATE}
   * statements.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports {@code SELECT FOR UPDATE}
   * statements.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports stored procedure calls
   * that use the stored procedure escape syntax.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports subqueries in comparison
   * expressions.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports subqueries in
   * {@code EXISTS} expressions.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return true; // sqllite has this marked as TODO: check
  }

  /**
   * Retrieves whether this database supports subqueries in
   * {@code IN} expressions.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return true; // sqllite has this marked as TODO: check
  }

  /**
   * Retrieves whether this database supports subqueries in quantified
   * expressions.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports correlated subqueries.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports SQL {@code UNION}.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsUnion() throws SQLException {
    return true;
  }

  /**
   * Retrieves whether this database supports SQL {@code UNION ALL}.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsUnionAll() throws SQLException {
    return true;
  }

  /**
   * Retrieves whether this database supports keeping cursors open
   * across commits.
   *
   * @return {@code true} if cursors always remain open;
   * {@code false} if they might not remain open
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports keeping cursors open
   * across rollbacks.
   *
   * @return {@code true} if cursors always remain open;
   * {@code false} if they might not remain open
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports keeping statements open
   * across commits.
   *
   * @return {@code true} if statements always remain open;
   * {@code false} if they might not remain open
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports keeping statements open
   * across rollbacks.
   *
   * @return {@code true} if statements always remain open;
   * {@code false} if they might not remain open
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return false;
  }

  /**
   * Retrieves the maximum number of hex characters this database allows in an
   * inline binary literal.
   *
   * @return max the maximum length (in hex characters) for a binary literal;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of characters this database allows
   * for a character literal.
   *
   * @return the maximum number of characters allowed for a character literal;
   * a result of zero means that there is no limit or the limit is
   * not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of characters this database allows
   * for a column name.
   *
   * @return the maximum number of characters allowed for a column name;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of columns this database allows in a
   * {@code GROUP BY} clause.
   *
   * @return the maximum number of columns allowed;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of columns this database allows in an index.
   *
   * @return the maximum number of columns allowed;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of columns this database allows in an
   * {@code ORDER BY} clause.
   *
   * @return the maximum number of columns allowed;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of columns this database allows in a
   * {@code SELECT} list.
   *
   * @return the maximum number of columns allowed;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of columns this database allows in a table.
   *
   * @return the maximum number of columns allowed;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxColumnsInTable() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of concurrent connections to this
   * database that are possible.
   *
   * @return the maximum number of active connections possible at one time;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxConnections() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of characters that this database allows in a
   * cursor name.
   *
   * @return the maximum number of characters allowed in a cursor name;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of bytes this database allows for an
   * index, including all of the parts of the index.
   *
   * @return the maximum number of bytes allowed; this limit includes the
   * composite of all the constituent parts of the index;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxIndexLength() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of characters that this database allows in a
   * schema name.
   *
   * @return the maximum number of characters allowed in a schema name;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of characters that this database allows in a
   * procedure name.
   *
   * @return the maximum number of characters allowed in a procedure name;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of characters that this database allows in a
   * catalog name.
   *
   * @return the maximum number of characters allowed in a catalog name;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of bytes this database allows in
   * a single row.
   *
   * @return the maximum number of bytes allowed for a row; a result of
   * zero means that there is no limit or the limit is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxRowSize() throws SQLException {
    return 0;
  }

  /**
   * Retrieves whether the return value for the method
   * {@code getMaxRowSize} includes the SQL data types
   * {@code LONGVARCHAR} and {@code LONGVARBINARY}.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  /**
   * Retrieves the maximum number of characters this database allows in
   * an SQL statement.
   *
   * @return the maximum number of characters allowed for an SQL statement;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxStatementLength() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of active statements to this database
   * that can be open at the same time.
   *
   * @return the maximum number of statements that can be open at one time;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxStatements() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of characters this database allows in
   * a table name.
   *
   * @return the maximum number of characters allowed for a table name;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxTableNameLength() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of tables this database allows in a
   * {@code SELECT} statement.
   *
   * @return the maximum number of tables allowed in a {@code SELECT}
   * statement; a result of zero means that there is no limit or
   * the limit is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the maximum number of characters this database allows in
   * a user name.
   *
   * @return the maximum number of characters allowed for a user name;
   * a result of zero means that there is no limit or the limit
   * is not known
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getMaxUserNameLength() throws SQLException {
    return 0;
  }

  /**
   * Retrieves this database's default transaction isolation level.  The
   * possible values are defined in {@code java.sql.Connection}.
   *
   * @return the default isolation level
   * @throws SQLException if a database access error occurs
   * @see Connection
   */
  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  /**
   * Retrieves whether this database supports transactions. If not, invoking the
   * method {@code commit} is a noop, and the isolation level is
   * {@code TRANSACTION_NONE}.
   *
   * @return {@code true} if transactions are supported;
   * {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports the given transaction isolation level.
   *
   * @param level one of the transaction isolation levels defined in
   *              {@code java.sql.Connection}
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @see Connection
   */
  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    return level != Connection.TRANSACTION_NONE;
  }

  /**
   * Retrieves whether this database supports both data definition and
   * data manipulation statements within a transaction.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports only data manipulation
   * statements within a transaction.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether a data definition statement within a transaction forces
   * the transaction to commit.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database ignores a data definition statement
   * within a transaction.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  /**
   * Retrieves a description of the stored procedures available in the given
   * catalog.
   * <p>
   * Only procedure descriptions matching the schema and
   * procedure name criteria are returned.  They are ordered by
   * {@code PROCEDURE_CAT}, {@code PROCEDURE_SCHEM},
   * {@code PROCEDURE_NAME} and {@code SPECIFIC_ NAME}.
   *
   * <P>Each procedure description has the following columns:
   * <OL>
   * <LI><B>PROCEDURE_CAT</B> String {@code =>} procedure catalog (may be {@code null})
   * <LI><B>PROCEDURE_SCHEM</B> String {@code =>} procedure schema (may be {@code null})
   * <LI><B>PROCEDURE_NAME</B> String {@code =>} procedure name
   * <LI> reserved for future use
   * <LI> reserved for future use
   * <LI> reserved for future use
   * <LI><B>REMARKS</B> String {@code =>} explanatory comment on the procedure
   * <LI><B>PROCEDURE_TYPE</B> short {@code =>} kind of procedure:
   * <UL>
   * <LI> procedureResultUnknown - Cannot determine if  a return value
   * will be returned
   * <LI> procedureNoResult - Does not return a return value
   * <LI> procedureReturnsResult - Returns a return value
   * </UL>
   * <LI><B>SPECIFIC_NAME</B> String  {@code =>} The name which uniquely identifies this
   * procedure within its schema.
   * </OL>
   * <p>
   * A user may not have permissions to execute any of the procedures that are
   * returned by {@code getProcedures}
   *
   * @param catalog              a catalog name; must match the catalog name as it
   *                             is stored in the database; "" retrieves those without a catalog;
   *                             {@code null} means that the catalog name should not be used to narrow
   *                             the search
   * @param schemaPattern        a schema name pattern; must match the schema name
   *                             as it is stored in the database; "" retrieves those without a schema;
   *                             {@code null} means that the schema name should not be used to narrow
   *                             the search
   * @param procedureNamePattern a procedure name pattern; must match the
   *                             procedure name as it is stored in the database
   * @return {@code ResultSet} - each row is a procedure description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   */
  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
    if (getProcedures == null) {
      getProcedures =
          connection.prepareStatement(
              "select null as PROCEDURE_CAT, null as PROCEDURE_SCHEM, "
                  + "null as PROCEDURE_NAME, null as UNDEF1, null as UNDEF2, null as UNDEF3, "
                  + "null as REMARKS, null as PROCEDURE_TYPE limit 0;");
    }
    return getProcedures.executeQuery();
  }

  /**
   * Retrieves a description of the given catalog's stored procedure parameter
   * and result columns.
   *
   * <P>Only descriptions matching the schema, procedure and
   * parameter name criteria are returned.  They are ordered by
   * PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME and SPECIFIC_NAME. Within this, the return value,
   * if any, is first. Next are the parameter descriptions in call
   * order. The column descriptions follow in column number order.
   *
   * <P>Each row in the {@code ResultSet} is a parameter description or
   * column description with the following fields:
   * <OL>
   * <LI><B>PROCEDURE_CAT</B> String {@code =>} procedure catalog (may be {@code null})
   * <LI><B>PROCEDURE_SCHEM</B> String {@code =>} procedure schema (may be {@code null})
   * <LI><B>PROCEDURE_NAME</B> String {@code =>} procedure name
   * <LI><B>COLUMN_NAME</B> String {@code =>} column/parameter name
   * <LI><B>COLUMN_TYPE</B> Short {@code =>} kind of column/parameter:
   * <UL>
   * <LI> procedureColumnUnknown - nobody knows
   * <LI> procedureColumnIn - IN parameter
   * <LI> procedureColumnInOut - INOUT parameter
   * <LI> procedureColumnOut - OUT parameter
   * <LI> procedureColumnReturn - procedure return value
   * <LI> procedureColumnResult - result column in {@code ResultSet}
   * </UL>
   * <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
   * <LI><B>TYPE_NAME</B> String {@code =>} SQL type name, for a UDT type the
   * type name is fully qualified
   * <LI><B>PRECISION</B> int {@code =>} precision
   * <LI><B>LENGTH</B> int {@code =>} length in bytes of data
   * <LI><B>SCALE</B> short {@code =>} scale -  null is returned for data types where
   * SCALE is not applicable.
   * <LI><B>RADIX</B> short {@code =>} radix
   * <LI><B>NULLABLE</B> short {@code =>} can it contain NULL.
   * <UL>
   * <LI> procedureNoNulls - does not allow NULL values
   * <LI> procedureNullable - allows NULL values
   * <LI> procedureNullableUnknown - nullability unknown
   * </UL>
   * <LI><B>REMARKS</B> String {@code =>} comment describing parameter/column
   * <LI><B>COLUMN_DEF</B> String {@code =>} default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be {@code null})
   * <UL>
   * <LI> The string NULL (not enclosed in quotes) - if NULL was specified as the default value
   * <LI> TRUNCATE (not enclosed in quotes)        - if the specified default value cannot be represented without truncation
   * <LI> NULL                                     - if a default value was not specified
   * </UL>
   * <LI><B>SQL_DATA_TYPE</B> int  {@code =>} reserved for future use
   * <LI><B>SQL_DATETIME_SUB</B> int  {@code =>} reserved for future use
   * <LI><B>CHAR_OCTET_LENGTH</B> int  {@code =>} the maximum length of binary and character based columns.  For any other datatype the returned value is a
   * NULL
   * <LI><B>ORDINAL_POSITION</B> int  {@code =>} the ordinal position, starting from 1, for the input and output parameters for a procedure. A value of 0
   * is returned if this row describes the procedure's return value.  For result set columns, it is the
   * ordinal position of the column in the result set starting from 1.  If there are
   * multiple result sets, the column ordinal positions are implementation
   * defined.
   * <LI><B>IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine the nullability for a column.
   * <UL>
   * <LI> YES           --- if the column can include NULLs
   * <LI> NO            --- if the column cannot include NULLs
   * <LI> empty string  --- if the nullability for the
   * column is unknown
   * </UL>
   * <LI><B>SPECIFIC_NAME</B> String  {@code =>} the name which uniquely identifies this procedure within its schema.
   * </OL>
   *
   * <P><B>Note:</B> Some databases may not return the column
   * descriptions for a procedure.
   *
   * <p>The PRECISION column represents the specified column size for the given column.
   * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
   * For datetime datatypes, this is the length in characters of the String representation (assuming the
   * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
   * this is the length in bytes. Null is returned for data types where the
   * column size is not applicable.
   *
   * @param catalog              a catalog name; must match the catalog name as it
   *                             is stored in the database; "" retrieves those without a catalog;
   *                             {@code null} means that the catalog name should not be used to narrow
   *                             the search
   * @param schemaPattern        a schema name pattern; must match the schema name
   *                             as it is stored in the database; "" retrieves those without a schema;
   *                             {@code null} means that the schema name should not be used to narrow
   *                             the search
   * @param procedureNamePattern a procedure name pattern; must match the
   *                             procedure name as it is stored in the database
   * @param columnNamePattern    a column name pattern; must match the column name
   *                             as it is stored in the database
   * @return {@code ResultSet} - each row describes a stored procedure parameter or
   * column
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   */
  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
    if (getProcedureColumns == null) {
      getProcedureColumns =
          connection.prepareStatement(
              "select null as PROCEDURE_CAT, "
                  + "null as PROCEDURE_SCHEM, null as PROCEDURE_NAME, null as COLUMN_NAME, "
                  + "null as COLUMN_TYPE, null as DATA_TYPE, null as TYPE_NAME, null as PRECISION, "
                  + "null as LENGTH, null as SCALE, null as RADIX, null as NULLABLE, "
                  + "null as REMARKS limit 0;");
    }
    return getProcedureColumns.executeQuery();
  }

  /**
   * Retrieves a description of the tables available in the given catalog.
   * Only table descriptions matching the catalog, schema, table
   * name and type criteria are returned.  They are ordered by
   * {@code TABLE_TYPE}, {@code TABLE_CAT},
   * {@code TABLE_SCHEM} and {@code TABLE_NAME}.
   * <p>
   * Each table description has the following columns:
   * <OL>
   * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be {@code null})
   * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be {@code null})
   * <LI><B>TABLE_NAME</B> String {@code =>} table name
   * <LI><B>TABLE_TYPE</B> String {@code =>} table type.  Typical types are "TABLE",
   * "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
   * "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
   * <LI><B>REMARKS</B> String {@code =>} explanatory comment on the table (may be {@code null})
   * <LI><B>TYPE_CAT</B> String {@code =>} the types catalog (may be {@code null})
   * <LI><B>TYPE_SCHEM</B> String {@code =>} the types schema (may be {@code null})
   * <LI><B>TYPE_NAME</B> String {@code =>} type name (may be {@code null})
   * <LI><B>SELF_REFERENCING_COL_NAME</B> String {@code =>} name of the designated
   * "identifier" column of a typed table (may be {@code null})
   * <LI><B>REF_GENERATION</B> String {@code =>} specifies how values in
   * SELF_REFERENCING_COL_NAME are created. Values are
   * "SYSTEM", "USER", "DERIVED". (may be {@code null})
   * </OL>
   *
   * <P><B>Note:</B> Some databases may not return information for
   * all tables.
   *
   * @param catalog          a catalog name; must match the catalog name as it
   *                         is stored in the database; "" retrieves those without a catalog;
   *                         {@code null} means that the catalog name should not be used to narrow
   *                         the search
   * @param schemaPattern    a schema name pattern; must match the schema name
   *                         as it is stored in the database; "" retrieves those without a schema;
   *                         {@code null} means that the schema name should not be used to narrow
   *                         the search
   * @param tblNamePattern a table name pattern; must match the
   *                         table name as it is stored in the database
   * @param types            a list of table types, which must be from the list of table types
   *                         returned from {@link #getTableTypes},to include; {@code null} returns
   *                         all types
   * @return {@code ResultSet} - each row is a table description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   */
  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tblNamePattern, String[] types) throws SQLException {
    tblNamePattern =
        (tblNamePattern == null || "".equals(tblNamePattern))
            ? "%"
            : escape(tblNamePattern);

    StringBuilder sql = new StringBuilder();
    sql.append("SELECT").append("\n");
    sql.append("  NULL AS TABLE_CAT,").append("\n");
    sql.append("  NULL AS TABLE_SCHEM,").append("\n");
    sql.append("  NAME AS TABLE_NAME,").append("\n");
    sql.append("  TYPE AS TABLE_TYPE,").append("\n");
    sql.append("  NULL AS REMARKS,").append("\n");
    sql.append("  NULL AS TYPE_CAT,").append("\n");
    sql.append("  NULL AS TYPE_SCHEM,").append("\n");
    sql.append("  NULL AS TYPE_NAME,").append("\n");
    sql.append("  NULL AS SELF_REFERENCING_COL_NAME,").append("\n");
    sql.append("  NULL AS REF_GENERATION").append("\n");
    sql.append("FROM").append("\n");
    sql.append("  (").append("\n");
    sql.append("    SELECT\n");
    sql.append("      'sqlite_schema' AS NAME,\n");
    sql.append("      'SYSTEM TABLE' AS TYPE");
    sql.append("    UNION ALL").append("\n");
    sql.append("    SELECT").append("\n");
    sql.append("      NAME,").append("\n");
    sql.append("      UPPER(TYPE) AS TYPE").append("\n");
    sql.append("    FROM").append("\n");
    sql.append("      sqlite_schema").append("\n");
    sql.append("    WHERE").append("\n");
    sql.append("      NAME NOT LIKE 'sqlite\\_%' ESCAPE '\\'").append("\n");
    sql.append("      AND UPPER(TYPE) IN ('TABLE', 'VIEW')").append("\n");
    sql.append("    UNION ALL").append("\n");
    sql.append("    SELECT").append("\n");
    sql.append("      NAME,").append("\n");
    sql.append("      'GLOBAL TEMPORARY' AS TYPE").append("\n");
    sql.append("    FROM").append("\n");
    sql.append("      sqlite_temp_master").append("\n");
    sql.append("    UNION ALL").append("\n");
    sql.append("    SELECT").append("\n");
    sql.append("      NAME,").append("\n");
    sql.append("      'SYSTEM TABLE' AS TYPE").append("\n");
    sql.append("    FROM").append("\n");
    sql.append("      sqlite_schema").append("\n");
    sql.append("    WHERE").append("\n");
    sql.append("      NAME LIKE 'sqlite\\_%' ESCAPE '\\'").append("\n");
    sql.append("  )").append("\n");
    sql.append(" WHERE TABLE_NAME LIKE '");
    sql.append(tblNamePattern);
    sql.append("' ESCAPE '");
    sql.append(getSearchStringEscape());
    sql.append("'");

    if (types != null && types.length != 0) {
      sql.append(" AND TABLE_TYPE IN (");
      sql.append(
          Arrays.stream(types)
              .map((t) -> "'" + t.toUpperCase() + "'")
              .collect(Collectors.joining(",")));
      sql.append(")");
    }

    sql.append(" ORDER BY TABLE_TYPE, TABLE_NAME;");

    try (Statement statement = connection.createStatement()) {
      return statement.executeQuery(sql.toString());
    }
  }

  /**
   * Retrieves the schema names available in this database.  The results
   * are ordered by {@code TABLE_CATALOG} and
   * {@code TABLE_SCHEM}.
   *
   * <P>The schema columns are:
   * <OL>
   * <LI><B>TABLE_SCHEM</B> String {@code =>} schema name
   * <LI><B>TABLE_CATALOG</B> String {@code =>} catalog name (may be {@code null})
   * </OL>
   *
   * @return a {@code ResultSet} object in which each row is a
   * schema description
   * @throws SQLException if a database access error occurs
   */
  @Override
  public ResultSet getSchemas() throws SQLException {
    if (getSchemas == null) {
      getSchemas =
          connection.prepareStatement(
              "select null as TABLE_SCHEM, null as TABLE_CATALOG limit 0;");
    }

    return getSchemas.executeQuery();
  }

  /**
   * Retrieves the catalog names available in this database.  The results
   * are ordered by catalog name.
   *
   * <P>The catalog column is:
   * <OL>
   * <LI><B>TABLE_CAT</B> String {@code =>} catalog name
   * </OL>
   *
   * @return a {@code ResultSet} object in which each row has a
   * single {@code String} column that is a catalog name
   * @throws SQLException if a database access error occurs
   */
  @Override
  public ResultSet getCatalogs() throws SQLException {
    if (getCatalogs == null) {
      getCatalogs = connection.prepareStatement("select null as TABLE_CAT limit 0;");
    }

    return getCatalogs.executeQuery();
  }

  /**
   * Retrieves the table types available in this database.  The results
   * are ordered by table type.
   *
   * <P>The table type is:
   * <OL>
   * <LI><B>TABLE_TYPE</B> String {@code =>} table type.  Typical types are "TABLE",
   * "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
   * "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
   * </OL>
   *
   * @return a {@code ResultSet} object in which each row has a
   * single {@code String} column that is a table type
   * @throws SQLException if a database access error occurs
   */
  @Override
  public ResultSet getTableTypes() throws SQLException {
    String sql =
        "SELECT 'TABLE' AS TABLE_TYPE "
            + "UNION "
            + "SELECT 'VIEW' AS TABLE_TYPE "
            + "UNION "
            + "SELECT 'SYSTEM TABLE' AS TABLE_TYPE "
            + "UNION "
            + "SELECT 'GLOBAL TEMPORARY' AS TABLE_TYPE;";

    if (getTableTypes == null) {
      getTableTypes = connection.prepareStatement(sql);
    }
    getTableTypes.clearParameters();
    return getTableTypes.executeQuery();
  }

  /**
   * Retrieves a description of table columns available in
   * the specified catalog.
   *
   * <P>Only column descriptions matching the catalog, schema, table
   * and column name criteria are returned.  They are ordered by
   * {@code TABLE_CAT},{@code TABLE_SCHEM},
   * {@code TABLE_NAME}, and {@code ORDINAL_POSITION}.
   *
   * <P>Each column description has the following columns:
   * <OL>
   * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be {@code null})
   * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be {@code null})
   * <LI><B>TABLE_NAME</B> String {@code =>} table name
   * <LI><B>COLUMN_NAME</B> String {@code =>} column name
   * <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
   * <LI><B>TYPE_NAME</B> String {@code =>} Data source dependent type name,
   * for a UDT the type name is fully qualified
   * <LI><B>COLUMN_SIZE</B> int {@code =>} column size.
   * <LI><B>BUFFER_LENGTH</B> is not used.
   * <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned for data types where
   * DECIMAL_DIGITS is not applicable.
   * <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
   * <LI><B>NULLABLE</B> int {@code =>} is NULL allowed.
   * <UL>
   * <LI> columnNoNulls - might not allow {@code NULL} values
   * <LI> columnNullable - definitely allows {@code NULL} values
   * <LI> columnNullableUnknown - nullability unknown
   * </UL>
   * <LI><B>REMARKS</B> String {@code =>} comment describing column (may be {@code null})
   * <LI><B>COLUMN_DEF</B> String {@code =>} default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be {@code null})
   * <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
   * <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
   * <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the
   * maximum number of bytes in the column
   * <LI><B>ORDINAL_POSITION</B> int {@code =>} index of column in table
   * (starting at 1)
   * <LI><B>IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine the nullability for a column.
   * <UL>
   * <LI> YES           --- if the column can include NULLs
   * <LI> NO            --- if the column cannot include NULLs
   * <LI> empty string  --- if the nullability for the
   * column is unknown
   * </UL>
   * <LI><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the scope
   * of a reference attribute ({@code null} if DATA_TYPE isn't REF)
   * <LI><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the scope
   * of a reference attribute ({@code null} if the DATA_TYPE isn't REF)
   * <LI><B>SCOPE_TABLE</B> String {@code =>} table name that this the scope
   * of a reference attribute ({@code null} if the DATA_TYPE isn't REF)
   * <LI><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or user-generated
   * Ref type, SQL type from java.sql.Types ({@code null} if DATA_TYPE
   * isn't DISTINCT or user-generated REF)
   * <LI><B>IS_AUTOINCREMENT</B> String  {@code =>} Indicates whether this column is auto incremented
   * <UL>
   * <LI> YES           --- if the column is auto incremented
   * <LI> NO            --- if the column is not auto incremented
   * <LI> empty string  --- if it cannot be determined whether the column is auto incremented
   * </UL>
   * <LI><B>IS_GENERATEDCOLUMN</B> String  {@code =>} Indicates whether this is a generated column
   * <UL>
   * <LI> YES           --- if this a generated column
   * <LI> NO            --- if this not a generated column
   * <LI> empty string  --- if it cannot be determined whether this is a generated column
   * </UL>
   * </OL>
   *
   * <p>The COLUMN_SIZE column specifies the column size for the given column.
   * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
   * For datetime datatypes, this is the length in characters of the String representation (assuming the
   * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
   * this is the length in bytes. Null is returned for data types where the
   * column size is not applicable.
   *
   * @param c           a catalog name; must match the catalog name as it
   *                          is stored in the database; "" retrieves those without a catalog;
   *                          {@code null} means that the catalog name should not be used to narrow
   *                          the search
   * @param s     a schema name pattern; must match the schema name
   *                          as it is stored in the database; "" retrieves those without a schema;
   *                          {@code null} means that the schema name should not be used to narrow
   *                          the search
   * @param tblNamePattern  a table name pattern; must match the
   *                          table name as it is stored in the database
   * @param colNamePattern a column name pattern; must match the column
   *                          name as it is stored in the database
   * @return {@code ResultSet} - each row is a column description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   */
  @Override
  public ResultSet getColumns(String c, String s, String tblNamePattern, String colNamePattern) throws SQLException {
    // From SQLLite Driver
    StringBuilder sql = new StringBuilder(700);
    sql.append("select null as TABLE_CAT, null as TABLE_SCHEM, tblname as TABLE_NAME, ")
        .append(
            "cn as COLUMN_NAME, ct as DATA_TYPE, tn as TYPE_NAME, colSize as COLUMN_SIZE, ")
        .append(
            "2000000000 as BUFFER_LENGTH, colDecimalDigits as DECIMAL_DIGITS, 10   as NUM_PREC_RADIX, ")
        .append("colnullable as NULLABLE, null as REMARKS, colDefault as COLUMN_DEF, ")
        .append(
            "0    as SQL_DATA_TYPE, 0    as SQL_DATETIME_SUB, 2000000000 as CHAR_OCTET_LENGTH, ")
        .append(
            "ordpos as ORDINAL_POSITION, (case colnullable when 0 then 'NO' when 1 then 'YES' else '' end)")
        .append("    as IS_NULLABLE, null as SCOPE_CATALOG, null as SCOPE_SCHEMA, ")
        .append("null as SCOPE_TABLE, null as SOURCE_DATA_TYPE, ")
        .append(
            "(case colautoincrement when 0 then 'NO' when 1 then 'YES' else '' end) as IS_AUTOINCREMENT, ")
        .append(
            "(case colgenerated when 0 then 'NO' when 1 then 'YES' else '' end) as IS_GENERATEDCOLUMN from (");

    boolean colFound = false;

    ResultSet rs = null;
    try {
      // Get all tables implied by the input
      rs = getTables(c, s, tblNamePattern, null);
      while (rs.next()) {
        String tableName = rs.getString(3);

        boolean isAutoIncrement;

        Statement statColAutoinc = connection.createStatement();
        ResultSet rsColAutoinc = null;
        try {
          statColAutoinc = connection.createStatement();
          rsColAutoinc =
              statColAutoinc.executeQuery(
                  "SELECT LIKE('%autoincrement%', LOWER(sql)) FROM sqlite_schema "
                      + "WHERE LOWER(name) = LOWER('"
                      + escape(tableName)
                      + "') AND TYPE IN ('table', 'view')");
          rsColAutoinc.next();
          isAutoIncrement = rsColAutoinc.getInt(1) == 1;
        } finally {
          if (rsColAutoinc != null) {
              rsColAutoinc.close();
          }
          if (statColAutoinc != null) {
              statColAutoinc.close();
          }
        }

        // For each table, get the column info and build into overall SQL
        String pragmaStatement = "PRAGMA table_xinfo('" + escape(tableName) + "')";
        try (Statement colstat = connection.createStatement();
             ResultSet rscol = colstat.executeQuery(pragmaStatement)) {

          for (int i = 0; rscol.next(); i++) {
            String colName = rscol.getString(2);
            String colType = rscol.getString(3);
            String colNotNull = rscol.getString(4);
            String colDefault = rscol.getString(5);
            boolean isPk = "1".equals(rscol.getString(6));
            String colHidden = rscol.getString(7);

            int colNullable = 2;
            if (colNotNull != null) {
              colNullable = colNotNull.equals("0") ? 1 : 0;
            }

            if (colFound) {
              sql.append(" union all ");
            }
            colFound = true;

            // default values
            int iColumnSize = 2000000000;
            int iDecimalDigits = 10;

            /*
             * improved column types
             * ref https://www.sqlite.org/datatype3.html - 2.1 Determination Of Column Affinity
             * plus some degree of artistic-license applied
             */
            colType = colType == null ? "TEXT" : colType.toUpperCase();

            int colAutoIncrement = 0;
            if (isPk && isAutoIncrement) {
              colAutoIncrement = 1;
            }
            int colJavaType;
            // rule #1 + boolean
            if (TYPE_INTEGER.matcher(colType).find()) {
              colJavaType = Types.INTEGER;
              // there are no decimal digits
              iDecimalDigits = 0;
            } else if (TYPE_VARCHAR.matcher(colType).find()) {
              colJavaType = Types.VARCHAR;
              // there are no decimal digits
              iDecimalDigits = 0;
            } else if (TYPE_FLOAT.matcher(colType).find()) {
              colJavaType = Types.FLOAT;
            } else {
              // catch-all
              colJavaType = Types.VARCHAR;
            }
            // try to find an (optional) length/dimension of the column
            int iStartOfDimension = colType.indexOf('(');
            if (iStartOfDimension > 0) {
              // find end of dimension
              int iEndOfDimension = colType.indexOf(')', iStartOfDimension);
              if (iEndOfDimension > 0) {
                String sInteger, sDecimal;
                // check for two values (integer part, fraction) divided by
                // comma
                int iDimensionSeparator = colType.indexOf(',', iStartOfDimension);
                if (iDimensionSeparator > 0) {
                  sInteger =
                      colType.substring(
                          iStartOfDimension + 1, iDimensionSeparator);
                  sDecimal =
                      colType.substring(
                          iDimensionSeparator + 1, iEndOfDimension);
                }
                // only a single dimension
                else {
                  sInteger =
                      colType.substring(
                          iStartOfDimension + 1, iEndOfDimension);
                  sDecimal = null;
                }
                // try to parse the values
                try {
                  int iInteger = Integer.parseUnsignedInt(sInteger);
                  // parse decimals?
                  if (sDecimal != null) {
                    iDecimalDigits = Integer.parseUnsignedInt(sDecimal);
                    // columns size equals sum of integer and decimal part
                    // of dimension
                    iColumnSize = iInteger + iDecimalDigits;
                  } else {
                    // no decimals
                    iDecimalDigits = 0;
                    // columns size equals dimension
                    iColumnSize = iInteger;
                  }
                } catch (NumberFormatException ex) {
                  // just ignore invalid dimension formats here
                }
              }
              // "TYPE_NAME" (colType) is without the length/ dimension
              colType = colType.substring(0, iStartOfDimension).trim();
            }

            int colGenerated = "2".equals(colHidden) ? 1 : 0;

            sql.append("select ")
                .append(i + 1)
                .append(" as ordpos, ")
                .append(colNullable)
                .append(" as colnullable,")
                .append(colJavaType)
                .append(" as ct, ")
                .append(iColumnSize)
                .append(" as colSize, ")
                .append(iDecimalDigits)
                .append(" as colDecimalDigits, ")
                .append("'")
                .append(tableName)
                .append("' as tblname, ")
                .append("'")
                .append(escape(colName))
                .append("' as cn, ")
                .append("'")
                .append(escape(colType))
                .append("' as tn, ")
                .append(quote(colDefault == null ? null : escape(colDefault)))
                .append(" as colDefault,")
                .append(colAutoIncrement)
                .append(" as colautoincrement,")
                .append(colGenerated)
                .append(" as colgenerated");

            if (colNamePattern != null) {
              sql.append(" where upper(cn) like upper('")
                  .append(escape(colNamePattern))
                  .append("') ESCAPE '")
                  .append(getSearchStringEscape())
                  .append("'");
            }
          }
        }
      }
    } finally {
      if (rs != null) {
          rs.close();
      }
    }

    if (colFound) {
      sql.append(") order by TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION;");
    } else {
      sql.append(
          "select null as ordpos, null as colnullable, null as ct, null as colsize, null as colDecimalDigits, null as tblname, null as cn, null as tn, null as colDefault, null as colautoincrement, null as colgenerated) limit 0;");
    }

    Statement stat = connection.createStatement();
    return stat.executeQuery(sql.toString());
  }

  /**
   * Retrieves a description of the access rights for a table's columns.
   *
   * <P>Only privileges matching the column name criteria are
   * returned.  They are ordered by COLUMN_NAME and PRIVILEGE.
   *
   * <P>Each privilege description has the following columns:
   * <OL>
   * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be {@code null})
   * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be {@code null})
   * <LI><B>TABLE_NAME</B> String {@code =>} table name
   * <LI><B>COLUMN_NAME</B> String {@code =>} column name
   * <LI><B>GRANTOR</B> String {@code =>} grantor of access (may be {@code null})
   * <LI><B>GRANTEE</B> String {@code =>} grantee of access
   * <LI><B>PRIVILEGE</B> String {@code =>} name of access (SELECT,
   * INSERT, UPDATE, REFERENCES, ...)
   * <LI><B>IS_GRANTABLE</B> String {@code =>} "YES" if grantee is permitted
   * to grant to others; "NO" if not; {@code null} if unknown
   * </OL>
   *
   * @param catalog           a catalog name; must match the catalog name as it
   *                          is stored in the database; "" retrieves those without a catalog;
   *                          {@code null} means that the catalog name should not be used to narrow
   *                          the search
   * @param schema            a schema name; must match the schema name as it is
   *                          stored in the database; "" retrieves those without a schema;
   *                          {@code null} means that the schema name should not be used to narrow
   *                          the search
   * @param table             a table name; must match the table name as it is
   *                          stored in the database
   * @param columnNamePattern a column name pattern; must match the column
   *                          name as it is stored in the database
   * @return {@code ResultSet} - each row is a column privilege description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   */
  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
    if (getColumnPrivileges == null) {
      getColumnPrivileges =
          connection.prepareStatement(
              "select null as TABLE_CAT, null as TABLE_SCHEM, "
                  + "null as TABLE_NAME, null as COLUMN_NAME, null as GRANTOR, null as GRANTEE, "
                  + "null as PRIVILEGE, null as IS_GRANTABLE limit 0;");
    }

    return getColumnPrivileges.executeQuery();
  }

  /**
   * Retrieves a description of the access rights for each table available
   * in a catalog. Note that a table privilege applies to one or
   * more columns in the table. It would be wrong to assume that
   * this privilege applies to all columns (this may be true for
   * some systems but is not true for all.)
   *
   * <P>Only privileges matching the schema and table name
   * criteria are returned.  They are ordered by
   * {@code TABLE_CAT},
   * {@code TABLE_SCHEM}, {@code TABLE_NAME},
   * and {@code PRIVILEGE}.
   *
   * <P>Each privilege description has the following columns:
   * <OL>
   * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be {@code null})
   * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be {@code null})
   * <LI><B>TABLE_NAME</B> String {@code =>} table name
   * <LI><B>GRANTOR</B> String {@code =>} grantor of access (may be {@code null})
   * <LI><B>GRANTEE</B> String {@code =>} grantee of access
   * <LI><B>PRIVILEGE</B> String {@code =>} name of access (SELECT,
   * INSERT, UPDATE, REFERENCES, ...)
   * <LI><B>IS_GRANTABLE</B> String {@code =>} "YES" if grantee is permitted
   * to grant to others; "NO" if not; {@code null} if unknown
   * </OL>
   *
   * @param catalog          a catalog name; must match the catalog name as it
   *                         is stored in the database; "" retrieves those without a catalog;
   *                         {@code null} means that the catalog name should not be used to narrow
   *                         the search
   * @param schemaPattern    a schema name pattern; must match the schema name
   *                         as it is stored in the database; "" retrieves those without a schema;
   *                         {@code null} means that the schema name should not be used to narrow
   *                         the search
   * @param tableNamePattern a table name pattern; must match the
   *                         table name as it is stored in the database
   * @return {@code ResultSet} - each row is a table privilege description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   */
  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    if (getTablePrivileges == null) {
      getTablePrivileges =
          connection.prepareStatement(
              "select  null as TABLE_CAT, "
                  + "null as TABLE_SCHEM, null as TABLE_NAME, null as GRANTOR, null "
                  + "GRANTEE,  null as PRIVILEGE, null as IS_GRANTABLE limit 0;");
    }
    return getTablePrivileges.executeQuery();
  }

  /**
   * Retrieves a description of a table's optimal set of columns that
   * uniquely identifies a row. They are ordered by SCOPE.
   *
   * <P>Each column description has the following columns:
   * <OL>
   * <LI><B>SCOPE</B> short {@code =>} actual scope of result
   * <UL>
   * <LI> bestRowTemporary - very temporary, while using row
   * <LI> bestRowTransaction - valid for remainder of current transaction
   * <LI> bestRowSession - valid for remainder of current session
   * </UL>
   * <LI><B>COLUMN_NAME</B> String {@code =>} column name
   * <LI><B>DATA_TYPE</B> int {@code =>} SQL data type from java.sql.Types
   * <LI><B>TYPE_NAME</B> String {@code =>} Data source dependent type name,
   * for a UDT the type name is fully qualified
   * <LI><B>COLUMN_SIZE</B> int {@code =>} precision
   * <LI><B>BUFFER_LENGTH</B> int {@code =>} not used
   * <LI><B>DECIMAL_DIGITS</B> short  {@code =>} scale - Null is returned for data types where
   * DECIMAL_DIGITS is not applicable.
   * <LI><B>PSEUDO_COLUMN</B> short {@code =>} is this a pseudo column
   * like an Oracle ROWID
   * <UL>
   * <LI> bestRowUnknown - may or may not be pseudo column
   * <LI> bestRowNotPseudo - is NOT a pseudo column
   * <LI> bestRowPseudo - is a pseudo column
   * </UL>
   * </OL>
   *
   * <p>The COLUMN_SIZE column represents the specified column size for the given column.
   * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
   * For datetime datatypes, this is the length in characters of the String representation (assuming the
   * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
   * this is the length in bytes. Null is returned for data types where the
   * column size is not applicable.
   *
   * @param catalog  a catalog name; must match the catalog name as it
   *                 is stored in the database; "" retrieves those without a catalog;
   *                 {@code null} means that the catalog name should not be used to narrow
   *                 the search
   * @param schema   a schema name; must match the schema name
   *                 as it is stored in the database; "" retrieves those without a schema;
   *                 {@code null} means that the schema name should not be used to narrow
   *                 the search
   * @param table    a table name; must match the table name as it is stored
   *                 in the database
   * @param scope    the scope of interest; use same values as SCOPE
   * @param nullable include columns that are nullable.
   * @return {@code ResultSet} - each row is a column description
   * @throws SQLException if a database access error occurs
   */
  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
    if (getBestRowIdentifier == null) {
      getBestRowIdentifier =
          connection.prepareStatement(
              "select null as SCOPE, null as COLUMN_NAME, "
                  + "null as DATA_TYPE, null as TYPE_NAME, null as COLUMN_SIZE, "
                  + "null as BUFFER_LENGTH, null as DECIMAL_DIGITS, null as PSEUDO_COLUMN limit 0;");
    }

    return getBestRowIdentifier.executeQuery();
  }

  /**
   * Retrieves a description of a table's columns that are automatically
   * updated when any value in a row is updated.  They are
   * unordered.
   *
   * <P>Each column description has the following columns:
   * <OL>
   * <LI><B>SCOPE</B> short {@code =>} is not used
   * <LI><B>COLUMN_NAME</B> String {@code =>} column name
   * <LI><B>DATA_TYPE</B> int {@code =>} SQL data type from {@code java.sql.Types}
   * <LI><B>TYPE_NAME</B> String {@code =>} Data source-dependent type name
   * <LI><B>COLUMN_SIZE</B> int {@code =>} precision
   * <LI><B>BUFFER_LENGTH</B> int {@code =>} length of column value in bytes
   * <LI><B>DECIMAL_DIGITS</B> short  {@code =>} scale - Null is returned for data types where
   * DECIMAL_DIGITS is not applicable.
   * <LI><B>PSEUDO_COLUMN</B> short {@code =>} whether this is pseudo column
   * like an Oracle ROWID
   * <UL>
   * <LI> versionColumnUnknown - may or may not be pseudo column
   * <LI> versionColumnNotPseudo - is NOT a pseudo column
   * <LI> versionColumnPseudo - is a pseudo column
   * </UL>
   * </OL>
   *
   * <p>The COLUMN_SIZE column represents the specified column size for the given column.
   * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
   * For datetime datatypes, this is the length in characters of the String representation (assuming the
   * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
   * this is the length in bytes. Null is returned for data types where the
   * column size is not applicable.
   *
   * @param catalog a catalog name; must match the catalog name as it
   *                is stored in the database; "" retrieves those without a catalog;
   *                {@code null} means that the catalog name should not be used to narrow
   *                the search
   * @param schema  a schema name; must match the schema name
   *                as it is stored in the database; "" retrieves those without a schema;
   *                {@code null} means that the schema name should not be used to narrow
   *                the search
   * @param table   a table name; must match the table name as it is stored
   *                in the database
   * @return a {@code ResultSet} object in which each row is a
   * column description
   * @throws SQLException if a database access error occurs
   */
  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
    if (getVersionColumns == null) {
      getVersionColumns =
          connection.prepareStatement(
              "select null as SCOPE, null as COLUMN_NAME, "
                  + "null as DATA_TYPE, null as TYPE_NAME, null as COLUMN_SIZE, "
                  + "null as BUFFER_LENGTH, null as DECIMAL_DIGITS, null as PSEUDO_COLUMN limit 0;");
    }
    return getVersionColumns.executeQuery();
  }

  /**
   * Retrieves a description of the given table's primary key columns.  They
   * are ordered by COLUMN_NAME.
   *
   * <P>Each primary key column description has the following columns:
   * <OL>
   * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be {@code null})
   * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be {@code null})
   * <LI><B>TABLE_NAME</B> String {@code =>} table name
   * <LI><B>COLUMN_NAME</B> String {@code =>} column name
   * <LI><B>KEY_SEQ</B> short {@code =>} sequence number within primary key( a value
   * of 1 represents the first column of the primary key, a value of 2 would
   * represent the second column within the primary key).
   * <LI><B>PK_NAME</B> String {@code =>} primary key name (may be {@code null})
   * </OL>
   *
   * @param catalog a catalog name; must match the catalog name as it
   *                is stored in the database; "" retrieves those without a catalog;
   *                {@code null} means that the catalog name should not be used to narrow
   *                the search
   * @param schema  a schema name; must match the schema name
   *                as it is stored in the database; "" retrieves those without a schema;
   *                {@code null} means that the schema name should not be used to narrow
   *                the search
   * @param table   a table name; must match the table name as it is stored
   *                in the database
   * @return {@code ResultSet} - each row is a primary key column description
   * @throws SQLException if a database access error occurs
   */
  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    PrimaryKeyFinder pkFinder = new PrimaryKeyFinder(table);
    String[] columns = pkFinder.getColumns();

    Statement stat = connection.createStatement();
    StringBuilder sql = new StringBuilder(512);
    sql.append("select null as TABLE_CAT, null as TABLE_SCHEM, '")
        .append(escape(table))
        .append("' as TABLE_NAME, cn as COLUMN_NAME, ks as KEY_SEQ, pk as PK_NAME from (");

    if (columns == null) {
      sql.append("select null as cn, null as pk, 0 as ks) limit 0;");

      return stat.executeQuery(sql.toString());
    }

    String pkName = pkFinder.getName();
    if (pkName != null) {
      pkName = "'" + pkName + "'";
    }

    for (int i = 0; i < columns.length; i++) {
      if (i > 0) sql.append(" union ");
      sql.append("select ")
          .append(pkName)
          .append(" as pk, '")
          .append(escape(unquoteIdentifier(columns[i])))
          .append("' as cn, ")
          .append(i + 1)
          .append(" as ks");
    }

    return stat.executeQuery(sql.append(") order by cn;").toString());
  }

  /**
   * Retrieves a description of the primary key columns that are
   * referenced by the given table's foreign key columns (the primary keys
   * imported by a table).  They are ordered by PKTABLE_CAT,
   * PKTABLE_SCHEM, PKTABLE_NAME, and KEY_SEQ.
   *
   * <P>Each primary key column description has the following columns:
   * <OL>
   * <LI><B>PKTABLE_CAT</B> String {@code =>} primary key table catalog
   * being imported (may be {@code null})
   * <LI><B>PKTABLE_SCHEM</B> String {@code =>} primary key table schema
   * being imported (may be {@code null})
   * <LI><B>PKTABLE_NAME</B> String {@code =>} primary key table name
   * being imported
   * <LI><B>PKCOLUMN_NAME</B> String {@code =>} primary key column name
   * being imported
   * <LI><B>FKTABLE_CAT</B> String {@code =>} foreign key table catalog (may be {@code null})
   * <LI><B>FKTABLE_SCHEM</B> String {@code =>} foreign key table schema (may be {@code null})
   * <LI><B>FKTABLE_NAME</B> String {@code =>} foreign key table name
   * <LI><B>FKCOLUMN_NAME</B> String {@code =>} foreign key column name
   * <LI><B>KEY_SEQ</B> short {@code =>} sequence number within a foreign key( a value
   * of 1 represents the first column of the foreign key, a value of 2 would
   * represent the second column within the foreign key).
   * <LI><B>UPDATE_RULE</B> short {@code =>} What happens to a
   * foreign key when the primary key is updated:
   * <UL>
   * <LI> importedNoAction - do not allow update of primary
   * key if it has been imported
   * <LI> importedKeyCascade - change imported key to agree
   * with primary key update
   * <LI> importedKeySetNull - change imported key to {@code NULL}
   * if its primary key has been updated
   * <LI> importedKeySetDefault - change imported key to default values
   * if its primary key has been updated
   * <LI> importedKeyRestrict - same as importedKeyNoAction
   * (for ODBC 2.x compatibility)
   * </UL>
   * <LI><B>DELETE_RULE</B> short {@code =>} What happens to
   * the foreign key when primary is deleted.
   * <UL>
   * <LI> importedKeyNoAction - do not allow delete of primary
   * key if it has been imported
   * <LI> importedKeyCascade - delete rows that import a deleted key
   * <LI> importedKeySetNull - change imported key to NULL if
   * its primary key has been deleted
   * <LI> importedKeyRestrict - same as importedKeyNoAction
   * (for ODBC 2.x compatibility)
   * <LI> importedKeySetDefault - change imported key to default if
   * its primary key has been deleted
   * </UL>
   * <LI><B>FK_NAME</B> String {@code =>} foreign key name (may be {@code null})
   * <LI><B>PK_NAME</B> String {@code =>} primary key name (may be {@code null})
   * <LI><B>DEFERRABILITY</B> short {@code =>} can the evaluation of foreign key
   * constraints be deferred until commit
   * <UL>
   * <LI> importedKeyInitiallyDeferred - see SQL92 for definition
   * <LI> importedKeyInitiallyImmediate - see SQL92 for definition
   * <LI> importedKeyNotDeferrable - see SQL92 for definition
   * </UL>
   * </OL>
   *
   * @param catalog a catalog name; must match the catalog name as it
   *                is stored in the database; "" retrieves those without a catalog;
   *                {@code null} means that the catalog name should not be used to narrow
   *                the search
   * @param schema  a schema name; must match the schema name
   *                as it is stored in the database; "" retrieves those without a schema;
   *                {@code null} means that the schema name should not be used to narrow
   *                the search
   * @param table   a table name; must match the table name as it is stored
   *                in the database
   * @return {@code ResultSet} - each row is a primary key column description
   * @throws SQLException if a database access error occurs
   * @see #getExportedKeys
   */
  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    ResultSet rs;
    Statement stat = connection.createStatement();
    StringBuilder sql = new StringBuilder(700);

    sql.append("select ")
        .append(quote(catalog))
        .append(" as PKTABLE_CAT, ")
        .append(quote(schema))
        .append(" as PKTABLE_SCHEM, ")
        .append("ptn as PKTABLE_NAME, pcn as PKCOLUMN_NAME, ")
        .append(quote(catalog))
        .append(" as FKTABLE_CAT, ")
        .append(quote(schema))
        .append(" as FKTABLE_SCHEM, ")
        .append(quote(table))
        .append(" as FKTABLE_NAME, ")
        .append(
            "fcn as FKCOLUMN_NAME, ks as KEY_SEQ, ur as UPDATE_RULE, dr as DELETE_RULE, fkn as FK_NAME, pkn as PK_NAME, ")
        .append(DatabaseMetaData.importedKeyInitiallyDeferred)
        .append(" as DEFERRABILITY from (");

    // Use a try catch block to avoid "query does not return ResultSet" error
    try {
      rs = stat.executeQuery("pragma foreign_key_list('" + escape(table) + "');");
    } catch (SQLException e) {
      sql = appendDummyForeignKeyList(sql);
      return stat.executeQuery(sql.toString());
    }

    final ImportedKeyFinder impFkFinder = new ImportedKeyFinder(table);
    List<ForeignKey> fkNames = impFkFinder.getFkList();

    int i = 0;
    for (; rs.next(); i++) {
      int keySeq = rs.getInt(2) + 1;
      int keyId = rs.getInt(1);
      String PKTabName = rs.getString(3);
      String FKColName = rs.getString(4);
      String PKColName = rs.getString(5);

      String pkName = null;
      try {
        PrimaryKeyFinder pkFinder = new PrimaryKeyFinder(PKTabName);
        pkName = pkFinder.getName();
        if (PKColName == null) {
          PKColName = pkFinder.getColumns()[0];
        }
      } catch (SQLException ignored) {
      }

      String updateRule = rs.getString(6);
      String deleteRule = rs.getString(7);

      if (i > 0) {
        sql.append(" union all ");
      }

      String fkName = null;
      if (fkNames.size() > keyId) fkName = fkNames.get(keyId).getFkName();

      sql.append("select ")
          .append(keySeq)
          .append(" as ks,")
          .append("'")
          .append(escape(PKTabName))
          .append("' as ptn, '")
          .append(escape(FKColName))
          .append("' as fcn, '")
          .append(escape(PKColName))
          .append("' as pcn,")
          .append("case '")
          .append(escape(updateRule))
          .append("'")
          .append(" when 'NO ACTION' then ")
          .append(DatabaseMetaData.importedKeyNoAction)
          .append(" when 'CASCADE' then ")
          .append(DatabaseMetaData.importedKeyCascade)
          .append(" when 'RESTRICT' then ")
          .append(DatabaseMetaData.importedKeyRestrict)
          .append(" when 'SET NULL' then ")
          .append(DatabaseMetaData.importedKeySetNull)
          .append(" when 'SET DEFAULT' then ")
          .append(DatabaseMetaData.importedKeySetDefault)
          .append(" end as ur, ")
          .append("case '")
          .append(escape(deleteRule))
          .append("'")
          .append(" when 'NO ACTION' then ")
          .append(DatabaseMetaData.importedKeyNoAction)
          .append(" when 'CASCADE' then ")
          .append(DatabaseMetaData.importedKeyCascade)
          .append(" when 'RESTRICT' then ")
          .append(DatabaseMetaData.importedKeyRestrict)
          .append(" when 'SET NULL' then ")
          .append(DatabaseMetaData.importedKeySetNull)
          .append(" when 'SET DEFAULT' then ")
          .append(DatabaseMetaData.importedKeySetDefault)
          .append(" end as dr, ")
          .append(fkName == null ? "''" : quote(fkName))
          .append(" as fkn, ")
          .append(pkName == null ? "''" : quote(pkName))
          .append(" as pkn");
    }
    rs.close();

    if (i == 0) {
      sql = appendDummyForeignKeyList(sql);
    } else {
      sql.append(") ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ;");
    }

    return stat.executeQuery(sql.toString());
  }

  private static final Map<String, Integer> RULE_MAP = new HashMap<>();

  static {
    RULE_MAP.put("NO ACTION", DatabaseMetaData.importedKeyNoAction);
    RULE_MAP.put("CASCADE", DatabaseMetaData.importedKeyCascade);
    RULE_MAP.put("RESTRICT", DatabaseMetaData.importedKeyRestrict);
    RULE_MAP.put("SET NULL", DatabaseMetaData.importedKeySetNull);
    RULE_MAP.put("SET DEFAULT", DatabaseMetaData.importedKeySetDefault);
  }

  /**
   * Retrieves a description of the foreign key columns that reference the
   * given table's primary key columns (the foreign keys exported by a
   * table).  They are ordered by FKTABLE_CAT, FKTABLE_SCHEM,
   * FKTABLE_NAME, and KEY_SEQ.
   *
   * <P>Each foreign key column description has the following columns:
   * <OL>
   * <LI><B>PKTABLE_CAT</B> String {@code =>} primary key table catalog (may be {@code null})
   * <LI><B>PKTABLE_SCHEM</B> String {@code =>} primary key table schema (may be {@code null})
   * <LI><B>PKTABLE_NAME</B> String {@code =>} primary key table name
   * <LI><B>PKCOLUMN_NAME</B> String {@code =>} primary key column name
   * <LI><B>FKTABLE_CAT</B> String {@code =>} foreign key table catalog (may be {@code null})
   * being exported (may be {@code null})
   * <LI><B>FKTABLE_SCHEM</B> String {@code =>} foreign key table schema (may be {@code null})
   * being exported (may be {@code null})
   * <LI><B>FKTABLE_NAME</B> String {@code =>} foreign key table name
   * being exported
   * <LI><B>FKCOLUMN_NAME</B> String {@code =>} foreign key column name
   * being exported
   * <LI><B>KEY_SEQ</B> short {@code =>} sequence number within foreign key( a value
   * of 1 represents the first column of the foreign key, a value of 2 would
   * represent the second column within the foreign key).
   * <LI><B>UPDATE_RULE</B> short {@code =>} What happens to
   * foreign key when primary is updated:
   * <UL>
   * <LI> importedNoAction - do not allow update of primary
   * key if it has been imported
   * <LI> importedKeyCascade - change imported key to agree
   * with primary key update
   * <LI> importedKeySetNull - change imported key to {@code NULL} if
   * its primary key has been updated
   * <LI> importedKeySetDefault - change imported key to default values
   * if its primary key has been updated
   * <LI> importedKeyRestrict - same as importedKeyNoAction
   * (for ODBC 2.x compatibility)
   * </UL>
   * <LI><B>DELETE_RULE</B> short {@code =>} What happens to
   * the foreign key when primary is deleted.
   * <UL>
   * <LI> importedKeyNoAction - do not allow delete of primary
   * key if it has been imported
   * <LI> importedKeyCascade - delete rows that import a deleted key
   * <LI> importedKeySetNull - change imported key to {@code NULL} if
   * its primary key has been deleted
   * <LI> importedKeyRestrict - same as importedKeyNoAction
   * (for ODBC 2.x compatibility)
   * <LI> importedKeySetDefault - change imported key to default if
   * its primary key has been deleted
   * </UL>
   * <LI><B>FK_NAME</B> String {@code =>} foreign key name (may be {@code null})
   * <LI><B>PK_NAME</B> String {@code =>} primary key name (may be {@code null})
   * <LI><B>DEFERRABILITY</B> short {@code =>} can the evaluation of foreign key
   * constraints be deferred until commit
   * <UL>
   * <LI> importedKeyInitiallyDeferred - see SQL92 for definition
   * <LI> importedKeyInitiallyImmediate - see SQL92 for definition
   * <LI> importedKeyNotDeferrable - see SQL92 for definition
   * </UL>
   * </OL>
   *
   * @param catalog a catalog name; must match the catalog name as it
   *                is stored in this database; "" retrieves those without a catalog;
   *                {@code null} means that the catalog name should not be used to narrow
   *                the search
   * @param schema  a schema name; must match the schema name
   *                as it is stored in the database; "" retrieves those without a schema;
   *                {@code null} means that the schema name should not be used to narrow
   *                the search
   * @param table   a table name; must match the table name as it is stored
   *                in this database
   * @return a {@code ResultSet} object in which each row is a
   * foreign key column description
   * @throws SQLException if a database access error occurs
   * @see #getImportedKeys
   */
  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    PrimaryKeyFinder pkFinder = new PrimaryKeyFinder(table);
    String[] pkColumns = pkFinder.getColumns();
    Statement stat = connection.createStatement();

    catalog = (catalog != null) ? quote(catalog) : null;
    schema = (schema != null) ? quote(schema) : null;

    StringBuilder exportedKeysQuery = new StringBuilder(512);

    String target = null;
    int count = 0;
    if (pkColumns != null) {
      // retrieve table list
      ArrayList<String> tableList;
      try (ResultSet rs =
               stat.executeQuery("select name from sqlite_schema where type = 'table'")) {
        tableList = new ArrayList<>();

        while (rs.next()) {
          String tblname = rs.getString(1);
          tableList.add(tblname);
          if (tblname.equalsIgnoreCase(table)) {
            // get the correct case as in the database
            // (not uppercase nor lowercase)
            target = tblname;
          }
        }
      }

      // find imported keys for each table
      for (String tbl : tableList) {
        final ImportedKeyFinder impFkFinder = new ImportedKeyFinder(tbl);
        List<ForeignKey> fkNames = impFkFinder.getFkList();

        for (ForeignKey foreignKey : fkNames) {
          String PKTabName = foreignKey.getPkTableName();

          if (PKTabName == null || !PKTabName.equalsIgnoreCase(target)) {
            continue;
          }

          for (int j = 0; j < foreignKey.getColumnMappingCount(); j++) {
            int keySeq = j + 1;
            String[] columnMapping = foreignKey.getColumnMapping(j);
            String PKColName = columnMapping[1];
            PKColName = (PKColName == null) ? "" : PKColName;
            String FKColName = columnMapping[0];
            FKColName = (FKColName == null) ? "" : FKColName;

            boolean usePkName = false;
            for (String pkColumn : pkColumns) {
              if (pkColumn != null && pkColumn.equalsIgnoreCase(PKColName)) {
                usePkName = true;
                break;
              }
            }
            String pkName =
                (usePkName && pkFinder.getName() != null) ? pkFinder.getName() : "";

            exportedKeysQuery
                .append(count > 0 ? " union all select " : "select ")
                .append(keySeq)
                .append(" as ks, '")
                .append(escape(tbl))
                .append("' as fkt, '")
                .append(escape(FKColName))
                .append("' as fcn, '")
                .append(escape(PKColName))
                .append("' as pcn, '")
                .append(escape(pkName))
                .append("' as pkn, ")
                .append(RULE_MAP.get(foreignKey.getOnUpdate()))
                .append(" as ur, ")
                .append(RULE_MAP.get(foreignKey.getOnDelete()))
                .append(" as dr, ");

            String fkName = foreignKey.getFkName();

            if (fkName != null) {
              exportedKeysQuery.append("'").append(escape(fkName)).append("' as fkn");
            } else {
              exportedKeysQuery.append("'' as fkn");
            }

            count++;
          }
        }
      }
    }

    boolean hasImportedKey = (count > 0);
    StringBuilder sql = new StringBuilder(512);
    sql.append("select ")
        .append(catalog)
        .append(" as PKTABLE_CAT, ")
        .append(schema)
        .append(" as PKTABLE_SCHEM, ")
        .append(quote(target))
        .append(" as PKTABLE_NAME, ")
        .append(hasImportedKey ? "pcn" : "''")
        .append(" as PKCOLUMN_NAME, ")
        .append(catalog)
        .append(" as FKTABLE_CAT, ")
        .append(schema)
        .append(" as FKTABLE_SCHEM, ")
        .append(hasImportedKey ? "fkt" : "''")
        .append(" as FKTABLE_NAME, ")
        .append(hasImportedKey ? "fcn" : "''")
        .append(" as FKCOLUMN_NAME, ")
        .append(hasImportedKey ? "ks" : "-1")
        .append(" as KEY_SEQ, ")
        .append(hasImportedKey ? "ur" : "3")
        .append(" as UPDATE_RULE, ")
        .append(hasImportedKey ? "dr" : "3")
        .append(" as DELETE_RULE, ")
        .append(hasImportedKey ? "fkn" : "''")
        .append(" as FK_NAME, ")
        .append(hasImportedKey ? "pkn" : "''")
        .append(" as PK_NAME, ")
        .append(DatabaseMetaData.importedKeyInitiallyDeferred) // FIXME: Check for pragma
        // foreign_keys = true ?
        .append(" as DEFERRABILITY ");

    if (hasImportedKey) {
      sql.append("from (")
          .append(exportedKeysQuery)
          .append(") ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ");
    } else {
      sql.append("limit 0");
    }

    return stat.executeQuery(sql.toString());
  }

  /**
   * Retrieves a description of the foreign key columns in the given foreign key
   * table that reference the primary key or the columns representing a unique constraint of the  parent table (could be the same or a different table).
   * The number of columns returned from the parent table must match the number of
   * columns that make up the foreign key.  They
   * are ordered by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and
   * KEY_SEQ.
   *
   * <P>Each foreign key column description has the following columns:
   * <OL>
   * <LI><B>PKTABLE_CAT</B> String {@code =>} parent key table catalog (may be {@code null})
   * <LI><B>PKTABLE_SCHEM</B> String {@code =>} parent key table schema (may be {@code null})
   * <LI><B>PKTABLE_NAME</B> String {@code =>} parent key table name
   * <LI><B>PKCOLUMN_NAME</B> String {@code =>} parent key column name
   * <LI><B>FKTABLE_CAT</B> String {@code =>} foreign key table catalog (may be {@code null})
   * being exported (may be {@code null})
   * <LI><B>FKTABLE_SCHEM</B> String {@code =>} foreign key table schema (may be {@code null})
   * being exported (may be {@code null})
   * <LI><B>FKTABLE_NAME</B> String {@code =>} foreign key table name
   * being exported
   * <LI><B>FKCOLUMN_NAME</B> String {@code =>} foreign key column name
   * being exported
   * <LI><B>KEY_SEQ</B> short {@code =>} sequence number within foreign key( a value
   * of 1 represents the first column of the foreign key, a value of 2 would
   * represent the second column within the foreign key).
   * <LI><B>UPDATE_RULE</B> short {@code =>} What happens to
   * foreign key when parent key is updated:
   * <UL>
   * <LI> importedNoAction - do not allow update of parent
   * key if it has been imported
   * <LI> importedKeyCascade - change imported key to agree
   * with parent key update
   * <LI> importedKeySetNull - change imported key to {@code NULL} if
   * its parent key has been updated
   * <LI> importedKeySetDefault - change imported key to default values
   * if its parent key has been updated
   * <LI> importedKeyRestrict - same as importedKeyNoAction
   * (for ODBC 2.x compatibility)
   * </UL>
   * <LI><B>DELETE_RULE</B> short {@code =>} What happens to
   * the foreign key when parent key is deleted.
   * <UL>
   * <LI> importedKeyNoAction - do not allow delete of parent
   * key if it has been imported
   * <LI> importedKeyCascade - delete rows that import a deleted key
   * <LI> importedKeySetNull - change imported key to {@code NULL} if
   * its primary key has been deleted
   * <LI> importedKeyRestrict - same as importedKeyNoAction
   * (for ODBC 2.x compatibility)
   * <LI> importedKeySetDefault - change imported key to default if
   * its parent key has been deleted
   * </UL>
   * <LI><B>FK_NAME</B> String {@code =>} foreign key name (may be {@code null})
   * <LI><B>PK_NAME</B> String {@code =>} parent key name (may be {@code null})
   * <LI><B>DEFERRABILITY</B> short {@code =>} can the evaluation of foreign key
   * constraints be deferred until commit
   * <UL>
   * <LI> importedKeyInitiallyDeferred - see SQL92 for definition
   * <LI> importedKeyInitiallyImmediate - see SQL92 for definition
   * <LI> importedKeyNotDeferrable - see SQL92 for definition
   * </UL>
   * </OL>
   *
   * @param pc  a catalog name; must match the catalog name
   *                       as it is stored in the database; "" retrieves those without a
   *                       catalog; {@code null} means drop catalog name from the selection criteria
   * @param ps   a schema name; must match the schema name as
   *                       it is stored in the database; "" retrieves those without a schema;
   *                       {@code null} means drop schema name from the selection criteria
   * @param pt    the name of the table that exports the key; must match
   *                       the table name as it is stored in the database
   * @param fc a catalog name; must match the catalog name as
   *                       it is stored in the database; "" retrieves those without a
   *                       catalog; {@code null} means drop catalog name from the selection criteria
   * @param fs  a schema name; must match the schema name as it
   *                       is stored in the database; "" retrieves those without a schema;
   *                       {@code null} means drop schema name from the selection criteria
   * @param ft   the name of the table that imports the key; must match
   *                       the table name as it is stored in the database
   * @return {@code ResultSet} - each row is a foreign key column description
   * @throws SQLException if a database access error occurs
   * @see #getImportedKeys
   */
  @Override
  public ResultSet getCrossReference(String pc, String ps, String pt, String fc, String fs, String ft) throws SQLException {
    if (pt == null) {
      return getExportedKeys(fc, fs, ft);
    }

    if (ft == null) {
      return getImportedKeys(pc, ps, pt);
    }

    String query =
        "select "
            + quote(pc)
            + " as PKTABLE_CAT, "
            + quote(ps)
            + " as PKTABLE_SCHEM, "
            + quote(pt)
            + " as PKTABLE_NAME, "
            + "'' as PKCOLUMN_NAME, "
            + quote(fc)
            + " as FKTABLE_CAT, "
            + quote(fs)
            + " as FKTABLE_SCHEM, "
            + quote(ft)
            + " as FKTABLE_NAME, "
            + "'' as FKCOLUMN_NAME, -1 as KEY_SEQ, 3 as UPDATE_RULE, 3 as DELETE_RULE, '' as FK_NAME, '' as PK_NAME, "
            + DatabaseMetaData.importedKeyInitiallyDeferred
            + " as DEFERRABILITY limit 0 ";

    return connection.createStatement().executeQuery(query);
  }

  /**
   * Retrieves a description of all the data types supported by
   * this database. They are ordered by DATA_TYPE and then by how
   * closely the data type maps to the corresponding JDBC SQL type.
   *
   * <P>If the database supports SQL distinct types, then getTypeInfo() will return
   * a single row with a TYPE_NAME of DISTINCT and a DATA_TYPE of Types.DISTINCT.
   * If the database supports SQL structured types, then getTypeInfo() will return
   * a single row with a TYPE_NAME of STRUCT and a DATA_TYPE of Types.STRUCT.
   *
   * <P>If SQL distinct or structured types are supported, then information on the
   * individual types may be obtained from the getUDTs() method.
   *
   *
   * <P>Each type description has the following columns:
   * <OL>
   * <LI><B>TYPE_NAME</B> String {@code =>} Type name
   * <LI><B>DATA_TYPE</B> int {@code =>} SQL data type from java.sql.Types
   * <LI><B>PRECISION</B> int {@code =>} maximum precision
   * <LI><B>LITERAL_PREFIX</B> String {@code =>} prefix used to quote a literal
   * (may be {@code null})
   * <LI><B>LITERAL_SUFFIX</B> String {@code =>} suffix used to quote a literal
   * (may be {@code null})
   * <LI><B>CREATE_PARAMS</B> String {@code =>} parameters used in creating
   * the type (may be {@code null})
   * <LI><B>NULLABLE</B> short {@code =>} can you use NULL for this type.
   * <UL>
   * <LI> typeNoNulls - does not allow NULL values
   * <LI> typeNullable - allows NULL values
   * <LI> typeNullableUnknown - nullability unknown
   * </UL>
   * <LI><B>CASE_SENSITIVE</B> boolean{@code =>} is it case sensitive.
   * <LI><B>SEARCHABLE</B> short {@code =>} can you use "WHERE" based on this type:
   * <UL>
   * <LI> typePredNone - No support
   * <LI> typePredChar - Only supported with WHERE .. LIKE
   * <LI> typePredBasic - Supported except for WHERE .. LIKE
   * <LI> typeSearchable - Supported for all WHERE ..
   * </UL>
   * <LI><B>UNSIGNED_ATTRIBUTE</B> boolean {@code =>} is it unsigned.
   * <LI><B>FIXED_PREC_SCALE</B> boolean {@code =>} can it be a money value.
   * <LI><B>AUTO_INCREMENT</B> boolean {@code =>} can it be used for an
   * auto-increment value.
   * <LI><B>LOCAL_TYPE_NAME</B> String {@code =>} localized version of type name
   * (may be {@code null})
   * <LI><B>MINIMUM_SCALE</B> short {@code =>} minimum scale supported
   * <LI><B>MAXIMUM_SCALE</B> short {@code =>} maximum scale supported
   * <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
   * <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
   * <LI><B>NUM_PREC_RADIX</B> int {@code =>} usually 2 or 10
   * </OL>
   *
   * <p>The PRECISION column represents the maximum column size that the server supports for the given datatype.
   * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
   * For datetime datatypes, this is the length in characters of the String representation (assuming the
   * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
   * this is the length in bytes. Null is returned for data types where the
   * column size is not applicable.
   *
   * @return a {@code ResultSet} object in which each row is an SQL
   * type description
   * @throws SQLException if a database access error occurs
   */
  @Override
  public ResultSet getTypeInfo() throws SQLException {
    if (getTypeInfo == null) {
      String sql =
          valuesQuery(
              Arrays.asList(
                  "TYPE_NAME",
                  "DATA_TYPE",
                  "PRECISION",
                  "LITERAL_PREFIX",
                  "LITERAL_SUFFIX",
                  "CREATE_PARAMS",
                  "NULLABLE",
                  "CASE_SENSITIVE",
                  "SEARCHABLE",
                  "UNSIGNED_ATTRIBUTE",
                  "FIXED_PREC_SCALE",
                  "AUTO_INCREMENT",
                  "LOCAL_TYPE_NAME",
                  "MINIMUM_SCALE",
                  "MAXIMUM_SCALE",
                  "SQL_DATA_TYPE",
                  "SQL_DATETIME_SUB",
                  "NUM_PREC_RADIX"),
              Arrays.asList(
                  Arrays.asList(
                      "BLOB",
                      Types.BLOB,
                      0,
                      null,
                      null,
                      null,
                      DatabaseMetaData.typeNullable,
                      0,
                      DatabaseMetaData.typeSearchable,
                      1,
                      0,
                      0,
                      null,
                      0,
                      0,
                      0,
                      0,
                      10),
                  Arrays.asList(
                      "INTEGER",
                      Types.INTEGER,
                      0,
                      null,
                      null,
                      null,
                      DatabaseMetaData.typeNullable,
                      0,
                      DatabaseMetaData.typeSearchable,
                      0,
                      0,
                      1,
                      null,
                      0,
                      0,
                      0,
                      0,
                      10),
                  Arrays.asList(
                      "NULL",
                      Types.NULL,
                      0,
                      null,
                      null,
                      null,
                      DatabaseMetaData.typeNullable,
                      0,
                      DatabaseMetaData.typeSearchable,
                      1,
                      0,
                      0,
                      null,
                      0,
                      0,
                      0,
                      0,
                      10),
                  Arrays.asList(
                      "REAL",
                      Types.REAL,
                      0,
                      null,
                      null,
                      null,
                      DatabaseMetaData.typeNullable,
                      0,
                      DatabaseMetaData.typeSearchable,
                      0,
                      0,
                      0,
                      null,
                      0,
                      0,
                      0,
                      0,
                      10),
                  Arrays.asList(
                      "TEXT",
                      Types.VARCHAR,
                      0,
                      null,
                      null,
                      null,
                      DatabaseMetaData.typeNullable,
                      1,
                      DatabaseMetaData.typeSearchable,
                      1,
                      0,
                      0,
                      null,
                      0,
                      0,
                      0,
                      0,
                      10)))
              + " order by DATA_TYPE";
      getTypeInfo = connection.prepareStatement(sql);
    }

    getTypeInfo.clearParameters();
    return getTypeInfo.executeQuery();
  }

  /**
   * Retrieves a description of the given table's indices and statistics. They are
   * ordered by NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION.
   *
   * <P>Each index column description has the following columns:
   * <OL>
   * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be {@code null})
   * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be {@code null})
   * <LI><B>TABLE_NAME</B> String {@code =>} table name
   * <LI><B>NON_UNIQUE</B> boolean {@code =>} Can index values be non-unique.
   * false when TYPE is tableIndexStatistic
   * <LI><B>INDEX_QUALIFIER</B> String {@code =>} index catalog (may be {@code null});
   * {@code null} when TYPE is tableIndexStatistic
   * <LI><B>INDEX_NAME</B> String {@code =>} index name; {@code null} when TYPE is
   * tableIndexStatistic
   * <LI><B>TYPE</B> short {@code =>} index type:
   * <UL>
   * <LI> tableIndexStatistic - this identifies table statistics that are
   * returned in conjunction with a table's index descriptions
   * <LI> tableIndexClustered - this is a clustered index
   * <LI> tableIndexHashed - this is a hashed index
   * <LI> tableIndexOther - this is some other style of index
   * </UL>
   * <LI><B>ORDINAL_POSITION</B> short {@code =>} column sequence number
   * within index; zero when TYPE is tableIndexStatistic
   * <LI><B>COLUMN_NAME</B> String {@code =>} column name; {@code null} when TYPE is
   * tableIndexStatistic
   * <LI><B>ASC_OR_DESC</B> String {@code =>} column sort sequence, "A" {@code =>} ascending,
   * "D" {@code =>} descending, may be {@code null} if sort sequence is not supported;
   * {@code null} when TYPE is tableIndexStatistic
   * <LI><B>CARDINALITY</B> long {@code =>} When TYPE is tableIndexStatistic, then
   * this is the number of rows in the table; otherwise, it is the
   * number of unique values in the index.
   * <LI><B>PAGES</B> long {@code =>} When TYPE is  tableIndexStatistic then
   * this is the number of pages used for the table, otherwise it
   * is the number of pages used for the current index.
   * <LI><B>FILTER_CONDITION</B> String {@code =>} Filter condition, if any.
   * (may be {@code null})
   * </OL>
   *
   * @param catalog     a catalog name; must match the catalog name as it
   *                    is stored in this database; "" retrieves those without a catalog;
   *                    {@code null} means that the catalog name should not be used to narrow
   *                    the search
   * @param schema      a schema name; must match the schema name
   *                    as it is stored in this database; "" retrieves those without a schema;
   *                    {@code null} means that the schema name should not be used to narrow
   *                    the search
   * @param table       a table name; must match the table name as it is stored
   *                    in this database
   * @param unique      when true, return only indices for unique values;
   *                    when false, return indices regardless of whether unique or not
   * @param approximate when true, result is allowed to reflect approximate
   *                    or out of data values; when false, results are requested to be
   *                    accurate
   * @return {@code ResultSet} - each row is an index column description
   * @throws SQLException if a database access error occurs
   */
  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
    ResultSet rs;
    Statement stat = connection.createStatement();
    StringBuilder sql = new StringBuilder(500);

    // define the column header
    // this is from the JDBC spec, it is part of the driver protocol
    sql.append("select null as TABLE_CAT, null as TABLE_SCHEM, '")
        .append(escape(table))
        .append(
            "' as TABLE_NAME, un as NON_UNIQUE, null as INDEX_QUALIFIER, n as INDEX_NAME, ")
        .append(Integer.toString(DatabaseMetaData.tableIndexOther))
        .append(" as TYPE, op as ORDINAL_POSITION, ")
        .append(
            "cn as COLUMN_NAME, null as ASC_OR_DESC, 0 as CARDINALITY, 0 as PAGES, null as FILTER_CONDITION from (");

    // this always returns a result set now, previously threw exception
    rs = stat.executeQuery("pragma index_list('" + escape(table) + "');");

    ArrayList<ArrayList<Object>> indexList = new ArrayList<>();
    while (rs.next()) {
      indexList.add(new ArrayList<>());
      indexList.get(indexList.size() - 1).add(rs.getString(2));
      indexList.get(indexList.size() - 1).add(rs.getInt(3));
    }
    rs.close();
    if (indexList.size() == 0) {
      // if pragma index_list() returns no information, use this null block
      sql.append("select null as un, null as n, null as op, null as cn) limit 0;");
      return stat.executeQuery(sql.toString());
    } else {
      // loop over results from pragma call, getting specific info for each index

      Iterator<ArrayList<Object>> indexIterator = indexList.iterator();
      ArrayList<Object> currentIndex;

      ArrayList<String> unionAll = new ArrayList<>();

      while (indexIterator.hasNext()) {
        currentIndex = indexIterator.next();
        String indexName = currentIndex.get(0).toString();
        rs = stat.executeQuery("pragma index_info('" + escape(indexName) + "');");

        while (rs.next()) {

          StringBuilder sqlRow = new StringBuilder();

          String colName = rs.getString(3);
          sqlRow.append("select ")
              .append(1 - (Integer) currentIndex.get(1))
              .append(" as un,'")
              .append(escape(indexName))
              .append("' as n,")
              .append(rs.getInt(1) + 1)
              .append(" as op,");
          if (colName == null) { // expression index
            sqlRow.append("null");
          } else {
            sqlRow.append("'").append(escape(colName)).append("'");
          }
          sqlRow.append(" as cn");

          unionAll.add(sqlRow.toString());
        }

        rs.close();
      }

      String sqlBlock = String.join(" union all ", unionAll);

      return stat.executeQuery(sql.append(sqlBlock).append(");").toString());
    }
  }

  /**
   * Retrieves whether this database supports the given result set type.
   *
   * @param type defined in {@code java.sql.ResultSet}
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @see Connection
   * @since 1.2
   */
  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    return type == ResultSet.TYPE_FORWARD_ONLY;
  }

  /**
   * Retrieves whether this database supports the given concurrency type
   * in combination with the given result set type.
   *
   * @param type        defined in {@code java.sql.ResultSet}
   * @param concurrency type defined in {@code java.sql.ResultSet}
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @see Connection
   * @since 1.2
   */
  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
  }

  /**
   * Retrieves whether for the given type of {@code ResultSet} object,
   * the result set's own updates are visible.
   *
   * @param type the {@code ResultSet} type; one of
   *             {@code ResultSet.TYPE_FORWARD_ONLY},
   *             {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *             {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return {@code true} if updates are visible for the given result set type;
   * {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.2
   */
  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  /**
   * Retrieves whether a result set's own deletes are visible.
   *
   * @param type the {@code ResultSet} type; one of
   *             {@code ResultSet.TYPE_FORWARD_ONLY},
   *             {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *             {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return {@code true} if deletes are visible for the given result set type;
   * {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.2
   */
  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    return false;
  }

  /**
   * Retrieves whether a result set's own inserts are visible.
   *
   * @param type the {@code ResultSet} type; one of
   *             {@code ResultSet.TYPE_FORWARD_ONLY},
   *             {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *             {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return {@code true} if inserts are visible for the given result set type;
   * {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.2
   */
  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  /**
   * Retrieves whether updates made by others are visible.
   *
   * @param type the {@code ResultSet} type; one of
   *             {@code ResultSet.TYPE_FORWARD_ONLY},
   *             {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *             {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return {@code true} if updates made by others
   * are visible for the given result set type;
   * {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.2
   */
  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  /**
   * Retrieves whether deletes made by others are visible.
   *
   * @param type the {@code ResultSet} type; one of
   *             {@code ResultSet.TYPE_FORWARD_ONLY},
   *             {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *             {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return {@code true} if deletes made by others
   * are visible for the given result set type;
   * {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.2
   */
  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    return false;
  }

  /**
   * Retrieves whether inserts made by others are visible.
   *
   * @param type the {@code ResultSet} type; one of
   *             {@code ResultSet.TYPE_FORWARD_ONLY},
   *             {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *             {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return {@code true} if inserts made by others
   * are visible for the given result set type;
   * {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.2
   */
  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  /**
   * Retrieves whether or not a visible row update can be detected by
   * calling the method {@code ResultSet.rowUpdated}.
   *
   * @param type the {@code ResultSet} type; one of
   *             {@code ResultSet.TYPE_FORWARD_ONLY},
   *             {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *             {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return {@code true} if changes are detected by the result set type;
   * {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.2
   */
  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    return false;
  }

  /**
   * Retrieves whether or not a visible row delete can be detected by
   * calling the method {@code ResultSet.rowDeleted}.  If the method
   * {@code deletesAreDetected} returns {@code false}, it means that
   * deleted rows are removed from the result set.
   *
   * @param type the {@code ResultSet} type; one of
   *             {@code ResultSet.TYPE_FORWARD_ONLY},
   *             {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *             {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return {@code true} if deletes are detected by the given result set type;
   * {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.2
   */
  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    return false;
  }

  /**
   * Retrieves whether or not a visible row insert can be detected
   * by calling the method {@code ResultSet.rowInserted}.
   *
   * @param type the {@code ResultSet} type; one of
   *             {@code ResultSet.TYPE_FORWARD_ONLY},
   *             {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *             {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return {@code true} if changes are detected by the specified result
   * set type; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.2
   */
  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports batch updates.
   *
   * @return {@code true} if this database supports batch updates;
   * {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.2
   */
  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return true;
  }

  /**
   * Retrieves a description of the user-defined types (UDTs) defined
   * in a particular schema.  Schema-specific UDTs may have type
   * {@code JAVA_OBJECT}, {@code STRUCT},
   * or {@code DISTINCT}.
   *
   * <P>Only types matching the catalog, schema, type name and type
   * criteria are returned.  They are ordered by {@code DATA_TYPE},
   * {@code TYPE_CAT}, {@code TYPE_SCHEM}  and
   * {@code TYPE_NAME}.  The type name parameter may be a fully-qualified
   * name.  In this case, the catalog and schemaPattern parameters are
   * ignored.
   *
   * <P>Each type description has the following columns:
   * <OL>
   * <LI><B>TYPE_CAT</B> String {@code =>} the type's catalog (may be {@code null})
   * <LI><B>TYPE_SCHEM</B> String {@code =>} type's schema (may be {@code null})
   * <LI><B>TYPE_NAME</B> String {@code =>} type name
   * <LI><B>CLASS_NAME</B> String {@code =>} Java class name
   * <LI><B>DATA_TYPE</B> int {@code =>} type value defined in java.sql.Types.
   * One of JAVA_OBJECT, STRUCT, or DISTINCT
   * <LI><B>REMARKS</B> String {@code =>} explanatory comment on the type
   * <LI><B>BASE_TYPE</B> short {@code =>} type code of the source type of a
   * DISTINCT type or the type that implements the user-generated
   * reference type of the SELF_REFERENCING_COLUMN of a structured
   * type as defined in java.sql.Types ({@code null} if DATA_TYPE is not
   * DISTINCT or not STRUCT with REFERENCE_GENERATION = USER_DEFINED)
   * </OL>
   *
   * <P><B>Note:</B> If the driver does not support UDTs, an empty
   * result set is returned.
   *
   * @param catalog         a catalog name; must match the catalog name as it
   *                        is stored in the database; "" retrieves those without a catalog;
   *                        {@code null} means that the catalog name should not be used to narrow
   *                        the search
   * @param schemaPattern   a schema pattern name; must match the schema name
   *                        as it is stored in the database; "" retrieves those without a schema;
   *                        {@code null} means that the schema name should not be used to narrow
   *                        the search
   * @param typeNamePattern a type name pattern; must match the type name
   *                        as it is stored in the database; may be a fully qualified name
   * @param types           a list of user-defined types (JAVA_OBJECT,
   *                        STRUCT, or DISTINCT) to include; {@code null} returns all types
   * @return {@code ResultSet} object in which each row describes a UDT
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   * @since 1.2
   */
  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
    if (getUDTs == null) {
      getUDTs =
          connection.prepareStatement(
              "select  null as TYPE_CAT, null as TYPE_SCHEM, "
                  + "null as TYPE_NAME,  null as CLASS_NAME,  null as DATA_TYPE, null as REMARKS, "
                  + "null as BASE_TYPE "
                  + "limit 0;");
    }

    getUDTs.clearParameters();
    return getUDTs.executeQuery();
  }

  /**
   * Retrieves the connection that produced this metadata object.
   *
   * @return the connection that produced this metadata object
   * @throws SQLException if a database access error occurs
   * @since 1.2
   */
  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }

  /**
   * Retrieves whether this database supports savepoints.
   *
   * @return {@code true} if savepoints are supported;
   * {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports named parameters to callable
   * statements.
   *
   * @return {@code true} if named parameters are supported;
   * {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return true;
  }

  /**
   * Retrieves whether it is possible to have multiple {@code ResultSet} objects
   * returned from a {@code CallableStatement} object
   * simultaneously.
   *
   * @return {@code true} if a {@code CallableStatement} object
   * can return multiple {@code ResultSet} objects
   * simultaneously; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether auto-generated keys can be retrieved after
   * a statement has been executed
   *
   * @return {@code true} if auto-generated keys can be retrieved
   * after a statement has executed; {@code false} otherwise
   * <p>If {@code true} is returned, the JDBC driver must support the
   * returning of auto-generated keys for at least SQL INSERT statements
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    return false;
  }

  /**
   * Retrieves a description of the user-defined type (UDT) hierarchies defined in a
   * particular schema in this database. Only the immediate super type/
   * sub type relationship is modeled.
   * <p>
   * Only supertype information for UDTs matching the catalog,
   * schema, and type name is returned. The type name parameter
   * may be a fully-qualified name. When the UDT name supplied is a
   * fully-qualified name, the catalog and schemaPattern parameters are
   * ignored.
   * <p>
   * If a UDT does not have a direct super type, it is not listed here.
   * A row of the {@code ResultSet} object returned by this method
   * describes the designated UDT and a direct supertype. A row has the following
   * columns:
   * <OL>
   * <LI><B>TYPE_CAT</B> String {@code =>} the UDT's catalog (may be {@code null})
   * <LI><B>TYPE_SCHEM</B> String {@code =>} UDT's schema (may be {@code null})
   * <LI><B>TYPE_NAME</B> String {@code =>} type name of the UDT
   * <LI><B>SUPERTYPE_CAT</B> String {@code =>} the direct super type's catalog
   * (may be {@code null})
   * <LI><B>SUPERTYPE_SCHEM</B> String {@code =>} the direct super type's schema
   * (may be {@code null})
   * <LI><B>SUPERTYPE_NAME</B> String {@code =>} the direct super type's name
   * </OL>
   *
   * <P><B>Note:</B> If the driver does not support type hierarchies, an
   * empty result set is returned.
   *
   * @param catalog         a catalog name; "" retrieves those without a catalog;
   *                        {@code null} means drop catalog name from the selection criteria
   * @param schemaPattern   a schema name pattern; "" retrieves those
   *                        without a schema
   * @param typeNamePattern a UDT name pattern; may be a fully-qualified
   *                        name
   * @return a {@code ResultSet} object in which a row gives information
   * about the designated UDT
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   * @since 1.4
   */
  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
    if (getSuperTypes == null) {
      getSuperTypes =
          connection.prepareStatement(
              "select null as TYPE_CAT, null as TYPE_SCHEM, "
                  + "null as TYPE_NAME, null as SUPERTYPE_CAT, null as SUPERTYPE_SCHEM, "
                  + "null as SUPERTYPE_NAME limit 0;");
    }
    return getSuperTypes.executeQuery();
  }

  /**
   * Retrieves a description of the table hierarchies defined in a particular
   * schema in this database.
   *
   * <P>Only supertable information for tables matching the catalog, schema
   * and table name are returned. The table name parameter may be a fully-
   * qualified name, in which case, the catalog and schemaPattern parameters
   * are ignored. If a table does not have a super table, it is not listed here.
   * Supertables have to be defined in the same catalog and schema as the
   * sub tables. Therefore, the type description does not need to include
   * this information for the supertable.
   *
   * <P>Each type description has the following columns:
   * <OL>
   * <LI><B>TABLE_CAT</B> String {@code =>} the type's catalog (may be {@code null})
   * <LI><B>TABLE_SCHEM</B> String {@code =>} type's schema (may be {@code null})
   * <LI><B>TABLE_NAME</B> String {@code =>} type name
   * <LI><B>SUPERTABLE_NAME</B> String {@code =>} the direct super type's name
   * </OL>
   *
   * <P><B>Note:</B> If the driver does not support type hierarchies, an
   * empty result set is returned.
   *
   * @param catalog          a catalog name; "" retrieves those without a catalog;
   *                         {@code null} means drop catalog name from the selection criteria
   * @param schemaPattern    a schema name pattern; "" retrieves those
   *                         without a schema
   * @param tableNamePattern a table name pattern; may be a fully-qualified
   *                         name
   * @return a {@code ResultSet} object in which each row is a type description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   * @since 1.4
   */
  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    if (getSuperTables == null) {
      getSuperTables =
          connection.prepareStatement(
              "select null as TABLE_CAT, null as TABLE_SCHEM, "
                  + "null as TABLE_NAME, null as SUPERTABLE_NAME limit 0;");
    }
    return getSuperTables.executeQuery();
  }

  /**
   * Retrieves a description of the given attribute of the given type
   * for a user-defined type (UDT) that is available in the given schema
   * and catalog.
   * <p>
   * Descriptions are returned only for attributes of UDTs matching the
   * catalog, schema, type, and attribute name criteria. They are ordered by
   * {@code TYPE_CAT}, {@code TYPE_SCHEM},
   * {@code TYPE_NAME} and {@code ORDINAL_POSITION}. This description
   * does not contain inherited attributes.
   * <p>
   * The {@code ResultSet} object that is returned has the following
   * columns:
   * <OL>
   * <LI><B>TYPE_CAT</B> String {@code =>} type catalog (may be {@code null})
   * <LI><B>TYPE_SCHEM</B> String {@code =>} type schema (may be {@code null})
   * <LI><B>TYPE_NAME</B> String {@code =>} type name
   * <LI><B>ATTR_NAME</B> String {@code =>} attribute name
   * <LI><B>DATA_TYPE</B> int {@code =>} attribute type SQL type from java.sql.Types
   * <LI><B>ATTR_TYPE_NAME</B> String {@code =>} Data source dependent type name.
   * For a UDT, the type name is fully qualified. For a REF, the type name is
   * fully qualified and represents the target type of the reference type.
   * <LI><B>ATTR_SIZE</B> int {@code =>} column size.  For char or date
   * types this is the maximum number of characters; for numeric or
   * decimal types this is precision.
   * <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned for data types where
   * DECIMAL_DIGITS is not applicable.
   * <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
   * <LI><B>NULLABLE</B> int {@code =>} whether NULL is allowed
   * <UL>
   * <LI> attributeNoNulls - might not allow NULL values
   * <LI> attributeNullable - definitely allows NULL values
   * <LI> attributeNullableUnknown - nullability unknown
   * </UL>
   * <LI><B>REMARKS</B> String {@code =>} comment describing column (may be {@code null})
   * <LI><B>ATTR_DEF</B> String {@code =>} default value (may be {@code null})
   * <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
   * <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
   * <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the
   * maximum number of bytes in the column
   * <LI><B>ORDINAL_POSITION</B> int {@code =>} index of the attribute in the UDT
   * (starting at 1)
   * <LI><B>IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine
   * the nullability for a attribute.
   * <UL>
   * <LI> YES           --- if the attribute can include NULLs
   * <LI> NO            --- if the attribute cannot include NULLs
   * <LI> empty string  --- if the nullability for the
   * attribute is unknown
   * </UL>
   * <LI><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the
   * scope of a reference attribute ({@code null} if DATA_TYPE isn't REF)
   * <LI><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the
   * scope of a reference attribute ({@code null} if DATA_TYPE isn't REF)
   * <LI><B>SCOPE_TABLE</B> String {@code =>} table name that is the scope of a
   * reference attribute ({@code null} if the DATA_TYPE isn't REF)
   * <LI><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or user-generated
   * Ref type,SQL type from java.sql.Types ({@code null} if DATA_TYPE
   * isn't DISTINCT or user-generated REF)
   * </OL>
   *
   * @param catalog              a catalog name; must match the catalog name as it
   *                             is stored in the database; "" retrieves those without a catalog;
   *                             {@code null} means that the catalog name should not be used to narrow
   *                             the search
   * @param schemaPattern        a schema name pattern; must match the schema name
   *                             as it is stored in the database; "" retrieves those without a schema;
   *                             {@code null} means that the schema name should not be used to narrow
   *                             the search
   * @param typeNamePattern      a type name pattern; must match the
   *                             type name as it is stored in the database
   * @param attributeNamePattern an attribute name pattern; must match the attribute
   *                             name as it is declared in the database
   * @return a {@code ResultSet} object in which each row is an
   * attribute description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   * @since 1.4
   */
  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
    if (getAttributes == null) {
      getAttributes =
          connection.prepareStatement(
              "select null as TYPE_CAT, null as TYPE_SCHEM, "
                  + "null as TYPE_NAME, null as ATTR_NAME, null as DATA_TYPE, "
                  + "null as ATTR_TYPE_NAME, null as ATTR_SIZE, null as DECIMAL_DIGITS, "
                  + "null as NUM_PREC_RADIX, null as NULLABLE, null as REMARKS, null as ATTR_DEF, "
                  + "null as SQL_DATA_TYPE, null as SQL_DATETIME_SUB, null as CHAR_OCTET_LENGTH, "
                  + "null as ORDINAL_POSITION, null as IS_NULLABLE, null as SCOPE_CATALOG, "
                  + "null as SCOPE_SCHEMA, null as SCOPE_TABLE, null as SOURCE_DATA_TYPE limit 0;");
    }

    return getAttributes.executeQuery();
  }

  /**
   * Retrieves whether this database supports the given result set holdability.
   *
   * @param holdability one of the following constants:
   *                    {@code ResultSet.HOLD_CURSORS_OVER_COMMIT} or
   *                    {@code ResultSet.CLOSE_CURSORS_AT_COMMIT}
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @see Connection
   * @since 1.4
   */
  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  /**
   * Retrieves this database's default holdability for {@code ResultSet}
   * objects.
   *
   * @return the default holdability; either
   * {@code ResultSet.HOLD_CURSORS_OVER_COMMIT} or
   * {@code ResultSet.CLOSE_CURSORS_AT_COMMIT}
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public int getResultSetHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  /**
   * Retrieves the major version number of the underlying database.
   *
   * @return the underlying database's major version
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return 0; // TODO: We can get this from server
  }

  /**
   * Retrieves the minor version number of the underlying database.
   *
   * @return underlying database's minor version
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return 0; // TODO: We can get this from server
  }

  /**
   * Retrieves the major JDBC version number for this
   * driver.
   *
   * @return JDBC version major number
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return 0;
  }

  /**
   * Retrieves the minor JDBC version number for this
   * driver.
   *
   * @return JDBC version minor number
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return 0;
  }

  /**
   * Indicates whether the SQLSTATE returned by {@code SQLException.getSQLState}
   * is X/Open (now known as Open Group) SQL CLI or SQL:2003.
   *
   * @return the type of SQLSTATE; one of:
   * sqlStateXOpen or
   * sqlStateSQL
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public int getSQLStateType() throws SQLException {
    return DatabaseMetaData.sqlStateSQL99;
  }

  /**
   * Indicates whether updates made to a LOB are made on a copy or directly
   * to the LOB.
   *
   * @return {@code true} if updates are made to a copy of the LOB;
   * {@code false} if updates are made directly to the LOB
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    return false;
  }

  /**
   * Retrieves whether this database supports statement pooling.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public boolean supportsStatementPooling() throws SQLException {
    return false;
  }

  /**
   * Indicates whether this data source supports the SQL {@code  ROWID} type,
   * and the lifetime for which a {@link  RowId} object remains valid.
   *
   * @return the status indicating the lifetime of a {@code  RowId}
   * @throws SQLException if a database access error occurs
   * @since 1.6
   */
  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Retrieves the schema names available in this database.  The results
   * are ordered by {@code TABLE_CATALOG} and
   * {@code TABLE_SCHEM}.
   *
   * <P>The schema columns are:
   * <OL>
   * <LI><B>TABLE_SCHEM</B> String {@code =>} schema name
   * <LI><B>TABLE_CATALOG</B> String {@code =>} catalog name (may be {@code null})
   * </OL>
   *
   * @param catalog       a catalog name; must match the catalog name as it is stored
   *                      in the database;"" retrieves those without a catalog; null means catalog
   *                      name should not be used to narrow down the search.
   * @param schemaPattern a schema name; must match the schema name as it is
   *                      stored in the database; null means
   *                      schema name should not be used to narrow down the search.
   * @return a {@code ResultSet} object in which each row is a
   * schema description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   * @since 1.6
   */
  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    if (getSchemas == null) {
      getSchemas =
          connection.prepareStatement(
              "select null as TABLE_SCHEM, null as TABLE_CATALOG limit 0;");
    }

    return getSchemas.executeQuery();
  }

  /**
   * Retrieves whether this database supports invoking user-defined or vendor functions
   * using the stored procedure escape syntax.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.6
   */
  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Retrieves whether a {@code SQLException} while autoCommit is {@code true} indicates
   * that all open ResultSets are closed, even ones that are holdable.  When a {@code SQLException} occurs while
   * autocommit is {@code true}, it is vendor specific whether the JDBC driver responds with a commit operation, a
   * rollback operation, or by doing neither a commit nor a rollback.  A potential result of this difference
   * is in whether or not holdable ResultSets are closed.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.6
   */
  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  /**
   * Retrieves a list of the client info properties
   * that the driver supports.  The result set contains the following columns
   *
   * <ol>
   * <li><b>NAME</b> String{@code =>} The name of the client info property<br>
   * <li><b>MAX_LEN</b> int{@code =>} The maximum length of the value for the property<br>
   * <li><b>DEFAULT_VALUE</b> String{@code =>} The default value of the property<br>
   * <li><b>DESCRIPTION</b> String{@code =>} A description of the property.  This will typically
   *                                              contain information as to where this property is
   *                                              stored in the database.
   * </ol>
   * <p>
   * The {@code ResultSet} is sorted by the NAME column
   *
   * @return A {@code ResultSet} object; each row is a supported client info
   * property
   * @throws SQLException if a database access error occurs
   * @since 1.6
   */
  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Retrieves a description of the  system and user functions available
   * in the given catalog.
   * <p>
   * Only system and user function descriptions matching the schema and
   * function name criteria are returned.  They are ordered by
   * {@code FUNCTION_CAT}, {@code FUNCTION_SCHEM},
   * {@code FUNCTION_NAME} and
   * {@code SPECIFIC_ NAME}.
   *
   * <P>Each function description has the following columns:
   * <OL>
   * <LI><B>FUNCTION_CAT</B> String {@code =>} function catalog (may be {@code null})
   * <LI><B>FUNCTION_SCHEM</B> String {@code =>} function schema (may be {@code null})
   * <LI><B>FUNCTION_NAME</B> String {@code =>} function name.  This is the name
   * used to invoke the function
   * <LI><B>REMARKS</B> String {@code =>} explanatory comment on the function
   * <LI><B>FUNCTION_TYPE</B> short {@code =>} kind of function:
   * <UL>
   * <LI>functionResultUnknown - Cannot determine if a return value
   * or table will be returned
   * <LI> functionNoTable- Does not return a table
   * <LI> functionReturnsTable - Returns a table
   * </UL>
   * <LI><B>SPECIFIC_NAME</B> String  {@code =>} the name which uniquely identifies
   * this function within its schema.  This is a user specified, or DBMS
   * generated, name that may be different then the {@code FUNCTION_NAME}
   * for example with overload functions
   * </OL>
   * <p>
   * A user may not have permission to execute any of the functions that are
   * returned by {@code getFunctions}
   *
   * @param catalog             a catalog name; must match the catalog name as it
   *                            is stored in the database; "" retrieves those without a catalog;
   *                            {@code null} means that the catalog name should not be used to narrow
   *                            the search
   * @param schemaPattern       a schema name pattern; must match the schema name
   *                            as it is stored in the database; "" retrieves those without a schema;
   *                            {@code null} means that the schema name should not be used to narrow
   *                            the search
   * @param functionNamePattern a function name pattern; must match the
   *                            function name as it is stored in the database
   * @return {@code ResultSet} - each row is a function description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   * @since 1.6
   */
  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Retrieves a description of the given catalog's system or user
   * function parameters and return type.
   *
   * <P>Only descriptions matching the schema,  function and
   * parameter name criteria are returned. They are ordered by
   * {@code FUNCTION_CAT}, {@code FUNCTION_SCHEM},
   * {@code FUNCTION_NAME} and
   * {@code SPECIFIC_ NAME}. Within this, the return value,
   * if any, is first. Next are the parameter descriptions in call
   * order. The column descriptions follow in column number order.
   *
   * <P>Each row in the {@code ResultSet}
   * is a parameter description, column description or
   * return type description with the following fields:
   * <OL>
   * <LI><B>FUNCTION_CAT</B> String {@code =>} function catalog (may be {@code null})
   * <LI><B>FUNCTION_SCHEM</B> String {@code =>} function schema (may be {@code null})
   * <LI><B>FUNCTION_NAME</B> String {@code =>} function name.  This is the name
   * used to invoke the function
   * <LI><B>COLUMN_NAME</B> String {@code =>} column/parameter name
   * <LI><B>COLUMN_TYPE</B> Short {@code =>} kind of column/parameter:
   * <UL>
   * <LI> functionColumnUnknown - nobody knows
   * <LI> functionColumnIn - IN parameter
   * <LI> functionColumnInOut - INOUT parameter
   * <LI> functionColumnOut - OUT parameter
   * <LI> functionColumnReturn - function return value
   * <LI> functionColumnResult - Indicates that the parameter or column
   * is a column in the {@code ResultSet}
   * </UL>
   * <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
   * <LI><B>TYPE_NAME</B> String {@code =>} SQL type name, for a UDT type the
   * type name is fully qualified
   * <LI><B>PRECISION</B> int {@code =>} precision
   * <LI><B>LENGTH</B> int {@code =>} length in bytes of data
   * <LI><B>SCALE</B> short {@code =>} scale -  null is returned for data types where
   * SCALE is not applicable.
   * <LI><B>RADIX</B> short {@code =>} radix
   * <LI><B>NULLABLE</B> short {@code =>} can it contain NULL.
   * <UL>
   * <LI> functionNoNulls - does not allow NULL values
   * <LI> functionNullable - allows NULL values
   * <LI> functionNullableUnknown - nullability unknown
   * </UL>
   * <LI><B>REMARKS</B> String {@code =>} comment describing column/parameter
   * <LI><B>CHAR_OCTET_LENGTH</B> int  {@code =>} the maximum length of binary
   * and character based parameters or columns.  For any other datatype the returned value
   * is a NULL
   * <LI><B>ORDINAL_POSITION</B> int  {@code =>} the ordinal position, starting
   * from 1, for the input and output parameters. A value of 0
   * is returned if this row describes the function's return value.
   * For result set columns, it is the
   * ordinal position of the column in the result set starting from 1.
   * <LI><B>IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine
   * the nullability for a parameter or column.
   * <UL>
   * <LI> YES           --- if the parameter or column can include NULLs
   * <LI> NO            --- if the parameter or column  cannot include NULLs
   * <LI> empty string  --- if the nullability for the
   * parameter  or column is unknown
   * </UL>
   * <LI><B>SPECIFIC_NAME</B> String  {@code =>} the name which uniquely identifies
   * this function within its schema.  This is a user specified, or DBMS
   * generated, name that may be different then the {@code FUNCTION_NAME}
   * for example with overload functions
   * </OL>
   *
   * <p>The PRECISION column represents the specified column size for the given
   * parameter or column.
   * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
   * For datetime datatypes, this is the length in characters of the String representation (assuming the
   * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
   * this is the length in bytes. Null is returned for data types where the
   * column size is not applicable.
   *
   * @param catalog             a catalog name; must match the catalog name as it
   *                            is stored in the database; "" retrieves those without a catalog;
   *                            {@code null} means that the catalog name should not be used to narrow
   *                            the search
   * @param schemaPattern       a schema name pattern; must match the schema name
   *                            as it is stored in the database; "" retrieves those without a schema;
   *                            {@code null} means that the schema name should not be used to narrow
   *                            the search
   * @param functionNamePattern a procedure name pattern; must match the
   *                            function name as it is stored in the database
   * @param columnNamePattern   a parameter name pattern; must match the
   *                            parameter or column name as it is stored in the database
   * @return {@code ResultSet} - each row describes a
   * user function parameter, column  or return type
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   * @since 1.6
   */
  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Retrieves a description of the pseudo or hidden columns available
   * in a given table within the specified catalog and schema.
   * Pseudo or hidden columns may not always be stored within
   * a table and are not visible in a ResultSet unless they are
   * specified in the query's outermost SELECT list. Pseudo or hidden
   * columns may not necessarily be able to be modified. If there are
   * no pseudo or hidden columns, an empty ResultSet is returned.
   *
   * <P>Only column descriptions matching the catalog, schema, table
   * and column name criteria are returned.  They are ordered by
   * {@code TABLE_CAT},{@code TABLE_SCHEM}, {@code TABLE_NAME}
   * and {@code COLUMN_NAME}.
   *
   * <P>Each column description has the following columns:
   * <OL>
   * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be {@code null})
   * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be {@code null})
   * <LI><B>TABLE_NAME</B> String {@code =>} table name
   * <LI><B>COLUMN_NAME</B> String {@code =>} column name
   * <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
   * <LI><B>COLUMN_SIZE</B> int {@code =>} column size.
   * <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned for data types where
   * DECIMAL_DIGITS is not applicable.
   * <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
   * <LI><B>COLUMN_USAGE</B> String {@code =>} The allowed usage for the column.  The
   * value returned will correspond to the enum name returned by {@link PseudoColumnUsage#name PseudoColumnUsage.name()}
   * <LI><B>REMARKS</B> String {@code =>} comment describing column (may be {@code null})
   * <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the
   * maximum number of bytes in the column
   * <LI><B>IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine the nullability for a column.
   * <UL>
   * <LI> YES           --- if the column can include NULLs
   * <LI> NO            --- if the column cannot include NULLs
   * <LI> empty string  --- if the nullability for the column is unknown
   * </UL>
   * </OL>
   *
   * <p>The COLUMN_SIZE column specifies the column size for the given column.
   * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
   * For datetime datatypes, this is the length in characters of the String representation (assuming the
   * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
   * this is the length in bytes. Null is returned for data types where the
   * column size is not applicable.
   *
   * @param catalog           a catalog name; must match the catalog name as it
   *                          is stored in the database; "" retrieves those without a catalog;
   *                          {@code null} means that the catalog name should not be used to narrow
   *                          the search
   * @param schemaPattern     a schema name pattern; must match the schema name
   *                          as it is stored in the database; "" retrieves those without a schema;
   *                          {@code null} means that the schema name should not be used to narrow
   *                          the search
   * @param tableNamePattern  a table name pattern; must match the
   *                          table name as it is stored in the database
   * @param columnNamePattern a column name pattern; must match the column
   *                          name as it is stored in the database
   * @return {@code ResultSet} - each row is a column description
   * @throws SQLException if a database access error occurs
   * @see PseudoColumnUsage
   * @since 1.7
   */
  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Retrieves whether a generated key will always be returned if the column
   * name(s) or index(es) specified for the auto generated key column(s)
   * are valid and the statement succeeds.  The key that is returned may or
   * may not be based on the column(s) for the auto generated key.
   * Consult your JDBC driver documentation for additional details.
   *
   * @return {@code true} if so; {@code false} otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.7
   */
  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    throw new SQLFeatureNotSupportedException();
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

  // From SQLLite Driver
  protected String escape(final String val) {
    int len = val.length();
    StringBuilder buf = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      if (val.charAt(i) == '\'') {
        buf.append('\'');
      }
      buf.append(val.charAt(i));
    }
    return buf.toString();
  }

  protected static String quote(String tableName) {
    if (tableName == null) {
      return "null";
    } else {
      return String.format("'%s'", tableName);
    }
  }

  private String unquoteIdentifier(String name) {
    if (name == null) return name;
    name = name.trim();
    if (name.length() > 2
        && ((name.startsWith("`") && name.endsWith("`"))
        || (name.startsWith("\"") && name.endsWith("\""))
        || (name.startsWith("[") && name.endsWith("]")))) {
      // unquote to be consistent with column names returned by getColumns()
      name = name.substring(1, name.length() - 1);
    }
    return name;
  }

  /** Pattern used to extract column order for an unnamed primary key. */
  protected static final Pattern PK_UNNAMED_PATTERN =
      Pattern.compile(
          ".*PRIMARY\\s+KEY\\s*\\((.*?)\\).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /** Pattern used to extract a named primary key. */
  protected static final Pattern PK_NAMED_PATTERN =
      Pattern.compile(
          ".*CONSTRAINT\\s*(.*?)\\s*PRIMARY\\s+KEY\\s*\\((.*?)\\).*",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /** Parses the sqlite_schema table for a table's primary key */
  class PrimaryKeyFinder {
    /** The table name. */
    String table;

    /** The primary key name. */
    String pkName = null;

    /** The column(s) for the primary key. */
    String[] pkColumns = null;

    /**
     * Constructor.
     *
     * @param table The table for which to get find a primary key.
     * @throws SQLException
     */
    public PrimaryKeyFinder(String table) throws SQLException {
      this.table = table;

      // specific handling for sqlite_schema and synonyms, so that
      // getExportedKeys/getPrimaryKeys return an empty ResultSet instead of throwing an
      // exception
      if ("sqlite_schema".equals(table) || "sqlite_master".equals(table)) return;

      if (table == null || table.trim().length() == 0) {
        throw new SQLException("Invalid table name: '" + this.table + "'");
      }

      try (Statement stat = connection.createStatement();
           // read create SQL script for table
           ResultSet rs =
               stat.executeQuery(
                   "select sql from sqlite_schema where"
                       + " lower(name) = lower('"
                       + escape(table)
                       + "') and type in ('table', 'view')")) {

        if (!rs.next()) throw new SQLException("Table not found: '" + table + "'");

        Matcher matcher = PK_NAMED_PATTERN.matcher(rs.getString(1));
        if (matcher.find()) {
          pkName = unquoteIdentifier(escape(matcher.group(1)));
          pkColumns = matcher.group(2).split(",");
        } else {
          matcher = PK_UNNAMED_PATTERN.matcher(rs.getString(1));
          if (matcher.find()) {
            pkColumns = matcher.group(1).split(",");
          }
        }

        if (pkColumns == null) {
          try (ResultSet rs2 =
                   stat.executeQuery("pragma table_info('" + escape(table) + "');")) {
            while (rs2.next()) {
              if (rs2.getBoolean(6)) pkColumns = new String[] {rs2.getString(2)};
            }
          }
        }

        if (pkColumns != null) {
          for (int i = 0; i < pkColumns.length; i++) {
            pkColumns[i] = unquoteIdentifier(pkColumns[i]);
          }
        }
      }
    }

    /** @return The primary key name if any. */
    public String getName() {
      return pkName;
    }

    /** @return Array of primary key column(s) if any. */
    public String[] getColumns() {
      return pkColumns;
    }
  }

  private StringBuilder appendDummyForeignKeyList(StringBuilder sql) {
    sql.append("select -1 as ks, '' as ptn, '' as fcn, '' as pcn, ")
        .append(DatabaseMetaData.importedKeyNoAction)
        .append(" as ur, ")
        .append(DatabaseMetaData.importedKeyNoAction)
        .append(" as dr, ")
        .append(" '' as fkn, ")
        .append(" '' as pkn ")
        .append(") limit 0;");
    return sql;
  }

  class ImportedKeyFinder {

    /** Pattern used to extract a named primary key. */
    private final Pattern FK_NAMED_PATTERN =
        Pattern.compile(
            "CONSTRAINT\\s*\"?([A-Za-z_][A-Za-z\\d_]*)?\"?\\s*FOREIGN\\s+KEY\\s*\\((.*?)\\)",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private final String fkTableName;
    private final List<ForeignKey> fkList = new ArrayList<>();

    public ImportedKeyFinder(String table) throws SQLException {

      if (table == null || table.trim().length() == 0) {
        throw new SQLException("Invalid table name: '" + table + "'");
      }

      this.fkTableName = table;

      List<String> fkNames = getForeignKeyNames(this.fkTableName);

      try (Statement stat = connection.createStatement();
           ResultSet rs =
               stat.executeQuery(
                   "pragma foreign_key_list('"
                       + escape(this.fkTableName.toLowerCase())
                       + "')")) {

        int prevFkId = -1;
        int count = 0;
        ForeignKey fk = null;
        while (rs.next()) {
          int fkId = rs.getInt(1);
          String pkTableName = rs.getString(3);
          String fkColName = rs.getString(4);
          String pkColName = rs.getString(5);
          String onUpdate = rs.getString(6);
          String onDelete = rs.getString(7);
          String match = rs.getString(8);

          String fkName = null;
          if (fkNames.size() > count) fkName = fkNames.get(count);

          if (fkId != prevFkId) {
            fk =
                new ForeignKey(
                    fkName,
                    pkTableName,
                    fkTableName,
                    onUpdate,
                    onDelete,
                    match);
            fkList.add(fk);
            prevFkId = fkId;
            count++;
          }
          if (fk != null) {
            fk.addColumnMapping(fkColName, pkColName);
          }
        }
      }
    }

    private List<String> getForeignKeyNames(String tbl) throws SQLException {
      List<String> fkNames = new ArrayList<>();
      if (tbl == null) {
        return fkNames;
      }
      try (Statement stat2 = connection.createStatement();
           ResultSet rs =
               stat2.executeQuery(
                   "select sql from sqlite_schema where"
                       + " lower(name) = lower('"
                       + escape(tbl)
                       + "')")) {

        if (rs.next()) {
          Matcher matcher = FK_NAMED_PATTERN.matcher(rs.getString(1));

          while (matcher.find()) {
            fkNames.add(matcher.group(1));
          }
        }
      }
      Collections.reverse(fkNames);
      return fkNames;
    }

    public String getFkTableName() {
      return fkTableName;
    }

    public List<ForeignKey> getFkList() {
      return fkList;
    }

    class ForeignKey {

      private final String fkName;
      private final String pkTableName;
      private final String fkTableName;
      private final List<String> fkColNames = new ArrayList<>();
      private final List<String> pkColNames = new ArrayList<>();
      private final String onUpdate;
      private final String onDelete;
      private final String match;

      ForeignKey(
          String fkName,
          String pkTableName,
          String fkTableName,
          String onUpdate,
          String onDelete,
          String match) {
        this.fkName = fkName;
        this.pkTableName = pkTableName;
        this.fkTableName = fkTableName;
        this.onUpdate = onUpdate;
        this.onDelete = onDelete;
        this.match = match;
      }

      public String getFkName() {
        return fkName;
      }

      void addColumnMapping(String fkColName, String pkColName) {
        fkColNames.add(fkColName);
        pkColNames.add(pkColName);
      }

      public String[] getColumnMapping(int colSeq) {
        return new String[] {fkColNames.get(colSeq), pkColNames.get(colSeq)};
      }

      public int getColumnMappingCount() {
        return fkColNames.size();
      }

      public String getPkTableName() {
        return pkTableName;
      }

      public String getFkTableName() {
        return fkTableName;
      }

      public String getOnUpdate() {
        return onUpdate;
      }

      public String getOnDelete() {
        return onDelete;
      }

      public String getMatch() {
        return match;
      }

      @Override
      public String toString() {
        return "ForeignKey [fkName="
            + fkName
            + ", pkTableName="
            + pkTableName
            + ", fkTableName="
            + fkTableName
            + ", pkColNames="
            + pkColNames
            + ", fkColNames="
            + fkColNames
            + "]";
      }
    }
  }

  public static String valuesQuery(List<String> columns, List<List<Object>> valuesList) {
    valuesList.forEach(
        (list) -> {
          if (list.size() != columns.size())
            throw new IllegalArgumentException(
                "values and columns must have the same size");
        });
    return "with cte("
        + String.join(",", columns)
        + ") as (values "
        + valuesList.stream()
        .map(
            (values) ->
                "("
                    + values.stream()
                    .map(
                        (o -> {
                          if (o instanceof String)
                            return "'" + o + "'";
                          if (o == null) return "null";
                          return o.toString();
                        }))
                    .collect(Collectors.joining(","))
                    + ")")
        .collect(Collectors.joining(","))
        + ") select * from cte";
  }
}
