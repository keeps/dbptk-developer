package com.databasepreservation.modules.sqlServer.in;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.modules.SQLUtils;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.sqlServer.SQLServerHelper;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;

/**
 * Microsoft SQL Server JDBC import module.
 *
 * @author Luis Faria
 */
public class SQLServerJDBCImportModule extends JDBCImportModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLServerJDBCImportModule.class);

  /**
   * Create a new Microsoft SQL Server import module using the default instance.
   *
   * @param serverName
   *          the name (host name) of the server
   * @param database
   *          the name of the database we'll be accessing
   * @param username
   *          the name of the user to use in the connection
   * @param password
   *          the password of the user to use in the connection
   * @param integratedSecurity
   *          true to use windows login, false to use SQL Server login
   * @param encrypt
   *          true to use encryption in the connection
   */
  public SQLServerJDBCImportModule(String serverName, String database, String username, String password,
    boolean integratedSecurity, boolean encrypt) {
    super("com.microsoft.sqlserver.jdbc.SQLServerDriver", new StringBuilder("jdbc:sqlserver://").append(serverName)
      .append(";database=").append(database).append(";user=").append(username).append(";password=").append(password)
      .append(";integratedSecurity=").append(integratedSecurity).append(";encrypt=").append(encrypt).toString(),
      new SQLServerHelper(), new SQLServerDatatypeImporter());
  }

  /**
   * Create a new Microsoft SQL Server import module using the instance name.
   * The constructor using the port number is preferred over this to avoid a
   * round-trip to the server to discover the instance port number.
   *
   * @param serverName
   *          the name (host name) of the server
   * @param instanceName
   *          the name of the instance
   * @param database
   *          the name of the database we'll be accessing
   * @param username
   *          the name of the user to use in the connection
   * @param password
   *          the password of the user to use in the connection
   * @param integratedSecurity
   *          true to use windows login, false to use SQL Server login
   * @param encrypt
   *          true to use encryption in the connection
   */
  public SQLServerJDBCImportModule(String serverName, String instanceName, String database, String username,
    String password, boolean integratedSecurity, boolean encrypt) {
    super("com.microsoft.sqlserver.jdbc.SQLServerDriver", new StringBuilder("jdbc:sqlserver://").append(serverName)
      .append("\\").append(instanceName).append(";database=").append(database).append(";user=").append(username)
      .append(";password=").append(password).append(";integratedSecurity=").append(integratedSecurity)
      .append(";encrypt=").append(encrypt).toString(), new SQLServerHelper(), new SQLServerDatatypeImporter());
  }

  /**
   * Create a new Microsoft SQL Server import module using the port number.
   *
   * @param serverName
   *          the name (host name) of the server
   * @param portNumber
   *          the port number of the server instance, default is 1433
   * @param database
   *          the name of the database we'll be accessing
   * @param username
   *          the name of the user to use in the connection
   * @param password
   *          the password of the user to use in the connection
   * @param integratedSecurity
   *          true to use windows login, false to use SQL Server login
   * @param encrypt
   *          true to use encryption in the connection
   */
  public SQLServerJDBCImportModule(String serverName, int portNumber, String database, String username,
    String password, boolean integratedSecurity, boolean encrypt) {
    super("com.microsoft.sqlserver.jdbc.SQLServerDriver", new StringBuilder("jdbc:sqlserver://").append(serverName)
      .append(":").append(portNumber).append(";database=").append(database).append(";user=").append(username)
      .append(";password=").append(password).append(";integratedSecurity=").append(integratedSecurity)
      .append(";encrypt=").append(encrypt).toString(), new SQLServerHelper(), new SQLServerDatatypeImporter());
  }

  @Override
  protected Statement getStatement() throws SQLException {
    if (statement == null) {
      statement = ((SQLServerConnection) getConnection()).createStatement(SQLServerResultSet.TYPE_FORWARD_ONLY,
        SQLServerResultSet.CONCUR_READ_ONLY);

      SQLServerStatement sqlServerStatement = statement.unwrap(com.microsoft.sqlserver.jdbc.SQLServerStatement.class);
      sqlServerStatement.setResponseBuffering("adaptive");

      statement = sqlServerStatement;
    }
    return statement;
  }

  @Override
  protected Set<String> getIgnoredImportedSchemas() {
    Set<String> ignored = new HashSet<String>();
    ignored.add("db_.*");
    ignored.add("sys");
    ignored.add("INFORMATION_SCHEMA");
    ignored.add("guest");
    return ignored;
  }

  @Override
  protected String processTriggerEvent(String string) {
    LOGGER.debug("Trigger event: " + string);
    char[] charArray = string.toCharArray();

    String res = "";
    if (charArray.length > 0 && charArray[0] == '1') {
      res = "INSERT";
    }

    if (charArray.length > 1 && charArray[1] == '1') {
      if (!"".equals(res)) {
        res += " OR ";
      }
      res = "UPDATE";
    }

    if (charArray.length > 2 && charArray[2] == '1') {
      if (!"".equals(res)) {
        res += " OR ";
      }
      res = "DELETE";
    }
    return res;
  }

  @Override
  protected String processActionTime(String string) {
    LOGGER.debug("Trigger action time: " + string);
    char[] charArray = string.toCharArray();

    String res = "";
    if (charArray.length > 0 && charArray[0] == '1') {
      res = "AFTER";
    }
    if (charArray.length > 1 && charArray[1] == '1') {
      if (!"".equals(res)) {
        res += " OR ";
      }
      res = "INSTEAD OF";
    }

    // note that SQL Server does not support BEFORE triggers

    return res;
  }

  @Override
  protected List<ViewStructure> getViews(String schemaName) throws SQLException {
    final String fieldName = "objdefinition";
    final String defaultValue = "unknown";

    List<ViewStructure> views = super.getViews(schemaName);
    for (ViewStructure v : views) {
      String originalQuery = null;
      ResultSet rset = null;
      PreparedStatement statement = getConnection().prepareStatement(
        "SELECT OBJECT_DEFINITION (OBJECT_ID(" + sqlHelper.escapeViewName(schemaName, v.getName()) + ")) AS ?");
      statement.setString(1, fieldName);

      try {
        // https://technet.microsoft.com/en-us/library/ms175067.aspx
        rset = statement.executeQuery();
        rset.next();
        originalQuery = rset.getString(fieldName);
      } catch (Exception e) {
        LOGGER.debug("Exception trying to get view SQL in SQL Server (method #1)", e);
      } finally {
        SQLUtils.closeQuietly(rset);
        SQLUtils.closeQuietly(statement);
      }

      try {
        // https://technet.microsoft.com/en-us/library/ms175067.aspx
        statement = getConnection().prepareStatement(
          "SELECT ? FROM sys.sql_modules WHERE object_id = OBJECT_ID("
            + sqlHelper.escapeViewName(schemaName, v.getName()) + ")");
        statement.setString(1, fieldName);
        rset = statement.executeQuery();
        rset.next();
        originalQuery = rset.getString(fieldName);
      } catch (Exception e) {
        LOGGER.debug("Exception trying to get view SQL in SQL Server (method #2)", e);
      } finally {
        SQLUtils.closeQuietly(rset);
        SQLUtils.closeQuietly(statement);
      }

      if (StringUtils.isBlank(originalQuery)) {
        originalQuery = defaultValue;
        Reporter.customMessage("SQLServerJDBCImportModule",
          "Could not obtain SQL statement for view " + sqlHelper.escapeViewName(schemaName, v.getName()));
      }

      v.setQueryOriginal(originalQuery);
    }
    return views;
  }
}
