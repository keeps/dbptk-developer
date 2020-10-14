/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.sqlserver.in;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.RoutineStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.modules.CloseableUtils;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.sqlserver.SQLServerExceptionNormalizer;
import com.databasepreservation.modules.sqlserver.SQLServerHelper;
import com.databasepreservation.modules.sqlserver.SQLServerJDBCModuleFactory;
import com.databasepreservation.utils.MapUtils;
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
  private static final String SCHEMA = "Schema";

  /**
   * Create a new Microsoft SQL Server import module using the instance name. The
   * constructor using the port number is preferred over this to avoid a
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
  public SQLServerJDBCImportModule(String moduleName, String serverName, int portNumber, String instanceName,
    String database, String username, String password, boolean integratedSecurity, boolean encrypt, boolean ssh,
    String sshHost, String sshUser, String sshPassword, String sshPortNumber) throws ModuleException {
    super("com.microsoft.sqlserver.jdbc.SQLServerDriver",
      "jdbc:sqlserver://" + serverName + "\\" + instanceName + ":" + portNumber + ";database=" + database
        + ";integratedSecurity=" + integratedSecurity + ";encrypt=" + encrypt,
      new SQLServerHelper(), new SQLServerDatatypeImporter(), moduleName,
      MapUtils.buildMapFromObjects(SQLServerJDBCModuleFactory.PARAMETER_SERVER_NAME, serverName,
        SQLServerJDBCModuleFactory.PARAMETER_PORT_NUMBER, portNumber,
        SQLServerJDBCModuleFactory.PARAMETER_INSTANCE_NAME, instanceName,
        SQLServerJDBCModuleFactory.PARAMETER_USE_INTEGRATED_LOGIN, integratedSecurity,
        SQLServerJDBCModuleFactory.PARAMETER_USERNAME, username, SQLServerJDBCModuleFactory.PARAMETER_PASSWORD,
        password, SQLServerJDBCModuleFactory.PARAMETER_DATABASE, database,
        SQLServerJDBCModuleFactory.PARAMETER_DISABLE_ENCRYPTION, !encrypt),
      MapUtils.buildMapFromObjectsIf(ssh, SQLServerJDBCModuleFactory.PARAMETER_SSH_HOST, sshHost,
        SQLServerJDBCModuleFactory.PARAMETER_SSH_PORT, sshPortNumber, SQLServerJDBCModuleFactory.PARAMETER_SSH_USER,
        sshUser, SQLServerJDBCModuleFactory.PARAMETER_SSH_PASSWORD, sshPassword));

    createCredentialsProperty(username, password);
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
  public SQLServerJDBCImportModule(String moduleName, String serverName, int portNumber, String database,
    String username, String password, boolean integratedSecurity, boolean encrypt, boolean ssh, String sshHost,
    String sshUser, String sshPassword, String sshPortNumber) throws ModuleException {
    super("com.microsoft.sqlserver.jdbc.SQLServerDriver",
      "jdbc:sqlserver://" + serverName + ":" + portNumber + ";database=" + database + ";integratedSecurity="
        + integratedSecurity + ";encrypt=" + encrypt,
      new SQLServerHelper(), new SQLServerDatatypeImporter(), moduleName,
      MapUtils.buildMapFromObjects(SQLServerJDBCModuleFactory.PARAMETER_SERVER_NAME, serverName,
        SQLServerJDBCModuleFactory.PARAMETER_PORT_NUMBER, portNumber,
        SQLServerJDBCModuleFactory.PARAMETER_USE_INTEGRATED_LOGIN, integratedSecurity,
        SQLServerJDBCModuleFactory.PARAMETER_USERNAME, username, SQLServerJDBCModuleFactory.PARAMETER_PASSWORD,
        password, SQLServerJDBCModuleFactory.PARAMETER_DATABASE, database,
        SQLServerJDBCModuleFactory.PARAMETER_DISABLE_ENCRYPTION, !encrypt),
      MapUtils.buildMapFromObjectsIf(ssh, SQLServerJDBCModuleFactory.PARAMETER_SSH_HOST, sshHost,
        SQLServerJDBCModuleFactory.PARAMETER_SSH_PORT, sshPortNumber, SQLServerJDBCModuleFactory.PARAMETER_SSH_USER,
        sshUser, SQLServerJDBCModuleFactory.PARAMETER_SSH_PASSWORD, sshPassword));

    createCredentialsProperty(username, password);
  }

  @Override
  protected Statement getStatement() throws SQLException, ModuleException {
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
    Set<String> ignored = new HashSet<>();
    ignored.add("db_.*");
    ignored.add("sys");
    ignored.add("INFORMATION_SCHEMA");
    ignored.add("guest");
    return ignored;
  }

  @Override
  protected String processTriggerEvent(String string) {
    LOGGER.debug("Trigger event: {}", string);
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
    LOGGER.debug("Trigger action time: {}", string);
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
  protected List<ViewStructure> getViews(String schemaName) throws SQLException, ModuleException {
    final String fieldName = "objdefinition";
    final String defaultValue = "unknown";

    List<ViewStructure> views = super.getViews(schemaName);
    for (ViewStructure v : views) {
      String originalQuery = null;
      ResultSet rset = null;
      PreparedStatement statement = null;
      statement = getConnection().prepareStatement("SELECT OBJECT_DEFINITION (OBJECT_ID('"
        + sqlHelper.escapeViewName(schemaName, v.getName()) + "')) AS objdefinition");

      try {
        // https://technet.microsoft.com/en-us/library/ms175067.aspx
        rset = statement.executeQuery();
        rset.next();
        originalQuery = rset.getString(fieldName);
      } catch (Exception e) {
        LOGGER.debug("Exception trying to get view SQL in SQL Server (method #1)", e);
      } finally {
        CloseableUtils.closeQuietly(rset);
        CloseableUtils.closeQuietly(statement);
      }

      try {
        // https://technet.microsoft.com/en-us/library/ms175067.aspx
        statement = getConnection()
          .prepareStatement("SELECT definition AS objdefinition FROM sys.sql_modules WHERE object_id = OBJECT_ID('"
            + sqlHelper.escapeViewName(schemaName, v.getName()) + "')");
        rset = statement.executeQuery();
        rset.next();
        originalQuery = rset.getString(fieldName);
      } catch (Exception e) {
        LOGGER.debug("Exception trying to get view SQL in SQL Server (method #2)", e);
      } finally {
        CloseableUtils.closeQuietly(rset);
        CloseableUtils.closeQuietly(statement);
      }

      if (StringUtils.isBlank(originalQuery)) {
        originalQuery = defaultValue;
        reporter.customMessage("SQLServerJDBCImportModule",
          "Could not obtain SQL statement for view " + sqlHelper.escapeViewName(schemaName, v.getName()));
      }

      v.setQueryOriginal(originalQuery);
    }
    return views;
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    ModuleException moduleException = SQLServerExceptionNormalizer.getInstance().normalizeException(exception,
      contextMessage);

    // in case the exception normalizer could not handle this exception
    if (moduleException == null) {
      moduleException = super.normalizeException(exception, contextMessage);
    }

    return moduleException;
  }

  private String getDescriptionForDatabase() throws ModuleException {
    try {
      return getDescription(null, null, null, null, null, null);
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "database");
    }
    return null;
  }

  private String getDescriptionForSchema(String schema) throws ModuleException {
    try {
      return getDescription(SCHEMA, schema, null, null, null, null);
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "schema", schema);
    }
    return null;
  }

  private String getDescriptionForTable(String schema, String table) throws ModuleException {
    try {
      return getDescription(SCHEMA, schema, "Table", table, null, null);
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "table", schema, table);
    }
    return null;
  }

  private String getDescriptionForColumn(String schema, String table, String column) throws ModuleException {
    try {
      return getDescription(SCHEMA, schema, "Table", table, "Column", column);
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "column", schema, table, column);
    }
    return null;
  }

  private String getDescription(String scope1, String value1, String scope2, String value2, String scope3,
    String value3) throws ModuleException, SQLException {

    String description = null;

    // example: SELECT * FROM ::fn_listextendedproperty ('MS_Description', 'Schema',
    // 'dbo', 'Table', 'spt_monitor', NULL, NULL)

    try (PreparedStatement statement = getConnection().prepareStatement(
      "SELECT CONVERT(varchar, value) As 'description' FROM ::fn_listextendedproperty ('MS_Description', ?, ?, ?, ?, ?, ?)")) {

      statement.setString(1, scope1);
      statement.setString(2, value1);
      statement.setString(3, scope2);
      statement.setString(4, value2);
      statement.setString(5, scope3);
      statement.setString(6, value3);
      statement.execute();

      try (ResultSet rs = statement.getResultSet()) {
        if (rs.next()) {
          description = rs.getString(1);
        }
      }
    }

    return description;
  }

  @Override
  protected List<SchemaStructure> getSchemas() throws SQLException, ModuleException {
    getDatabaseStructure().setDescription(getDescriptionForDatabase());
    return super.getSchemas();
  }

  @Override
  protected SchemaStructure getSchemaStructure(String schemaName, int schemaIndex)
    throws SQLException, ModuleException {
    SchemaStructure schemaStructure = super.getSchemaStructure(schemaName, schemaIndex);
    schemaStructure.setDescription(getDescriptionForSchema(schemaName));
    return schemaStructure;
  }

  @Override
  protected TableStructure getTableStructure(SchemaStructure schema, String tableName, int tableIndex,
    String description, boolean view) throws SQLException, ModuleException {
    TableStructure tableStructure = super.getTableStructure(schema, tableName, tableIndex, description, view);
    tableStructure.setDescription(getDescriptionForTable(schema.getName(), tableName));
    return tableStructure;
  }

  @Override
  protected List<ColumnStructure> getColumns(String schemaName, String tableName) throws SQLException, ModuleException {
    List<ColumnStructure> columns = super.getColumns(schemaName, tableName);
    for (ColumnStructure column : columns) {
      column.setDescription(getDescriptionForColumn(schemaName, tableName, column.getName()));
    }
    return columns;
  }

  @Override
  protected List<RoutineStructure> getRoutines(String schemaName) throws SQLException, ModuleException {
    List<RoutineStructure> routines = new ArrayList<>();

    try (ResultSet rset = getMetadata().getProcedures(dbStructure.getName(), schemaName, "%")) {
      while (rset.next()) {
        String routineName = rset.getString(3);
        LOGGER.info("Obtaining routine {}", routineName);
        RoutineStructure routine = new RoutineStructure();
        routine.setName(routineName);
        if (rset.getString(7) != null) {
          routine.setDescription(rset.getString(7));
        } else {
          if (rset.getShort(8) == 1) {
            routine.setDescription("Routine does not " + "return a result");
          } else if (rset.getShort(8) == 2) {
            routine.setDescription("Routine returns a result");
          }
        }

        try {
          routineName = routineName.replaceAll(";[0-9]+$", "");
          try (ResultSet resultSet = getStatement().executeQuery("SELECT ROUTINE_DEFINITION FROM " + getDatabaseName()
            + ".information_schema.routines WHERE specific_name='" + routineName + "'")) {
            if (resultSet.next()) {
              routine.setBody(resultSet.getString("ROUTINE_DEFINITION"));
            }
          }
        } catch (SQLException e) {
          LOGGER.debug("Could not retrieve routine code (as routine).", e);
        }
        routines.add(routine);
      }
    }
    return routines;
  }
}
