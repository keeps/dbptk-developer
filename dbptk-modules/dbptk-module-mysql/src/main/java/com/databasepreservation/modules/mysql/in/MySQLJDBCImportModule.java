/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
/**
 *
 */
package com.databasepreservation.modules.mysql.in;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.CheckConstraint;
import com.databasepreservation.model.structure.RoutineStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.UserStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.CloseableUtils;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.mysql.MySQLExceptionNormalizer;
import com.databasepreservation.modules.mysql.MySQLHelper;
import com.databasepreservation.modules.mysql.MySQLModuleFactory;
import com.databasepreservation.utils.MapUtils;

import static com.databasepreservation.modules.mysql.Constants.MYSQL_DRIVER_CLASS_NAME;

/**
 * @author Luis Faria <lfaria@keep.pt>
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class MySQLJDBCImportModule extends JDBCImportModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(MySQLJDBCImportModule.class);
  private final String username;

  /**
   * MySQL JDBC import module constructor
   *
   * @param hostname
   *          the hostname of the MySQL server
   * @param port
   *          the port that the MySQL server is listening
   * @param database
   *          the name of the database to import from
   * @param username
   *          the name of the user to use in connection
   * @param password
   *          the password of the user to use in connection
   * @param encrypt
   *          encrypt connection
   */
  public MySQLJDBCImportModule(String moduleName, String hostname, int port, String database, String username,
    String password, boolean encrypt) {
    super(MYSQL_DRIVER_CLASS_NAME,
      "jdbc:mysql://" + hostname + ":" + port + "/" + database + (encrypt ? "?useSSL=true" : ""), new MySQLHelper(),
      new MySQLDatatypeImporter(), moduleName,
      MapUtils.buildMapFromObjects(MySQLModuleFactory.PARAMETER_HOSTNAME, hostname,
        MySQLModuleFactory.PARAMETER_PORT_NUMBER, port, MySQLModuleFactory.PARAMETER_USERNAME, username,
        MySQLModuleFactory.PARAMETER_PASSWORD, password, MySQLModuleFactory.PARAMETER_DATABASE, database,
        MySQLModuleFactory.PARAMETER_DISABLE_ENCRYPTION, !encrypt));
    this.username = username;

    createCredentialsProperty(username, password);
  }

  public MySQLJDBCImportModule(String moduleName, String hostname, int port, String database, String username,
    String password, boolean encrypt, String sshHost, String sshUser, String sshPassword, String sshPortNumber)
    throws ModuleException {
    super(MYSQL_DRIVER_CLASS_NAME,
      "jdbc:mysql://" + hostname + ":" + port + "/" + database + (encrypt ? "?useSSL=true" : ""), new MySQLHelper(),
      new MySQLDatatypeImporter(), moduleName,
      MapUtils.buildMapFromObjects(MySQLModuleFactory.PARAMETER_HOSTNAME, hostname,
        MySQLModuleFactory.PARAMETER_PORT_NUMBER, port, MySQLModuleFactory.PARAMETER_USERNAME, username,
        MySQLModuleFactory.PARAMETER_PASSWORD, password, MySQLModuleFactory.PARAMETER_DATABASE, database,
        MySQLModuleFactory.PARAMETER_DISABLE_ENCRYPTION, !encrypt),
      MapUtils.buildMapFromObjects(MySQLModuleFactory.PARAMETER_SSH, true, MySQLModuleFactory.PARAMETER_SSH_HOST,
        sshHost, MySQLModuleFactory.PARAMETER_SSH_PORT, sshPortNumber, MySQLModuleFactory.PARAMETER_SSH_USER, sshUser,
        MySQLModuleFactory.PARAMETER_SSH_PASSWORD, sshPassword));
    this.username = username;

    createCredentialsProperty(username, password);
  }

  @Override
  protected boolean isGetRowAvailable() {
    return false;
  }

  @Override
  protected List<SchemaStructure> getSchemas() throws SQLException, ModuleException {
    List<SchemaStructure> schemas = new ArrayList<>();
    String schemaName = getConnection().getCatalog();
    schemas.add(getSchemaStructure(schemaName, 1));
    return schemas;
  }

  @Override
  protected String getReferencedSchema(String s) throws SQLException, ModuleException {
    return (s == null) ? getConnection().getCatalog() : s;
  }

  @Override
  protected List<UserStructure> getUsers(String databaseName) throws SQLException, ModuleException {
    List<UserStructure> users = new ArrayList<>();

    String query = sqlHelper.getUsersSQL(null);
    Statement statement = getStatement();
    if (query != null) {
      ResultSet rs = null;

      try {
        rs = statement.executeQuery(sqlHelper.getUsersSQL(null));

        while (rs.next()) {
          UserStructure user = new UserStructure(rs.getString(2) + "@" + rs.getString(1), null);
          users.add(user);
        }
      } catch (SQLException e) {
        if (e.getMessage().startsWith("SELECT command denied to user ")
          && e.getMessage().endsWith(" for table 'user'")) {
          LOGGER.warn(
            "The selected MySQL user does not have permissions to list database users. This permission can be granted with the command \"GRANT SELECT ON mysql.user TO 'username'@'localhost' IDENTIFIED BY 'password';\"");
        } else {
          LOGGER.error("It was not possible to retrieve the list of database users.", e);
        }
      } finally {
        CloseableUtils.closeQuietly(rs);
      }
    }

    if (users.isEmpty()) {
      users.add(new UserStructure(username, ""));
      LOGGER.warn("Users were not imported. '{}' will be set as the user name.", username);
    }

    return users;
  }

  /**
   * @param schema
   * @param tableName
   *          the name of the table
   * @param tableIndex
   * @return the table structure
   * @throws SQLException
   * @throws ModuleException
   */
  @Override
  protected TableStructure getTableStructure(SchemaStructure schema, String tableName, int tableIndex,
    String description, boolean view) throws SQLException, ModuleException {
    TableStructure tableStructure = super.getTableStructure(schema, tableName, tableIndex, description, view);

    // obtain mysql remarks/comments (unsupported by the mysql driver up to
    // 5.1.38)
    if (StringUtils.isBlank(tableStructure.getDescription())) {
      String query = "SELECT TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = ? AND TABLE_NAME = ?";
      PreparedStatement preparedStatement = getConnection().prepareStatement(query);
      preparedStatement.setString(1, schema.getName());
      preparedStatement.setString(2, tableName);
      try {
        boolean gotResultSet = preparedStatement.execute();
        if (gotResultSet) {
          try (ResultSet rs = preparedStatement.getResultSet()) {
            if (rs.next()) {
              String tableComment = rs.getString(1);
              tableStructure.setDescription(tableComment);
            }
          }
        }
      } catch (Exception e) {
        LOGGER.debug("Exception while trying to obtain MySQL table '{}' description (comment). with query ", tableIndex,
          e);
      }
    }

    return tableStructure;
  }

  @Override
  protected ResultSet getTableRawData(String query, String tableId) throws SQLException, ModuleException {
    Statement st = getStatement();
    st.setFetchSize(Integer.MIN_VALUE);
    return st.executeQuery(query);
  }

  @Override
  protected boolean resultSetNext(ResultSet tableResultSet) throws ModuleException {
    try {
      return tableResultSet.next();
    } catch (SQLException e) {
      LOGGER.debug("Exception trying to get fetch size", e);
      // MySQL uses Integer.MIN_VALUE which is a special value meaning data
      // should be streamed.
      LOGGER.debug("MySQL fetch size is set to 'streaming'. It will not be adjusted further.");
      return false;
    }
  }

  @Override
  protected Statement getStatement() throws SQLException, ModuleException {
    if (statement == null) {
      statement = getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }
    return statement;
  }

  @Override
  protected List<ViewStructure> getViews(String schemaName) throws SQLException, ModuleException {
    List<ViewStructure> views = super.getViews(schemaName);
    for (ViewStructure v : views) {
      Statement statement = null;
      ResultSet rset = null;
      if (v.getQueryOriginal() == null || v.getQueryOriginal().isEmpty()) {
        try {
          statement = getConnection().createStatement();
          String query = "SHOW CREATE VIEW " + sqlHelper.escapeViewName(v.getName());
          rset = statement.executeQuery(query);
          rset.next(); // Returns only one tuple

          v.setQueryOriginal(rset.getString(2));
        } finally {
          CloseableUtils.closeQuietly(rset);
          CloseableUtils.closeQuietly(statement);
        }
      }
    }
    return views;
  }

  @Override
  protected Cell rawToCellSimpleTypeNumericExact(String id, String columnName, Type cellType, ResultSet rawData)
    throws SQLException, ModuleException {
    if ("YEAR".equals(cellType.getOriginalTypeName())) {
      // for inputs 15, 2015, 99 and 1999
      // rawData.getInt returns numbers like 15, 2015, 99, 1999
      // rawData.getString returns dates like 2015-01-01, 2015-01-01,
      // 1999-01-01, 1999-01-01
      // to get the "real" year value, using the first 4 characters from the
      // date string
      String data = rawData.getString(columnName);
      if (data != null) {
        return new SimpleCell(id, data.substring(0, 4));
      } else {
        return new NullCell(id);
      }
    } else {
      return super.rawToCellSimpleTypeNumericExact(id, columnName, cellType, rawData);
    }
  }

  /**
   * Gets the check constraints of a given schema table
   *
   * @param schemaName
   * @param tableName
   * @return
   */
  @Override
  protected List<CheckConstraint> getCheckConstraints(String schemaName, String tableName) {
    reporter.notYetSupported("check constraints", "MySQL");
    return new ArrayList<>();
  }

  /**
   * @param schemaName
   * @return
   * @throws SQLException
   */
  @Override
  protected List<RoutineStructure> getRoutines(String schemaName) throws SQLException, ModuleException {
    // TODO add optional fields to routine (use getProcedureColumns)
    List<RoutineStructure> routines = new ArrayList<>();

    ResultSet rset = getMetadata().getProcedures(dbStructure.getName(), schemaName, "%");
    while (rset.next()) {
      String routineName = rset.getString(3);
      LOGGER.info("Obtaining routine {}", routineName);
      RoutineStructure routine = new RoutineStructure();
      routine.setName(routineName);
      if (rset.getString(7) != null) {
        routine.setDescription(rset.getString(7));
      } else {
        if (rset.getShort(8) == 1) {
          routine.setDescription("Routine does not return a result");
        } else if (rset.getShort(8) == 2) {
          routine.setDescription("Routine returns a result");
        }
      }

      try (ResultSet resultSet = getStatement()
        .executeQuery("SHOW CREATE PROCEDURE " + sqlHelper.escapeTableName(routineName))) {
        if (resultSet.next()) {
          routine.setBody(resultSet.getString("Create Procedure"));
        }
      } catch (SQLException e) {
        try (ResultSet resultSet = getStatement()
          .executeQuery("SHOW CREATE FUNCTION " + sqlHelper.escapeTableName(routineName))) {
          if (resultSet.next()) {
            routine.setBody(resultSet.getString("Create Function"));
          }
        } catch (SQLException e1) {
          LOGGER.debug("Could not retrieve routine code (as routine).", e);
          if (e.getNextException() != e) {
            LOGGER.debug("nextException:", e);
          }
          LOGGER.debug("Could not retrieve routine code (as function).", e1);
          if (e1.getNextException() != e1) {
            LOGGER.debug("nextException:", e1);
          }
        }
      }

      routines.add(routine);
    }
    return routines;
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    ModuleException moduleException = MySQLExceptionNormalizer.getInstance().normalizeException(exception,
      contextMessage);

    // in case the exception normalizer could not handle this exception
    if (moduleException == null) {
      moduleException = super.normalizeException(exception, contextMessage);
    }

    return moduleException;
  }
}
