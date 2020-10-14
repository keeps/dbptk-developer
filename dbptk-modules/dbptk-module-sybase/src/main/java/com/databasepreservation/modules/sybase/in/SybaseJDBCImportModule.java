/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.sybase.in;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.RoutineStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.CloseableUtils;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.sybase.SybaseHelper;
import com.databasepreservation.modules.sybase.SybaseModuleFactory;
import com.databasepreservation.utils.MapUtils;
import com.databasepreservation.utils.RemoteConnectionUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SybaseJDBCImportModule extends JDBCImportModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(SybaseJDBCImportModule.class);
  private static final String DRIVER_CLASS_NAME = "com.sybase.jdbc4.jdbc.SybDriver";
  private static final String URL_CONNECTION_PREFIX = "jdbc:sybase:Tds:";

  private final Pattern pattern = Pattern.compile("create trigger ([^\\s]+)", Pattern.CASE_INSENSITIVE);

  /**
   * Creates a new Sybase import module using the default instance.
   * 
   * @param hostname
   *          the name of the Sybase server host (e.g. localhost)
   * @param port
   *          the port that sybase is listening
   * @param database
   *          the name of the database we'll be accessing
   * @param username
   *          the name of the user to use in the connection
   * @param password
   *          the password of the user to use in the connection
   *
   */
  public SybaseJDBCImportModule(String moduleName, String hostname, int port, String database, String username,
    String password) {
    super(DRIVER_CLASS_NAME, URL_CONNECTION_PREFIX + hostname + ":" + port + "/" + database, new SybaseHelper(),
      new SybaseDataTypeImporter(), moduleName,
      MapUtils.buildMapFromObjects(SybaseModuleFactory.PARAMETER_HOSTNAME, hostname,
        SybaseModuleFactory.PARAMETER_PORT_NUMBER, port, SybaseModuleFactory.PARAMETER_USERNAME, username,
        SybaseModuleFactory.PARAMETER_PASSWORD, password, SybaseModuleFactory.PARAMETER_DATABASE, database));

    createCredentialsProperty(username, password);
  }

  public SybaseJDBCImportModule(String moduleName, String hostname, int port, String database, String username,
    String password, String sshHost, String sshUser, String sshPassword, String sshPortNumber) throws ModuleException {
    super(DRIVER_CLASS_NAME, URL_CONNECTION_PREFIX + hostname + ":" + port + "/" + database, new SybaseHelper(),
      new SybaseDataTypeImporter(), moduleName,
      MapUtils.buildMapFromObjects(SybaseModuleFactory.PARAMETER_HOSTNAME, hostname,
        SybaseModuleFactory.PARAMETER_PORT_NUMBER, port, SybaseModuleFactory.PARAMETER_USERNAME, username,
        SybaseModuleFactory.PARAMETER_PASSWORD, password, SybaseModuleFactory.PARAMETER_DATABASE, database),
      MapUtils.buildMapFromObjects(SybaseModuleFactory.PARAMETER_SSH, true, SybaseModuleFactory.PARAMETER_SSH_HOST,
        sshHost, SybaseModuleFactory.PARAMETER_SSH_PORT, sshPortNumber, SybaseModuleFactory.PARAMETER_SSH_USER, sshUser,
        SybaseModuleFactory.PARAMETER_SSH_PASSWORD, sshPassword));

    createCredentialsProperty(username, password);
  }

  /**
   * Connect to the server using the properties defined in the constructor
   *
   * @return the new connection
   * @throws ModuleException
   */
  @Override
  protected Connection createConnection() throws ModuleException {
    Connection connection;
    try {
      if (ssh) {
        connectionURL = RemoteConnectionUtils.replaceHostAndPort(connectionURL);
      }
      connection = DriverManager.getConnection(connectionURL, getCredentials());
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      throw normalizeException(e, null);
    }
    LOGGER.debug("Connected");
    return connection;
  }

  /**
   * Gets schemas that won't be imported
   * <p>
   * Accepts schema names in as regular expressions I.e. SYS.* will ignore SYSCAT,
   * SYSFUN, etc
   *
   * @return the schema names not to be imported
   */
  @Override
  protected Set<String> getIgnoredImportedSchemas() {
    Set<String> ignored = new HashSet<>();
    ignored.add("diagnostics");
    ignored.add("EXEC_ROLE");
    ignored.add("EXTENV_MAIN");
    ignored.add("EXTENV_WORKER");
    ignored.add("MODIFY_ROLE");
    ignored.add("READ_ROLE");
    ignored.add("rs_systabgroup");
    ignored.add("SA_DEBUG");
    ignored.add("SYS");
    ignored.add("SYS_.*");
    ignored.add("UPDATER");

    return ignored;
  }

  /**
   * Returns a {@link Statement} with specific {@link ResultSet} options
   *
   * @return A {@link Statement}
   * @throws SQLException
   * @throws ModuleException
   */
  @Override
  protected Statement getStatement() throws SQLException, ModuleException {
    if (statement == null) {
      statement = getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }
    return statement;
  }

  @Override
  protected Cell rawToCellSimpleTypeBinary(String id, String columnName, Type cellType, ResultSet rawData)
    throws SQLException, ModuleException {
    Cell cell;

    InputStream binaryStream = rawData.getBinaryStream(columnName);

    if (binaryStream != null && !rawData.wasNull()) {
      cell = new BinaryCell(id, binaryStream);
    } else {
      cell = new NullCell(id);
    }
    return cell;
  }

  /**
   * Sanitizes the trigger action time data
   *
   * @param string
   * @return
   */
  @Override
  protected String processActionTime(String string) {
    LOGGER.debug("Trigger action time: {}", string);
    char[] charArray = string.toCharArray();

    String res = "";
    if (charArray.length == 1 && (charArray[0] == 'A' || charArray[0] == 'S')) {
      res = "AFTER";
    }

    if (charArray.length == 1 && charArray[0] == 'B') {
      res = "BEFORE";
    }
    if (charArray.length == 1 && (charArray[0] == 'K' || charArray[0] == 'I')) {
      res = "INSTEAD OF";
    }

    return res;
  }

  /**
   * Sanitizes the trigger event data
   *
   * @param string
   * @return
   */
  @Override
  protected String processTriggerEvent(String string) {
    LOGGER.debug("Trigger event: {}", string);
    char[] charArray = string.toCharArray();

    String res = "";
    if (charArray.length == 1 && charArray[0] == 'A') {
      res = "INSERT, DELETE";
    }

    if (charArray.length == 1 && charArray[0] == 'B') {
      res = "INSERT, UPDATE";
    }

    if (charArray.length == 1 && charArray[0] == 'C') {
      res = "UPDATE";
    }

    if (charArray.length == 1 && charArray[0] == 'D') {
      res = "DELETE";
    }

    if (charArray.length == 1 && charArray[0] == 'E') {
      res = "DELETE, UPDATE";
    }

    if (charArray.length == 1 && charArray[0] == 'I') {
      res = "INSERT";
    }

    if (charArray.length == 1 && charArray[0] == 'U') {
      res = "UPDATE";
    }

    if (charArray.length == 1 && charArray[0] == 'M') {
      res = "INSERT, DELETE, UPDATE";
    }

    return res;
  }

  /**
   * Method invoked to try obtain the trigger name from the source code
   *
   * @param string
   * @return
   */
  @Override
  protected String processTriggerName(String string) {
    final Matcher matcher = pattern.matcher(string);
    String triggerName = "undefined";
    while (matcher.find()) {
      triggerName = matcher.group(1);
    }

    return triggerName;
  }

  /**
   * Gets the views of a given schema
   *
   * @param schemaName
   *          the schema name
   * @return A list of {@link ViewStructure}
   * @throws SQLException
   * @throws ModuleException
   */
  @Override
  protected List<ViewStructure> getViews(String schemaName) throws SQLException, ModuleException {
    List<ViewStructure> views = super.getViews(schemaName);
    for (ViewStructure v : views) {

      String queryForViewSQL = ((SybaseHelper) sqlHelper).getViewSQL(v.getName());
      ResultSet rset = null;
      PreparedStatement statement = null;
      statement = getConnection().prepareStatement(queryForViewSQL);
      try {
        rset = statement.executeQuery();
        StringBuilder b = new StringBuilder();
        while (rset.next()) {
          b.append(rset.getString("TEXT"));
        }

        v.setQueryOriginal(b.toString());
      } catch (SQLException e) {
        LOGGER.debug("Exception trying to get view SQL in Sybase", e);
      } finally {
        CloseableUtils.closeQuietly(rset);
        CloseableUtils.closeQuietly(statement);
      }
    }
    return views;
  }

  /**
   * Gets the routines of a given schema
   *
   *
   * @param schemaName
   *          the schema name
   * @return A list of {@link RoutineStructure}
   * @throws SQLException
   * @throws ModuleException
   */
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

        String queryForProcedureSQL = ((SybaseHelper) sqlHelper).getProcedureSQL(routine.getName());
        try (ResultSet res = getStatement().executeQuery(queryForProcedureSQL)) {
          StringBuilder b = new StringBuilder();
          while (res.next()) {
            b.append(res.getString("TEXT"));
          }
          routine.setBody(b.toString());
        } catch (SQLException e) {
          LOGGER.debug("Could not retrieve routine code (as routine) for " + routine.getName(), e);
          routine.setBody("");
        }

        routines.add(routine);
      }
    }

    return routines;
  }
}