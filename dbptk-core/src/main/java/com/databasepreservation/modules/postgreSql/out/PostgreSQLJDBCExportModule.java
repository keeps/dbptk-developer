package com.databasepreservation.modules.postgreSql.out;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TreeSet;

import com.databasepreservation.model.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.util.InvalidDateException;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.out.JDBCExportModule;
import com.databasepreservation.modules.postgreSql.PostgreSQLHelper;

/**
 * <p>
 * Module to export data to a PostgreSQL database management system via JDBC
 * driver. The postgresql-8.3-603.jdbc3.jar driver supports PostgreSQL version
 * 7.4 to 8.3.
 * </p>
 * <p>
 * <p>
 * To use this module, the PostgreSQL server must be configured:
 * </p>
 * <ol>
 * <li>Server must be configured to accept TCP/IP connections. This can be done
 * by setting <code>listen_addresses = 'localhost'</code> (or
 * <code>tcpip_socket = true</code> in older versions) in the postgresql.conf
 * file.</li>
 * <li>The client authentication setup in the pg_hba.conf file may need to be
 * configured, adding a line like
 * <code>host all all 127.0.0.1 255.0.0.0 trust</code>. The JDBC driver supports
 * the trust, ident, password, md5, and crypt authentication methods.</li>
 * </ol>
 *
 * @author Luis Faria
 */
public class PostgreSQLJDBCExportModule extends JDBCExportModule {

  private static final String POSTGRES_CONNECTION_DATABASE = "postgres";
  // same as Types.TIME_WITH_TIMEZONE. the type constant was only released in
  // java 7
  private static final int Types_TIME_WITH_TIMEZONE = 0x7dd;
  private static final String[] IGNORED_SCHEMAS = {};
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLJDBCExportModule.class);
  private final String hostname;
  private final int port;
  private final String database;
  private final String username;
  private final String password;
  private final boolean encrypt;

  /**
   * Create a new PostgreSQL JDBC export module
   *
   * @param hostname
   *          the name of the PostgreSQL server host (e.g. localhost)
   * @param database
   *          the name of the database to connect to
   * @param username
   *          the name of the user to use in connection
   * @param password
   *          the password of the user to use in connection
   * @param encrypt
   *          encrypt connection
   */
  public PostgreSQLJDBCExportModule(String hostname, String database, String username, String password, boolean encrypt) {
    super("org.postgresql.Driver", createConnectionURL(hostname, -1, database, username, password, encrypt),
      new PostgreSQLHelper());
    this.hostname = hostname;
    this.port = -1;
    this.database = database;
    this.username = username;
    this.password = password;
    this.encrypt = encrypt;
    this.ignoredSchemas = new TreeSet<String>(Arrays.asList(IGNORED_SCHEMAS));
  }

  /**
   * Create a new PostgreSQL JDBC export module
   *
   * @param hostname
   *          the name of the PostgreSQL server host (e.g. localhost)
   * @param port
   *          the port of where the PostgreSQL server is listening, default is
   *          5432
   * @param database
   *          the name of the database to connect to
   * @param username
   *          the name of the user to use in connection
   * @param password
   *          the password of the user to use in connection
   * @param encrypt
   *          encrypt connection
   */
  public PostgreSQLJDBCExportModule(String hostname, int port, String database, String username, String password,
    boolean encrypt) {
    super("org.postgresql.Driver", createConnectionURL(hostname, port, database, username, password, encrypt),
      new PostgreSQLHelper());
    this.hostname = hostname;
    this.port = port;
    this.database = database;
    this.username = username;
    this.password = password;
    this.encrypt = encrypt;
    this.ignoredSchemas = new TreeSet<String>(Arrays.asList(IGNORED_SCHEMAS));
  }

  public static String createConnectionURL(String hostname, int port, String database, String username,
    String password, boolean encrypt) {
    return "jdbc:postgresql://" + hostname + (port >= 0 ? ":" + port : "") + "/" + database + "?user=" + username
      + "&password=" + password + (encrypt ? "&ssl=true" : "");
  }

  public String createConnectionURL(String databaseName) {
    return createConnectionURL(hostname, port, databaseName, username, password, encrypt);
  }

  @Override
  public void initDatabase() throws ModuleException {
    String connectionURL = createConnectionURL(POSTGRES_CONNECTION_DATABASE);

    if (canDropDatabase) {
      try {
        getConnection(POSTGRES_CONNECTION_DATABASE, connectionURL).createStatement().executeUpdate(
          sqlHelper.dropDatabase(database));
      } catch (SQLException e) {
        throw new ModuleException("Error droping database " + database, e);
      }

    }

    if (databaseExists(POSTGRES_CONNECTION_DATABASE, database, connectionURL)) {
      LOGGER.info("Target database already exists, reusing it.");
      Reporter.customMessage(getClass().getName(), "target database existed and was used anyway");
    } else {
      try {
        LOGGER.info("Target database does not exist. Creating database " + database);
        Reporter.customMessage(getClass().getName(), "target database with name " + Reporter.CODE_DELIMITER + database
          + Reporter.CODE_DELIMITER + " did not exist and was created");
        getConnection(POSTGRES_CONNECTION_DATABASE, connectionURL).createStatement().executeUpdate(
          sqlHelper.createDatabaseSQL(database));

      } catch (SQLException e) {
        throw new ModuleException("Error creating database " + database, e);
      }
    }
  }

  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {
    try {
      if (!currentIsIgnoredSchema) {
        getStatement().executeUpdate(
          ((PostgreSQLHelper) getSqlHelper()).grantPermissionsSQL(currentTableStructure.getId()));
      }
    } catch (SQLException e) {
      throw new ModuleException("Error granting permissions to public", e);
    }
    super.handleDataCloseTable(tableId);
  }

  @Override
  protected void handleSimpleTypeDateTimeDataCell(String data, PreparedStatement ps, int index, Cell cell, Type type)
    throws InvalidDateException, SQLException {
    SimpleTypeDateTime dateTime = (SimpleTypeDateTime) type;
    if (dateTime.getTimeDefined()) {
      if ("TIME WITH TIME ZONE".equalsIgnoreCase(type.getSql99TypeName())) {
        if (data != null) {
          Calendar cal = javax.xml.bind.DatatypeConverter.parseTime(data);
          Time time = new Time(cal.getTimeInMillis());
          LOGGER.debug("time with timezone after: " + time.toString() + "; timezone: " + cal.getTimeZone().getID());
          ps.setTime(index, time, cal);
        } else {
          ps.setNull(index, Types_TIME_WITH_TIMEZONE);
        }
      } else {
        super.handleSimpleTypeDateTimeDataCell(data, ps, index, cell, type);
      }
    } else {
      super.handleSimpleTypeDateTimeDataCell(data, ps, index, cell, type);
    }
  }

  @Override
  protected void handleSimpleTypeNumericApproximateDataCell(String data, PreparedStatement ps, int index, Cell cell,
    Type type) throws NumberFormatException, SQLException {
    if (data != null) {
      LOGGER.debug("set approx: " + data);
      if ("FLOAT".equalsIgnoreCase(type.getSql99TypeName())) {
        ps.setFloat(index, Float.valueOf(data));
      } else {
        ps.setDouble(index, Double.valueOf(data));
      }
    } else {
      ps.setNull(index, Types.FLOAT);
    }
  }

  @Override
  protected void handleSimpleTypeString(PreparedStatement ps, int index, BinaryCell bin) throws SQLException,
    ModuleException {
    ps.setBinaryStream(index, bin.createInputstream(), bin.getLength());
  }
}
