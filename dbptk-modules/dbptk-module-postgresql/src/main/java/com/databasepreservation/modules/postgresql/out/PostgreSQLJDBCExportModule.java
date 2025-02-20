/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.postgresql.out;

import java.io.InputStream;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.data.ArrayCell;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.type.ComposedTypeArray;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.modules.jdbc.out.JDBCExportModule;
import com.databasepreservation.modules.postgresql.PostgreSQLExceptionNormalizer;
import com.databasepreservation.modules.postgresql.PostgreSQLHelper;
import com.databasepreservation.utils.RemoteConnectionUtils;
import com.google.common.base.Function;

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
  private final boolean ssh;
  private final String sshHost;
  private final String sshUser;
  private final String sshPassword;
  private final String sshPort;

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
   * 
   *          public PostgreSQLJDBCExportModule(String hostname, String database,
   *          String username, String password, boolean encrypt) {
   *          super("org.postgresql.Driver", createConnectionURL(hostname, -1,
   *          database, username, password, encrypt), new PostgreSQLHelper());
   *          this.hostname = hostname; this.port = -1; this.database = database;
   *          this.username = username; this.password = password; this.encrypt =
   *          encrypt; this.ignoredSchemas = new
   *          TreeSet<String>(Arrays.asList(IGNORED_SCHEMAS)); }
   */

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
    boolean encrypt, boolean ssh, String sshHost, String sshUser, String sshPassword, String sshPort)
    throws ModuleException {
    super("org.postgresql.Driver", createConnectionURL(hostname, port, database, username, password, encrypt),
      new PostgreSQLHelper(), ssh, sshHost, sshUser, sshPassword, sshPort);
    this.hostname = hostname;
    this.port = port;
    this.database = database;
    this.username = username;
    this.password = password;
    this.encrypt = encrypt;
    this.ssh = ssh;
    this.sshHost = sshHost;
    this.sshUser = sshUser;
    this.sshPassword = sshPassword;
    this.sshPort = sshPort;
    this.ignoredSchemas = new TreeSet<String>(Arrays.asList(IGNORED_SCHEMAS));
  }

  public static String createConnectionURL(String hostname, int port, String database, String username, String password,
    boolean encrypt) {
    return "jdbc:postgresql://" + hostname + (port >= 0 ? ":" + port : "") + "/" + database + "?user=" + username
      + "&password=" + password + (encrypt ? "&ssl=true" : "");
  }

  public String createConnectionURL(String databaseName) {
    if (ssh) {
      return createConnectionURL(hostname, RemoteConnectionUtils.getLocalPort(), databaseName, username, password,
        encrypt);
    }
    return createConnectionURL(hostname, port, databaseName, username, password, encrypt);
  }

  @Override
  public void initDatabase() throws ModuleException {
    String connectionURL = createConnectionURL(POSTGRES_CONNECTION_DATABASE);

    if (canDropDatabase) {
      try {
        getConnection(POSTGRES_CONNECTION_DATABASE, connectionURL).createStatement()
          .executeUpdate(sqlHelper.dropDatabase(database));
      } catch (SQLException e) {
        throw new ModuleException().withMessage("Error dropping database " + database).withCause(e);
      }

    }

    if (databaseExists(POSTGRES_CONNECTION_DATABASE, database, connectionURL)) {
      LOGGER.info("Target database already exists, reusing it.");
      reporter.customMessage(getClass().getName(), "target database existed and was used anyway");
    } else {
      try {
        LOGGER.info("Target database does not exist. Creating database " + database);
        reporter.customMessage(getClass().getName(), "target database with name " + reporter.CODE_DELIMITER + database
          + reporter.CODE_DELIMITER + " did not exist and was created");
        getConnection(POSTGRES_CONNECTION_DATABASE, connectionURL).createStatement()
          .executeUpdate(sqlHelper.createDatabaseSQL(database));

      } catch (SQLException e) {
        throw new ModuleException().withMessage("Error creating database " + database).withCause(e);
      }
    }

    this.exportModule.initDatabase();
  }

  /**
   * Checks if a schema with 'schemaName' already exists on the database.
   *
   * @param schemaName
   *          the schema name to be checked.
   * @return
   * @throws SQLException
   * @throws ModuleException
   */
  @Override
  protected boolean isExistingSchema(String schemaName) throws SQLException, ModuleException {
    boolean exists = false;
    for (String existingName : getExistingSchemasNames()) {
      if (existingName.equals(schemaName)) {
        exists = true;
        break;
      }
    }
    return exists;
  }

  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {
    try {
      if (!currentIsIgnoredSchema) {
        getStatement().executeUpdate(((PostgreSQLHelper) getSqlHelper()).grantPermissionsSQL(tableId));
      }
    } catch (SQLException e) {
      LOGGER.error("Error granting public read access permissions on table " + tableId, e);
    }
    super.handleDataCloseTable(tableId);
  }

  @Override
  protected void handleSimpleTypeDateTimeDataCell(String data, PreparedStatement ps, int index, Cell cell,
    ColumnStructure column) throws SQLException {
    SimpleTypeDateTime type = (SimpleTypeDateTime) column.getType();
    if (type.getTimeDefined()) {
      if ("TIME WITH TIME ZONE".equalsIgnoreCase(type.getSql99TypeName())) {
        if (data != null) {
          Calendar cal = jakarta.xml.bind.DatatypeConverter.parseTime(data);
          Time time = new Time(cal.getTimeInMillis());
          LOGGER.debug("time with timezone after: " + time.toString() + "; timezone: " + cal.getTimeZone().getID());
          ps.setTime(index, time, cal);
        } else {
          ps.setNull(index, Types.TIME);
        }
      } else if ("TIMESTAMP".equalsIgnoreCase(type.getSql99TypeName())
        || "TIMESTAMP WITH TIME ZONE".equalsIgnoreCase(type.getSql99TypeName())) {
        if (data != null) {
          Instant instant = Instant.parse(data);
          ps.setTimestamp(index, Timestamp.from(instant));
        } else {
          ps.setNull(index, Types.TIMESTAMP);
        }
      } else {
        super.handleSimpleTypeDateTimeDataCell(data, ps, index, cell, column);
      }
    } else {
      super.handleSimpleTypeDateTimeDataCell(data, ps, index, cell, column);
    }
  }

  @Override
  protected void handleSimpleTypeNumericApproximateDataCell(String data, PreparedStatement ps, int index, Cell cell,
    ColumnStructure column) throws NumberFormatException, SQLException {
    if (data != null) {
      LOGGER.debug("set approx: " + data);
      if ("FLOAT".equalsIgnoreCase(column.getType().getSql99TypeName())) {
        ps.setFloat(index, Float.parseFloat(data));
      } else {
        ps.setDouble(index, Double.parseDouble(data));
      }
    } else {
      ps.setNull(index, Types.FLOAT);
    }
  }

  @Override
  protected InputStream handleSimpleTypeString(PreparedStatement ps, int index, BinaryCell bin, ColumnStructure column)
    throws SQLException, ModuleException {
    InputStream inputStream = bin.createInputStream();
    ps.setBinaryStream(index, inputStream, bin.getSize());
    return inputStream;
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    ModuleException moduleException = PostgreSQLExceptionNormalizer.getInstance().normalizeException(exception,
      contextMessage);

    // in case the exception normalizer could not handle this exception
    if (moduleException == null) {
      moduleException = super.normalizeException(exception, contextMessage);
    }

    return moduleException;
  }

  @Override
  protected void handleComposedTypeArrayDataCell(final ArrayCell arrayCell, PreparedStatement ps, int index,
    ColumnStructure column) throws SQLException, ModuleException {

    ComposedTypeArray arrayType = (ComposedTypeArray) column.getType();

    Function<Cell, String> conversionFunction = new Function<Cell, String>() {
      @Override
      public String apply(Cell cell) {
        if (cell instanceof SimpleCell) {
          return ((SimpleCell) cell).getSimpleData();
        } else {
          LOGGER.debug("Exporting composed data inside an array is not supported.");
          return null;
        }
      }
    };

    Object[] array = arrayCell.toArray(conversionFunction, String.class);

    Array sqlArray = getConnection().createArrayOf(sqlHelper.createTypeSQL(arrayType.getElementType(), false, false),
      array);

    ps.setArray(index, sqlArray);
  }

@Override
protected void handleSimpleTypeStringDataCell(String data, PreparedStatement ps, int index, Cell cell,
  ColumnStructure column) throws SQLException {
    if (data != null) {
      data = data.replace("\u0000", "");
      ps.setString(index, data);
    } else {
      ps.setNull(index, Types.VARCHAR);
    }
  }
}
