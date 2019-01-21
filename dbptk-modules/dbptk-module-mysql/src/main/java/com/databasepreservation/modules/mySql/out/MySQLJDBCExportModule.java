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
package com.databasepreservation.modules.mySql.out;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.jdbc.out.JDBCExportModule;
import com.databasepreservation.modules.mySql.MySQLHelper;

/**
 * @author Luis Faria
 */
public class MySQLJDBCExportModule extends JDBCExportModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(MySQLJDBCExportModule.class);
  private static final String[] IGNORED_SCHEMAS = {"mysql", "performance_schema", "information_schema"};

  private static final String MYSQL_CONNECTION_DATABASE = "information_schema";

  protected final String hostname;

  protected final String database;

  protected final int port;

  protected final String username;

  protected final String password;

  /**
   * MySQL JDBC export module constructor
   *
   * @param hostname
   *          the hostname of the MySQL server
   * @param database
   *          the name of the database to import from
   * @param username
   *          the name of the user to use in connection
   * @param password
   *          the password of the user to use in connection
   */
  public MySQLJDBCExportModule(String hostname, String database, String username, String password) {
    super("com.mysql.jdbc.Driver", createConnectionURL(hostname, -1, database, username, password), new MySQLHelper());
    this.hostname = hostname;
    this.port = -1;
    this.database = database;
    this.username = username;
    this.password = password;
    this.ignoredSchemas = new TreeSet<String>(Arrays.asList(IGNORED_SCHEMAS));
  }

  /**
   * MySQL JDBC export module constructor
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
   */
  public MySQLJDBCExportModule(String hostname, int port, String database, String username, String password) {
    super("com.mysql.jdbc.Driver", createConnectionURL(hostname, port, database, username, password),
      new MySQLHelper());
    this.hostname = hostname;
    this.port = port;
    this.database = database;
    this.username = username;
    this.password = password;
    this.ignoredSchemas = new TreeSet<String>(Arrays.asList(IGNORED_SCHEMAS));
  }

  public static String createConnectionURL(String hostname, int port, String database, String username,
    String password) {
    return "jdbc:mysql://" + hostname + (port >= 0 ? ":" + port : "") + "/" + database + "?" + "user=" + username
      + "&password=" + password + "&rewriteBatchedStatements=true";
  }

  public String createConnectionURL(String databaseName) {
    return createConnectionURL(hostname, port, databaseName, username, password);
  }

  @Override
  public void initDatabase() throws ModuleException {
    String connectionURL = createConnectionURL(MYSQL_CONNECTION_DATABASE);

    if (databaseExists(MYSQL_CONNECTION_DATABASE, database, connectionURL)) {
      LOGGER.info("Target database already exists, reusing it.");
      reporter.customMessage(getClass().getName(), "target database existed and was used anyway");
    } else {
      try {
        LOGGER.info("Target database does not exist. Creating database {}", database);
        reporter.customMessage(getClass().getName(), "target database with name " + reporter.CODE_DELIMITER + database
          + reporter.CODE_DELIMITER + " did not exist and was created");
        getConnection(MYSQL_CONNECTION_DATABASE, connectionURL).createStatement()
          .executeUpdate(sqlHelper.createDatabaseSQL(database));

      } catch (SQLException e) {
        throw new ModuleException("Error creating database " + database, e);
      }
    }

    try {
      getConnection().createStatement().execute("SET FOREIGN_KEY_CHECKS = 0");
    } catch (SQLException e) {
      LOGGER.info("Could not temporarily disable \"foreign key\" checks. Performance may be affected.", e);
    }

    try {
      getConnection().createStatement().execute("SET UNIQUE_CHECKS = 0");
    } catch (SQLException e) {
      LOGGER.info("Could not temporarily disable \"unique\" checks. Performance may be affected.", e);
    }

    try {
      // magic number "1073741824" means 1GB of size
      getConnection().createStatement().execute("SET GLOBAL max_allowed_packet=1073741824");
      connection.setClientInfo("max_allowed_packet", "1073741824");
    } catch (SQLException e) {
      LOGGER.info("Could not set max_allowed_packet to 1GB. Some data may be lost.", e);
    }
  }

  @Override
  protected void handleForeignKeys() throws ModuleException {
    LOGGER.debug("Creating foreign keys");
    for (SchemaStructure schema : databaseStructure.getSchemas()) {
      if (isIgnoredSchema(schema.getName())) {
        continue;
      }
      for (TableStructure table : schema.getTables()) {
        int count = 0;
        for (ForeignKey fkey : table.getForeignKeys()) {
          count++;
          String originalReferencedSchema = fkey.getReferencedSchema();

          String tableId = originalReferencedSchema + "." + fkey.getReferencedTable();

          TableStructure tableAux = databaseStructure.getTableById(tableId);
          if (tableAux != null) {
            if (isIgnoredSchema(tableAux.getSchema())) {
              LOGGER.warn("Foreign key not exported: " + "referenced schema (" + fkey.getReferencedSchema()
                + ") is ignored at export.");
              continue;
            }
          }

          String fkeySQL = ((MySQLHelper) sqlHelper).createForeignKeySQL(table, fkey, true, count);
          LOGGER.debug("Returned fkey: " + fkeySQL);
          statementAddBatch(fkeySQL);
        }
        statementExecuteAndClearBatch();
      }
    }
    statementExecuteAndClearBatch();
    LOGGER.debug("Getting fkeys finished");
  }

  @Override
  protected Set<String> getExistingSchemasNames() throws SQLException, ModuleException {
    if (existingSchemas == null) {
      existingSchemas = new HashSet<String>();
      ResultSet rs = getConnection().getMetaData().getCatalogs();
      while (rs.next()) {
        existingSchemas.add(rs.getString(1));
      }
    }
    return existingSchemas;
  }

  protected void handleSchemaStructure(SchemaStructure schema) throws ModuleException, UnknownTypeException {
    LOGGER.debug("Handling schema structure {}", schema.getName());
    // for mysql the schema never needs to be created, because it is the same as
    // the database and the database must already exist
    for (TableStructure table : schema.getTables()) {
      handleTableStructure(table);
    }
    LOGGER.debug("Handling schema structure {} finished", schema.getName());
  }

  @Override
  public void finishDatabase() throws ModuleException {
    if (databaseStructure != null) {
      try {
        commit();
        getConnection().setAutoCommit(true);
      } catch (SQLException e) {
        throw new ModuleException("Could not enable autocommit before creating foreign keys", e);
      }
      handleForeignKeys();

      try {
        getConnection().createStatement().execute("SET FOREIGN_KEY_CHECKS = 0");
      } catch (SQLException e) {
        LOGGER.info("Problem activating \"foreign key\" checks.", e);
      }

      try {
        getConnection().createStatement().execute("SET UNIQUE_CHECKS = 0");
      } catch (SQLException e) {
        LOGGER.info("Problem activating \"unique\" checks.", e);
      }
    }
    closeConnections();
  }
}
