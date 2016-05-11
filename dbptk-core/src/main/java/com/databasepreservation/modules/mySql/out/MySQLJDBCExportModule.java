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

import com.databasepreservation.model.Reporter;
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
    super("com.mysql.jdbc.Driver", createConnectionURL(hostname, port, database, username, password), new MySQLHelper());
    this.hostname = hostname;
    this.port = port;
    this.database = database;
    this.username = username;
    this.password = password;
    this.ignoredSchemas = new TreeSet<String>(Arrays.asList(IGNORED_SCHEMAS));
  }

  public static String createConnectionURL(String hostname, int port, String database, String username, String password) {
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
      Reporter.customMessage(getClass().getName(), "target database existed and was used anyway");
    } else {
      try {
        LOGGER.info("Target database does not exist. Creating database " + database);
        Reporter.customMessage(getClass().getName(), "target database with name " + Reporter.CODE_DELIMITER + database
          + Reporter.CODE_DELIMITER + " did not exist and was created");
        getConnection(MYSQL_CONNECTION_DATABASE, connectionURL).createStatement().executeUpdate(
          sqlHelper.createDatabaseSQL(database));

      } catch (SQLException e) {
        throw new ModuleException("Error creating database " + database, e);
      }
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

          TableStructure tableAux = databaseStructure.lookupTableStructure(tableId);
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
    LOGGER.info("Handling schema structure " + schema.getName());
    // for mysql the schema never needs to be created, because it is the same as
    // the database and the database must already exist
    for (TableStructure table : schema.getTables()) {
      handleTableStructure(table);
    }
    LOGGER.info("Handling schema structure " + schema.getName() + " finished");
  }
}
