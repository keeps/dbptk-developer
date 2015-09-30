/**
 *
 */
package com.databasepreservation.modules.mySql.out;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.jdbc.out.JDBCExportModule;
import com.databasepreservation.modules.mySql.MySQLHelper;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author Luis Faria
 */
public class MySQLJDBCExportModule extends JDBCExportModule {

  private static final String[] IGNORED_SCHEMAS = {"mysql", "performance_schema", "information_schema"};
  protected final String hostname;

  protected final String database;

  protected final int port;

  protected final String username;

  protected final String password;
  private final Logger logger = Logger.getLogger(MySQLJDBCExportModule.class);

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

  /**
   * Check if a database exists
   *
   * @param defaultConnectionDb
   *          an existing dbml database to establish the connection
   * @param database
   *          the name of the database to check
   * @param connectionURL
   *          the connection URL needed by getConnection
   * @return true if exists, false otherwise
   * @throws ModuleException
   */
  public Set<String> getExistingSchemasByName(String defaultConnectionDb, String database, String connectionURL)
    throws ModuleException {
    HashSet<String> found = new HashSet<String>();
    try {
      ResultSet result = getConnection(defaultConnectionDb, connectionURL).createStatement().executeQuery(
        sqlHelper.getDatabases(database));
      while (result.next()) {
        found.add(result.getString(1));
      }
    } catch (SQLException e) {
      throw new ModuleException("Error checking if database " + database + " exists", e);
    }
    return found;
  }

  public String createConnectionURL(String databaseName) {
    return createConnectionURL(hostname, port, databaseName, username, password);
  }

  @Override
  protected void handleForeignKeys() throws ModuleException {
    logger.debug("Creating foreign keys");
    try {
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
                logger.warn("Foreign key not exported: " + "referenced schema (" + fkey.getReferencedSchema()
                  + ") is ignored at export.");
                continue;
              }
            }

            String fkeySQL = ((MySQLHelper) sqlHelper).createForeignKeySQL(table, fkey, true, count);
            logger.debug("Returned fkey: " + fkeySQL);
            getStatement().addBatch(fkeySQL);
          }

          getStatement().executeBatch();
          getStatement().clearBatch();
        }
      }
      logger.debug("Getting fkeys finished");
    } catch (SQLException e) {
      SQLException ei = e;
      do {
        if (ei != null) {
          logger.error("Error handleing foreign key (next exception)", ei);
          logger.error("Error description: ", ei);
        }
        ei = ei.getNextException();
      } while (ei != null);
      throw new ModuleException("Error creating foreign keys", e);
    }
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
}
