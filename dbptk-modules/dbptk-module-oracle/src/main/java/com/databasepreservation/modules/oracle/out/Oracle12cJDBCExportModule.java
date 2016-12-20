package com.databasepreservation.modules.oracle.out;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.jdbc.out.JDBCExportModule;
import com.databasepreservation.modules.oracle.OracleHelper;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 * @author Miguel Coutada
 */

public class Oracle12cJDBCExportModule extends JDBCExportModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(Oracle12cJDBCExportModule.class);

  private final String username;
  private String sourceSchema = null;

  /**
   * Create a new Oracle12c import module
   *
   * @param serverName
   *          the name (host name) of the server
   * @param database
   *          the name of the database we'll be accessing
   * @param username
   *          the name of the user to use in the connection
   * @param password
   *          the password of the user to use in the connection
   */
  public Oracle12cJDBCExportModule(String serverName, int port, String database, String username, String password) {
    super("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:" + username + "/" + password + "@//" + serverName + ":"
      + port + "/" + database, new OracleHelper());

    this.username = username;
  }

  public Oracle12cJDBCExportModule(String serverName, int port, String database, String username, String password,
    String sourceSchema) {
    this(serverName, port, database, username, password);
    this.sourceSchema = sourceSchema;
  }

  @Override
  protected boolean isIgnoredSchema(String schema) {
    // if a default schema was defined, it will be the only one to be exported
    // if no default schema was defined, the first schema will become the
    // sourceSchema, making the module ignore the following schemas and only
    // export the first one
    if (sourceSchema == null) {
      return false;
    } else if (schema.equalsIgnoreCase(sourceSchema)) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  protected void handleSchemaStructure(SchemaStructure schema) throws ModuleException, UnknownTypeException {
    LOGGER.info("Handling schema structure " + schema.getName());

    // if no source schema was specified, use the first schema as the default
    // schema
    // it will then be used in isIgnoredSchema method to exclude all the other
    // schemas
    if (sourceSchema == null) {
      sourceSchema = schema.getName();
    }

    try {
      if (!isExistingSchema(schema.getName())) {
        throw new ModuleException(
          "Schema (tablespace) "
            + schema.getName()
            + " does not exist in target database. The schema/tablespace must be created before inserting the data into the target database.");
      }

      if (!isDefaultTableSpace(schema.getName())) {
        throw new ModuleException(
          "Schema/Tablespace "
            + schema.getName()
            + " is not the default tablespace for user "
            + username
            + ". The tablespace must be set as the default tablespace for this user before inserting the data into the target database.");
      }

      for (TableStructure table : schema.getTables()) {
        handleTableStructure(table);
      }

      LOGGER.info("Handling schema structure " + schema.getName() + " finished");
    } catch (SQLException e) {
      LOGGER.error("Error handling schema structure", e);
      SQLException nextException = e.getNextException();
      if (nextException != null) {
        LOGGER.error("Error details", nextException);
      }
      throw new ModuleException("Error while adding schema SQL to batch", e);
    }
  }

  /**
   * Checks if the specified schema is the default tablespace for the user being
   * used in the connection to the database
   * 
   * @param expectedDefaultTablespaceName
   *          the tablespace name that should be the default for this user
   * @return the actual and expected tablespace names match
   * @throws ModuleException
   * @throws SQLException
   */
  private boolean isDefaultTableSpace(String expectedDefaultTablespaceName) throws ModuleException, SQLException {
    String query = "select default_tablespace from user_users";
    LOGGER.debug("Getting default tablespace: " + query);
    ResultSet rs = getStatement().executeQuery(query);

    if (rs.next()) {
      String defaultTablespaceName = rs.getString(1);
      LOGGER.debug("Default tablespace for user " + username + " is " + defaultTablespaceName + " (expected it to be: "
        + expectedDefaultTablespaceName + ").");
      if (defaultTablespaceName.equalsIgnoreCase(expectedDefaultTablespaceName)) {
        return true;
      }
    }
    return false;
  }
}
