/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.oracle.out;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
  private String targetSchema = null;

  public Oracle12cJDBCExportModule(String serverName, int port, String instance, String username, String password,
    String sourceSchema) {
    super("oracle.jdbc.driver.OracleDriver",
      "jdbc:oracle:thin:" + username + "/" + password + "@//" + serverName + ":" + port + "/" + instance,
      new OracleHelper());

    this.username = username;
    this.sourceSchema = sourceSchema;
  }

  @Override
  public void initDatabase() throws ModuleException {
    super.initDatabase();

    try(Statement statement = getConnection().createStatement()) {
      statement.execute("select sys_context('USERENV', 'CURRENT_SCHEMA') from dual");
      try(ResultSet resultSet = statement.getResultSet()){
        if (resultSet.next()) {
          targetSchema = resultSet.getString(1);
          ((OracleHelper) getSqlHelper()).setTargetSchema(targetSchema);
          LOGGER.info("Exporting into oracle user's default schema ({})", targetSchema);
        }
      }
    } catch (SQLException e) {
      LOGGER.error("Could not get user default schema", e);
    }
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

    ((OracleHelper) getSqlHelper()).setSourceSchema(sourceSchema);

    for (TableStructure table : schema.getTables()) {
      handleTableStructure(table);
    }

    LOGGER.info("Handling schema structure {} finished", schema.getName());
  }
}
