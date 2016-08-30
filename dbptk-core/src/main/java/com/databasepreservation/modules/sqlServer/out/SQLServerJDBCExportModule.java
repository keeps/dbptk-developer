/**
 *
 */
package com.databasepreservation.modules.sqlServer.out;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.out.JDBCExportModule;
import com.databasepreservation.modules.sqlServer.SQLServerHelper;

/**
 * @author Luis Faria
 */
public class SQLServerJDBCExportModule extends JDBCExportModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLServerJDBCExportModule.class);

  /**
   * Create a new Microsoft SQL Server export module using the default instance.
   *
   * @param serverName
   *          the name (host name) of the server
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
  public SQLServerJDBCExportModule(String serverName, String database, String username, String password,
    boolean integratedSecurity, boolean encrypt) {
    super("com.microsoft.sqlserver.jdbc.SQLServerDriver", new StringBuilder("jdbc:sqlserver://").append(serverName)
      .append(";database=").append(database).append(";user=").append(username).append(";password=").append(password)
      .append(";integratedSecurity=").append(integratedSecurity).append(";encrypt=").append(encrypt).toString(),
      new SQLServerHelper());
  }

  /**
   * Create a new Microsoft SQL Server export module using the instance name.
   * The constructor using the port number is preferred over this to avoid a
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
  public SQLServerJDBCExportModule(String serverName, String instanceName, String database, String username,
    String password, boolean integratedSecurity, boolean encrypt) {
    super("com.microsoft.sqlserver.jdbc.SQLServerDriver", new StringBuilder("jdbc:sqlserver://").append(serverName)
      .append("\\").append(instanceName).append(";database=").append(database).append(";user=").append(username)
      .append(";password=").append(password).append(";integratedSecurity=").append(integratedSecurity)
      .append(";encrypt=").append(encrypt).toString(), new SQLServerHelper());
  }

  /**
   * Create a new Microsoft SQL Server export module using the port number.
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
  public SQLServerJDBCExportModule(String serverName, int portNumber, String database, String username,
    String password, boolean integratedSecurity, boolean encrypt) {
    super("com.microsoft.sqlserver.jdbc.SQLServerDriver", new StringBuilder("jdbc:sqlserver://").append(serverName)
      .append(":").append(portNumber).append(";database=").append(database).append(";user=").append(username)
      .append(";password=").append(password).append(";integratedSecurity=").append(integratedSecurity)
      .append(";encrypt=").append(encrypt).toString(), new SQLServerHelper());
  }

  @Override
  protected CleanResourcesInterface handleDataCell(PreparedStatement ps, int index, Cell cell, Type type)
    throws InvalidDataException, ModuleException {
    if (cell instanceof SimpleCell && type instanceof SimpleTypeDateTime) {
      SimpleCell simple = (SimpleCell) cell;
      String data = simple.getSimpleData();
      try {
        if (data != null) {
          ps.setString(index, data);
        } else {
          ps.setNull(index, Types.CHAR);
        }
      } catch (SQLException e) {
        throw new ModuleException("SQL error while handling cell " + cell.getId(), e);
      }
    } else {
      return super.handleDataCell(ps, index, cell, type);
    }
    return noOpCleanResourcesInterface;
  }

  /**
   * Although a it's not a schema, a 'public' object exists on SQLServer. A new
   * schema name is assigned.
   */
  @Override
  protected void handleSchemaStructure(SchemaStructure schema) throws ModuleException, UnknownTypeException {
    LOGGER.debug("Handling schema structure " + schema.getName());
    if ("public".equalsIgnoreCase(schema.getName())) {
      existingSchemas.add("public");
    }
    super.handleSchemaStructure(schema);
  }
}
