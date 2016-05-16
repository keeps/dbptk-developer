package com.databasepreservation.modules.sqlServer.in;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.FileItem;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.sqlServer.SQLServerHelper;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;

/**
 * Microsoft SQL Server JDBC import module.
 *
 * @author Luis Faria
 */
public class SQLServerJDBCImportModule extends JDBCImportModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLServerJDBCImportModule.class);

  /**
   * Create a new Microsoft SQL Server import module using the default instance.
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
  public SQLServerJDBCImportModule(String serverName, String database, String username, String password,
    boolean integratedSecurity, boolean encrypt) {
    super("com.microsoft.sqlserver.jdbc.SQLServerDriver", "jdbc:sqlserver://" + serverName + ";database=" + database
      + ";user=" + username + ";password=" + password + ";integratedSecurity="
      + (integratedSecurity ? "true" : "false") + ";encrypt=" + (encrypt ? "true" : "false"), new SQLServerHelper(),
      new SQLServerDatatypeImporter());

    System.setProperty("java.net.preferIPv6Addresses", "true");

  }

  /**
   * Create a new Microsoft SQL Server import module using the instance name.
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
  public SQLServerJDBCImportModule(String serverName, String instanceName, String database, String username,
    String password, boolean integratedSecurity, boolean encrypt) {
    super("com.microsoft.sqlserver.jdbc.SQLServerDriver", "jdbc:sqlserver://" + serverName + "\\" + instanceName
      + ";database=" + database + ";user=" + username + ";password=" + password + ";integratedSecurity="
      + (integratedSecurity ? "true" : "false") + ";encrypt=" + (encrypt ? "true" : "false"), new SQLServerHelper(),
      new SQLServerDatatypeImporter());

  }

  /**
   * Create a new Microsoft SQL Server import module using the port number.
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
  public SQLServerJDBCImportModule(String serverName, int portNumber, String database, String username,
    String password, boolean integratedSecurity, boolean encrypt) {
    super("com.microsoft.sqlserver.jdbc.SQLServerDriver", "jdbc:sqlserver://" + serverName + ":" + portNumber
      + ";database=" + database + ";user=" + username + ";password=" + password + ";integratedSecurity="
      + (integratedSecurity ? "true" : "false") + ";encrypt=" + (encrypt ? "true" : "false"), new SQLServerHelper(),
      new SQLServerDatatypeImporter());

  }

  @Override
  protected Statement getStatement() throws SQLException, ClassNotFoundException {
    if (statement == null) {
      statement = ((SQLServerConnection) getConnection()).createStatement(
        SQLServerResultSet.TYPE_FORWARD_ONLY, SQLServerResultSet.CONCUR_READ_ONLY);

      SQLServerStatement sqlServerStatement = statement.unwrap(com.microsoft.sqlserver.jdbc.SQLServerStatement.class);
      sqlServerStatement.setResponseBuffering("adaptive");
    }
    return statement;
  }

  @Override
  protected Set<String> getIgnoredImportedSchemas() {
    Set<String> ignored = new HashSet<String>();
    ignored.add("db_.*");
    ignored.add("sys");
    ignored.add("INFORMATION_SCHEMA");
    ignored.add("guest");
    return ignored;
  }

  protected Cell convertRawToCell(String tableName, String columnName, int columnIndex, int rowIndex, Type cellType,
    ResultSet rawData) throws SQLException, InvalidDataException, ClassNotFoundException, ModuleException {
    Cell cell;
    String id = tableName + "." + columnName + "." + rowIndex;
    if (cellType instanceof SimpleTypeBinary) {
      InputStream input = rawData.getBinaryStream(columnName);
      if (input != null) {
        LOGGER.debug("SQL ServerbinaryStream: " + columnName);
        FileItem fileItem = new FileItem(input);
        cell = new BinaryCell(id, fileItem);
      } else {
        cell = new BinaryCell(id, null);
      }

    } else {
      cell = super.convertRawToCell(tableName, columnName, columnIndex, rowIndex, cellType, rawData);
    }
    return cell;
  }

  @Override
  protected String processTriggerEvent(String string) {
    LOGGER.debug("Trigger event: " + string);
    char[] charArray = string.toCharArray();

    String res = "";
    if (charArray.length > 0 && charArray[0] == '1') {
      res = "INSERT";
    }

    if (charArray.length > 1 && charArray[1] == '1') {
      if (!"".equals(res)) {
        res += " OR ";
      }
      res = "UPDATE";
    }

    if (charArray.length > 2 && charArray[2] == '1') {
      if (!"".equals(res)) {
        res += " OR ";
      }
      res = "DELETE";
    }
    return res;
  }

  @Override
  protected String processActionTime(String string) {
    LOGGER.debug("Trigger action time: " + string);
    char[] charArray = string.toCharArray();

    String res = "";
    if (charArray.length > 0 && charArray[0] == '1') {
      res = "AFTER";
    }
    if (charArray.length > 1 && charArray[1] == '1') {
      if (!"".equals(res)) {
        res += " OR ";
      }
      res = "INSTEAD OF";
    }

    // note that SQL Server does not support BEFORE triggers

    return res;
  }
}
