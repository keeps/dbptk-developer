/**
 *
 */
package com.databasepreservation.modules.mySql.in;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.CheckConstraint;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.UserStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.mySql.MySQLHelper;

/**
 * @author Luis Faria <lfaria@keep.pt>
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class MySQLJDBCImportModule extends JDBCImportModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(MySQLJDBCImportModule.class);
  private final String username;

  /**
   * MySQL JDBC import module constructor
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
  public MySQLJDBCImportModule(String hostname, String database, String username, String password) {
    super("com.mysql.jdbc.Driver", "jdbc:mysql://" + hostname + "/" + database + "?" + "user=" + username
      + "&password=" + password, new MySQLHelper(), new MySQLDatatypeImporter());
    this.username = username;
  }

  /**
   * MySQL JDBC import module constructor
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
  public MySQLJDBCImportModule(String hostname, int port, String database, String username, String password) {
    super("com.mysql.jdbc.Driver", "jdbc:mysql://" + hostname + ":" + port + "/" + database + "?" + "user=" + username
      + "&password=" + password, new MySQLHelper(), new MySQLDatatypeImporter());
    this.username = username;
  }

  @Override
  protected boolean isGetRowAvailable() {
    return false;
  }

  @Override
  protected List<SchemaStructure> getSchemas() throws SQLException, ClassNotFoundException, UnknownTypeException {
    List<SchemaStructure> schemas = new ArrayList<SchemaStructure>();
    String schemaName = getConnection().getCatalog();
    schemas.add(getSchemaStructure(schemaName, 1));
    return schemas;
  }

  @Override
  protected String getReferencedSchema(String s) throws SQLException, ClassNotFoundException {
    return (s == null) ? getConnection().getCatalog() : s;
  }

  @Override
  protected List<UserStructure> getUsers() throws SQLException, ClassNotFoundException {
    List<UserStructure> users = new ArrayList<>();

    String query = sqlHelper.getUsersSQL(null);
    Statement statement = getStatement();
    if (query != null) {
      ResultSet rs = null;

      try {
        rs = statement.executeQuery(sqlHelper.getUsersSQL(null));

        while (rs.next()) {
          UserStructure user = new UserStructure(rs.getString(2) + "@" + rs.getString(1), null);
          users.add(user);
        }
      } catch (SQLException e) {
        if (e.getMessage().startsWith("SELECT command denied to user ") && e.getMessage().endsWith(" for table 'user'")) {
          LOGGER
            .warn("The selected MySQL user does not have permissions to list database users. This permission can be granted with the command \"GRANT SELECT ON mysql.user TO 'username'@'localhost' IDENTIFIED BY 'password';\"");
        } else {
          LOGGER.error("It was not possible to retrieve the list of database users.", e);
        }
      }
    }

    if (users.isEmpty()) {
      users.add(new UserStructure(username, ""));
      LOGGER.warn("Users were not imported. '" + username + "' will be set as the user name.");
    }

    return users;
  }

  @Override
  protected ResultSet getTableRawData(TableStructure table) throws SQLException, ClassNotFoundException,
    ModuleException {
    LOGGER.debug("query: " + sqlHelper.selectTableSQL(table.getId()));

    Statement statement = getStatement();
    statement.setFetchSize(Integer.MIN_VALUE);

    ResultSet set = statement.executeQuery(sqlHelper.selectTableSQL(table.getId()));
    return set;
  }

  @Override
  protected Statement getStatement() throws SQLException, ClassNotFoundException {
    if (statement == null) {
      statement = getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }
    return statement;
  }

  @Override
  protected List<ViewStructure> getViews(String schemaName) throws SQLException, ClassNotFoundException,
    UnknownTypeException {
    List<ViewStructure> views = super.getViews(schemaName);
    for (ViewStructure v : views) {
      Statement statement = getConnection().createStatement();
      String query = "SHOW CREATE VIEW " + sqlHelper.escapeViewName(v.getName());
      ResultSet rset = statement.executeQuery(query);
      rset.next(); // Returns only one tuple

      // TO-DO: the string given below by rset.getString(2) has to be parsed a
      // little before it is set to as the view
      v.setQueryOriginal(rset.getString(2));
    }
    return views;
  }

  @Override
  protected Cell rawToCellSimpleTypeNumericExact(String id, String columnName, Type cellType, ResultSet rawData)
    throws SQLException {
    if ("YEAR".equals(cellType.getOriginalTypeName())) {
      // for inputs 15, 2015, 99 and 1999
      // rawData.getInt returns numbers like 15, 2015, 99, 1999
      // rawData.getString returns dates like 2015-01-01, 2015-01-01,
      // 1999-01-01, 1999-01-01
      // to get the "real" year value, using the first 4 characters from the
      // date string
      String data = rawData.getString(columnName);
      if (data != null) {
        return new SimpleCell(id, data.substring(0, 4));
      } else {
        return new NullCell(id);
      }
    } else {
      return super.rawToCellSimpleTypeNumericExact(id, columnName, cellType, rawData);
    }
  }

  /**
   * Gets the check constraints of a given schema table
   *
   * @param schemaName
   * @param tableName
   * @return
   * @throws ClassNotFoundException
   */
  @Override
  protected List<CheckConstraint> getCheckConstraints(String schemaName, String tableName)
    throws ClassNotFoundException {
    Reporter.notYetSupported("check constraints", "MySQL");
    return new ArrayList<CheckConstraint>();
  }
}
