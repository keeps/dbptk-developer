/**
 *
 */
package com.databasepreservation.modules.mySql.in;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.UserStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.mySql.MySQLHelper;

/**
 * @author Luis Faria
 */
public class MySQLJDBCImportModule extends JDBCImportModule {

  private final Logger logger = Logger.getLogger(MySQLJDBCImportModule.class);

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
      + "&password=" + password, new MySQLHelper());
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
      + "&password=" + password, new MySQLHelper());
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
    List<UserStructure> users = new ArrayList<UserStructure>();
    ResultSet rs = getStatement().executeQuery(sqlHelper.getUsersSQL(null));
    while (rs.next()) {
      UserStructure user = new UserStructure(rs.getString(2) + "@" + rs.getString(1), null);
      users.add(user);
    }

    return users;
  }

  @Override
  protected ResultSet getTableRawData(String tableId) throws SQLException, ClassNotFoundException, ModuleException {
    logger.debug("query: " + sqlHelper.selectTableSQL(tableId));

    Statement statement = getStatement();
    statement.setFetchSize(Integer.MIN_VALUE);

    ResultSet set = statement.executeQuery(sqlHelper.selectTableSQL(tableId));
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
  protected Type getRealType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;

    if (columnSize == 12 && decimalDigits == 0) {
      type = new SimpleTypeNumericApproximate(columnSize);
      type.setSql99TypeName("REAL");
    } else {
      type = getDecimalType(typeName, columnSize, decimalDigits, numPrecRadix);
    }

    return type;
  }

  @Override
  protected Type getDoubleType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;

    if (columnSize == 22 && decimalDigits == 0) {
      type = new SimpleTypeNumericApproximate(columnSize);
      type.setSql99TypeName("DOUBLE PRECISION");
    } else {
      type = getDecimalType(typeName, columnSize, decimalDigits, numPrecRadix);
    }

    return type;
  }

  @Override
  protected Type getBinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeBinary(columnSize);

    if (typeName.equalsIgnoreCase("TINYBLOB")) {
      type.setSql99TypeName("BIT VARYING(2040)");
    } else if (typeName.equalsIgnoreCase("BIT")) {
      type.setSql99TypeName("BIT", columnSize);
      type.setOriginalTypeName(typeName, columnSize);
    } else {
      type.setSql99TypeName("BIT", columnSize * 8);
    }

    return type;
  }

  @Override
  protected Type getVarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeBinary(columnSize);
    type.setSql99TypeName("BIT VARYING", columnSize * 8);
    return type;
  }

  @Override
  protected List<ViewStructure> getViews(String schemaName) throws SQLException, ClassNotFoundException,
    UnknownTypeException {
    List<ViewStructure> views = super.getViews(schemaName);
    for (ViewStructure v : views) {
      Statement statement = getConnection().createStatement();
      String query = "SHOW CREATE VIEW " + v.getName();
      ResultSet rset = statement.executeQuery(query);
      rset.next(); // Returns only one tuple

      // TO-DO: the string given below by rset.getString(2) has to be parsed a
      // little before it is set to as the view
      v.setQueryOriginal(rset.getString(2));
    }
    return views;
  }

  @Override
  protected Type getFloatType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;

    if (columnSize == 12 && decimalDigits == 0) {
      type = new SimpleTypeNumericApproximate(columnSize);
      type.setSql99TypeName("FLOAT");
    } else {
      type = getDecimalType(typeName, columnSize, decimalDigits, numPrecRadix);
    }

    return type;
  }

  @Override
  protected Type getDateType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    if (typeName.equals("YEAR")) {
      return getNumericType(typeName, 4, decimalDigits, numPrecRadix);
    } else {
      return super.getDateType(typeName, columnSize, decimalDigits, numPrecRadix);
    }
  }

  @Override
  protected Cell rawToCellSimpleTypeNumericExact(String id, String columnName, Type cellType, ResultSet rawData)
    throws SQLException {
    if (cellType.getOriginalTypeName().equals("YEAR")) {
      // for inputs 15, 2015, 99 and 1999
      // rawData.getInt returns numbers like 15, 2015, 99, 1999
      // rawData.getString returns dates like 2015-01-01, 2015-01-01,
      // 1999-01-01, 1999-01-01
      // to get the "real" year value, using the first 4 characters from the
      // date string
      return new SimpleCell(id, rawData.getString(columnName).substring(0, 4));
    } else {
      return super.rawToCellSimpleTypeNumericExact(id, columnName, cellType, rawData);
    }
  }
}
