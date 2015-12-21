package com.databasepreservation.modules.oracle.in;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import oracle.jdbc.OracleTypes;

import com.databasepreservation.CustomLogger;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeNumericExact;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.oracle.OracleHelper;

/**
 * Microsoft SQL Server JDBC import module.
 *
 * @author Luis Faria
 */
public class Oracle12cJDBCImportModule extends JDBCImportModule {

  private final CustomLogger logger = CustomLogger.getLogger(Oracle12cJDBCImportModule.class);

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
  public Oracle12cJDBCImportModule(String serverName, int port, String database, String username, String password) {

    super("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:" + username + "/" + password + "@//" + serverName + ":"
      + port + "/" + database, new OracleHelper());

    logger.info("jdbc:oracle:thin:<username>/<password>@//" + serverName + ":" + port + "/" + database);
  }

  @Override
  protected Statement getStatement() throws SQLException, ClassNotFoundException {
    if (statement == null) {
      statement = getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }
    return statement;
  }

  @Override
  protected String getDbName() throws SQLException, ClassNotFoundException {
    return getMetadata().getUserName();
  }

  @Override
  protected List<SchemaStructure> getSchemas() throws SQLException, ClassNotFoundException, UnknownTypeException {
    List<SchemaStructure> schemas = new ArrayList<SchemaStructure>();
    String schemaName = getMetadata().getUserName();
    schemas.add(getSchemaStructure(schemaName, 1));
    return schemas;
  }

  @Override
  protected Type getLongvarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    throw new UnknownTypeException("Unsuported JDBC type, code: -1. Oracle " + typeName
      + " data type is not supported.");
  }

  @Override
  protected Type getDecimalType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;

    // special case when NUMBER is defined without specifying precision nor
    // scale
    if (columnSize == 0 && decimalDigits == -127 && numPrecRadix == 10) {
      type = new SimpleTypeNumericApproximate(columnSize);
      type.setSql99TypeName("DOUBLE PRECISION");
      type.setSql2003TypeName("DOUBLE PRECISION");
    }
    // for all other cases NUMBER is a DECIMAL
    else {
      type = new SimpleTypeNumericExact(columnSize, decimalDigits);
      if (decimalDigits > 0) {
        type.setSql99TypeName("DECIMAL", columnSize, decimalDigits);
        type.setSql2003TypeName("DECIMAL", columnSize, decimalDigits);
      } else {
        type.setSql99TypeName("DECIMAL", columnSize);
        type.setSql2003TypeName("DECIMAL", columnSize);
      }
    }
    return type;
  }

  @Override
  protected Type getOtherType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    Type type;
    // TODO define charset
    if ("NCHAR".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeString(Integer.valueOf(columnSize), false, "CHARSET");
      type.setSql99TypeName("CHARACTER");
      type.setSql2003TypeName("CHARACTER");
    } else if ("NVARCHAR2".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeString(Integer.valueOf(columnSize), true, "CHARSET");
      type.setSql99TypeName("CHARACTER VARYING", columnSize);
      type.setSql2003TypeName("CHARACTER VARYING", columnSize);
    } else if ("NCLOB".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeString(Integer.valueOf(columnSize), true, "CHARSET");
      type.setSql99TypeName("CHARACTER LARGE OBJECT");
      type.setSql2003TypeName("CHARACTER LARGE OBJECT");
    } else if ("ROWID".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeString(Integer.valueOf(columnSize), true);
      type.setSql99TypeName("CHARACTER VARYING", columnSize);
      type.setSql2003TypeName("CHARACTER VARYING", columnSize);
    } else if ("UROWID".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeString(Integer.valueOf(columnSize), true);
      type.setSql99TypeName("CHARACTER VARYING", columnSize);
      type.setSql2003TypeName("CHARACTER VARYING", columnSize);
    } else {
      // try to get everything else as string
      type = new SimpleTypeString(65535, true);
      type.setSql99TypeName("CHARACTER LARGE OBJECT");
      type.setSql2003TypeName("CHARACTER LARGE OBJECT");
      // type = super.getOtherType(dataType, typeName, columnSize,
      // decimalDigits, numPrecRadix);
    }
    return type;
  }

  @Override
  protected Type getSpecificType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    Type type;
    switch (dataType) {
      case OracleTypes.BINARY_DOUBLE:
        type = new SimpleTypeNumericApproximate(Integer.valueOf(columnSize));
        type.setSql99TypeName("BIT VARYING", columnSize); // todo: not sure if
                                                          // columnSize is the
                                                          // correct value here
        type.setSql2003TypeName("BIT VARYING", columnSize); // todo: not sure if
                                                            // columnSize is the
                                                            // correct value
                                                            // here
        break;
      case OracleTypes.BINARY_FLOAT:
        type = new SimpleTypeNumericApproximate(Integer.valueOf(columnSize));
        type.setSql99TypeName("BIT VARYING", columnSize); // todo: not sure if
                                                          // columnSize is the
                                                          // correct value here
        type.setSql2003TypeName("BIT VARYING", columnSize); // todo: not sure if
                                                            // columnSize is the
                                                            // correct value
                                                            // here
        break;
      // TODO add support to BFILEs
      // case OracleTypes.BFILE:
      // type = new SimpleTypeBinary();
      // break;
      case OracleTypes.TIMESTAMPTZ:
        type = new SimpleTypeDateTime(true, true);
        type.setSql99TypeName("TIMESTAMP");
        type.setSql2003TypeName("TIMESTAMP");
        break;
      case OracleTypes.TIMESTAMPLTZ:
        type = new SimpleTypeDateTime(true, true);
        type.setSql99TypeName("TIMESTAMP");
        type.setSql2003TypeName("TIMESTAMP");
        break;
      default:
        type = super.getSpecificType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
        break;
    }
    return type;
  }

  @Override
  protected String processActionTime(String string) {
    String[] parts = string.split("\\s+");
    String res = parts[0];
    if ("INSTEAD".equalsIgnoreCase(res)) {
      res += " OF";
    }
    return res;
  }
}
