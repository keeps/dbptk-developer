/**
 *
 */
package com.databasepreservation.modules.sqlServer;

import com.databasepreservation.CustomLogger;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeNumericExact;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.SQLHelper;

/**
 * SQL Server 2005 Helper
 *
 * @author Luis Faria
 */
public class SQLServerHelper extends SQLHelper {

  private final CustomLogger logger = CustomLogger.getLogger(SQLServerHelper.class);

  private String startQuote = "[";

  private String endQuote = "]";

  public String getStartQuote() {
    return startQuote;
  }

  public String getEndQuote() {
    return endQuote;
  }

  protected String createTypeSQL(Type type, boolean isPkey, boolean isFkey) throws UnknownTypeException {
    String ret = null;
    if (type instanceof SimpleTypeString) {
      SimpleTypeString string = (SimpleTypeString) type;
      if (string.isLengthVariable()) {
        if (string.getLength().intValue() > 8000) {
          if (isPkey) {
            ret = "varchar(8000)";
            logger.warn("Resizing column length to 8000" + " so it can be a primary key");
          } else {
            ret = "text";
          }
        } else {
          ret = "varchar(" + string.getLength() + ")";
        }
      } else {
        if (string.getLength().intValue() > 8000) {
          ret = "text";
        } else {
          ret = "char(" + string.getLength() + ")";
        }
      }
    } else if (type instanceof SimpleTypeBoolean) {
      ret = "bit";
    } else if (type instanceof SimpleTypeNumericExact) {
      String sql99TypeName = type.getSql99TypeName();
      Integer precision = ((SimpleTypeNumericExact) type).getPrecision();
      Integer scale = ((SimpleTypeNumericExact) type).getScale();
      if (sql99TypeName.equals("INTEGER")) {
        ret = "int";
      } else if (sql99TypeName.equals("SMALLINT")) {
        ret = "smallint";
      } else {
        ret = "decimal(";
        int min = Math.min(precision, 28);
        ret += min;
        if (scale > 0) {
          ret += "," + (scale - precision + min);
        }
        ret += ")";
      }
    } else if (type instanceof SimpleTypeDateTime) {
      String sql99TypeName = type.getSql99TypeName();
      if (sql99TypeName.equals("TIME")) {
        ret = "time";
      } else if (sql99TypeName.equals("DATE")) {
        ret = "date";
      } else if (sql99TypeName.equals("TIMESTAMP")) {
        ret = "datetime2";
      } else {
        logger.warn("Using string instead of datetime type because "
          + "SQL Server doesn't support dates before 1753-01-01");
        ret = "char(23)";
      }
    } else if (type instanceof SimpleTypeBinary) {
      SimpleTypeBinary binType = (SimpleTypeBinary) type;
      String sql99TypeName = binType.getSql99TypeName();
      if (sql99TypeName.startsWith("BIT")) {
        String dataType = null;
        if (sql99TypeName.equals("BIT")) {
          logger.debug("is BIT");
          dataType = "binary";
        } else {
          dataType = "varbinary";
        }
        Integer length = binType.getLength();
        Integer bytes = (((length / 8.0) % 1 == 0) ? (length / 8) : ((length / 8) + 1));

        if (dataType.equals("varbinary") && bytes <= 0) {
          ret = "varbinary(max)";
        } else if (bytes > 0 && bytes <= 8000) {
          ret = dataType + "(" + bytes + ")";
        } else {
          ret = "image";
        }
      } else if (sql99TypeName.equals("BINARY LARGE OBJECT")) {
        ret = "image";
      } else {
        ret = "image";
      }
    } else {
      ret = super.createTypeSQL(type, isPkey, isFkey);
    }
    return ret;
  }

  @Override
  protected String escapeTableName(String table) {
    return "[" + table + "]";
  }

  @Override
  public String getCheckConstraintsSQL(String schemaName, String tableName) {
    return "SELECT cc.CONSTRAINT_NAME AS CHECK_NAME, " + "cc.CHECK_CLAUSE AS CHECK_CONDITION "
      + "FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc " + "INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c "
      + "ON cc.CONSTRAINT_NAME = c.CONSTRAINT_NAME " + "WHERE c.TABLE_NAME = '" + tableName + "' "
      + "AND c.TABLE_SCHEMA = '" + schemaName + "'";

  }

  public String getTriggersSQL(String schemaName, String tableName) {
    return "SELECT o.name AS TRIGGER_NAME, " + "CAST(OBJECTPROPERTY(id, 'ExecIsAfterTrigger') AS char(1)) "
      + "+ CAST(OBJECTPROPERTY(id, 'ExecIsInsteadOfTrigger') " + "AS char(1)) AS ACTION_TIME, "
      + "CAST(OBJECTPROPERTY(id, 'ExecIsInsertTrigger') AS char(1)) "
      + "+ CAST(OBJECTPROPERTY(id, 'ExecIsUpdateTrigger') AS char(1))"
      + "+ CAST(OBJECTPROPERTY(id, 'ExecIsDeleteTrigger') AS char(1))" + "AS TRIGGER_EVENT, "
      + "OBJECT_DEFINITION(o.id) AS TRIGGERED_ACTION " + "FROM sysobjects o "
      + "INNER JOIN sys.tables tab ON o.parent_obj = tab.object_id "
      + "INNER JOIN sys.schemas s ON tab.schema_id = s.schema_id " + "WHERE o.type = 'TR' AND tab.name = '" + tableName
      + "' " + "AND s.name='" + schemaName + "';";
  }

  @Override
  public String getUsersSQL(String dbName) {
    return "SELECT suser_sname(owner_sid) AS USER_NAME FROM sys.databases " + "WHERE name = '" + dbName + "'";
  }

  @Override
  public String getRolesSQL() {
    return "SELECT name AS ROLE_NAME FROM sysusers WHERE issqlrole = 1";
  }

  @Override
  public String getDatabases(String database) {
    // TODO test
    return "EXEC sp_databases;";
  }
}
