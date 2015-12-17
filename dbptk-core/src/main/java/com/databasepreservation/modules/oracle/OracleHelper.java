package com.databasepreservation.modules.oracle;

import com.databasepreservation.CustomLogger;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.SQLHelper;

/**
 * @author Miguel Coutada
 */

public class OracleHelper extends SQLHelper {

  private static final int MAX_SIZE_VARCHAR = 4000;
  private static final int MAX_SIZE_CHAR = 2000;

  private final CustomLogger logger = CustomLogger.getLogger(OracleHelper.class);

  private String startQuote = "";

  private String endQuote = "";

  @Override public String getStartQuote() {
    return startQuote;
  }

  @Override public String getEndQuote() {
    return endQuote;
  }

  @Override
  public String getCheckConstraintsSQL(String schemaName, String tableName) {
    return "SELECT constraint_name AS CHECK_NAME, " + "search_condition AS CHECK_CONDITION " + "FROM all_constraints "
      + "WHERE table_name = '" + tableName + "' AND constraint_type = 'C'";
  }

  @Override
  public String getTriggersSQL(String schemaName, String tableName) {
    return "SELECT TRIGGER_NAME, TRIGGER_TYPE AS ACTION_TIME, " + "TRIGGERING_EVENT AS TRIGGER_EVENT, "
      + "TRIGGER_BODY AS TRIGGERED_ACTION " + "FROM ALL_TRIGGERS " + "WHERE TABLE_NAME = '" + tableName + "'";
  }

  @Override
  public String getUsersSQL(String dbName) {
    return "SELECT username AS USER_NAME FROM ALL_USERS";
  }

  @Override
  public String getRolesSQL() {
    return "SELECT role AS ROLE_NAME FROM session_roles";
  }

  @Override
  public String getDatabases(String database) {
    return null;
  }

  @Override
  public String createPrimaryKeySQL(String tableId, PrimaryKey pkey) throws ModuleException {
    PrimaryKey replacementPkey = pkey;
    // if the name is primary, conflicts will occur. avoid creating a named
    // constraint for those cases
    if ("primary".equalsIgnoreCase(pkey.getName())) {
      replacementPkey = new PrimaryKey(null, pkey.getColumnNames(), pkey.getDescription());
    }
    return super.createPrimaryKeySQL(tableId, replacementPkey);
  }

  @Override
  protected String createTypeSQL(Type type, boolean isPrimaryKey, boolean isForeignKey) throws UnknownTypeException {
    String ret;
    if (type instanceof SimpleTypeString) {
      SimpleTypeString string = (SimpleTypeString) type;
      if (string.isLengthVariable()) {
        if (string.getLength() > MAX_SIZE_VARCHAR) {
          ret = "clob";
        } else {
          ret = "varchar(" + string.getLength() + ")";
        }
      } else {
        if (string.getLength() > MAX_SIZE_CHAR) {
          ret = "clob";
        } else {
          ret = "char(" + string.getLength() + ")";
        }
      }
    } else if (type instanceof SimpleTypeBoolean) {
      ret = "NUMBER(1)";
    } else {
      ret = super.createTypeSQL(type, isPrimaryKey, isForeignKey);
    }
    return ret;
  }
}
