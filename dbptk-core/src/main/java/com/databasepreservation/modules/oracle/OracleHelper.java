package com.databasepreservation.modules.oracle;

import com.databasepreservation.modules.SQLHelper;

/**
 * @author Miguel Coutada
 */

public class OracleHelper extends SQLHelper {

  // private final Logger logger = Logger.getLogger(OracleHelper.class);

  private String startQuote = "";

  private String endQuote = "";

  public String getStartQuote() {
    return startQuote;
  }

  public String getEndQuote() {
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

}
