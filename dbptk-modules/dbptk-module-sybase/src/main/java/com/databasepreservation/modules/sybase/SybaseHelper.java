/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.sybase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.modules.SQLHelper;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SybaseHelper extends SQLHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(SybaseHelper.class);

  private String startQuote = "\"";

  private String endQuote = "\"";

  @Override
  public String getStartQuote() {
    return startQuote;
  }

  @Override
  public String getEndQuote() {
    return endQuote;
  }

  @Override
  public String escapeSchemaName(String schema) {
    return getStartQuote() + schema + getEndQuote();
  }

  @Override
  public String escapeTableName(String table) {
    return getStartQuote() + table + getEndQuote();
  }

  @Override
  protected String escapeColumnName(String column) {
    return getStartQuote() + column + getEndQuote();
  }

  @Override
  public String getTriggersSQL(String schemaName, String tableName) {
    return "select c.trigger_name as TRIGGER_NAME, c.trigger_time as ACTION_TIME, c.event as TRIGGER_EVENT, "
      + "c.trigger_defn as TRIGGERED_ACTION, " + "c.remarks as REMARKS " + "FROM systrigger c, systab t, sysuser u "
      + "wHERE c.table_id = t.table_id and t.table_name = '" + tableName
      + "' and u.user_id = t.creator and u.user_name='" + schemaName + "' and c.trigger_name is not null";
  }

  @Override
  public String getCheckConstraintsSQL(String schemaName, String tableName) {
    return "select const.constraint_name as CHECK_NAME, ch.check_defn as CHECK_CONDITION "
      + "from sysconstraint const, systab tab, syscheck ch, sysuser u "
      + "where const.table_object_id = tab.object_id and ch.check_id = " + "const.constraint_id and tab.table_name='"
      + tableName + "' and u.user_id = tab.creator and u.user_name='" + schemaName + "'";
  }

  @Override
  public String getUsersSQL(String dbName) {
    return "SELECT name AS USER_NAME FROM sysusers WHERE uid < 16384";
  }

  public String getViewSQL(String viewName) {
    return "SELECT u.name as SCHEMA_NAME, o.name AS VIEW_NAME, c.text as TEXT "
      + "FROM sysusers u, syscomments c, sysobjects o "
      + "WHERE o.type = 'V' AND o.id = c.id AND o.uid = u.uid AND o.name = '" + viewName + "'";
  }

  public String getProcedureSQL(String procName) {
    return "SELECT u.name as SCHEMA_NAME, o.name AS PROC_NAME, c.text as TEXT "
      + "FROM sysusers u, syscomments c, sysobjects o "
      + "WHERE o.type = 'P' AND o.id = c.id AND o.uid = u.uid AND o.name = '" + procName + "'";
  }
}
