/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class OpenEdgeHelper extends  SQLHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenEdgeHelper.class);

  public String getViewSQL(String viewName) {
    return "SELECT \"_Viewtext\" AS TEXT FROM PUB.\"_Sysviews\" WHERE \"_Viewname\" ='" + viewName + "'";
  }

  public String getTableDescription(String schema, String table) {
    return "SELECT \"_File-Name\" AS TABLE_NAME, \"_Desc\" AS DESCRIPTION, \"_Owner\" AS DB_SCHEMA" +
        " FROM PUB.\"_File\" " +
        " WHERE \"_Tbl-Type\" = 'T' AND \"_File-Name\" = '" + table + "' AND \"_Owner\" = '" + schema + "'";
  }

  public String getColumnDescription(String schema, String table, String column) {
    return "SELECT F.\"_Field-Name\" AS COLUMN_NAME, F.\"_Desc\" AS DESCRIPTION " +
        " FROM PUB.\"_Field\" AS F, PUB.\"_File\" AS T " +
        " WHERE T.\"_TBL-Type\" = 'T' AND F.\"_File-recid\" = T.rowid AND T.\"_File-Name\" = '" + table + "' AND F.\"_Field-Name\" = '" + column + "' AND T.\"_Owner\" = '" + schema + "'";
  }

  public String getTriggersSQL(String schema, String table) {
    return "SELECT T.\"_Triggername\" AS TRIGGER_NAME, T.\"_Triggertime\" AS ACTION_TIME, T.\"_Triggerevent\" AS TRIGGER_EVENT" +
        " FROM PUB.\"_Systrigger\" AS T" +
        " WHERE T.\"_Tbl\" = '" + table + "' AND T.\"_Owner\" = '" + schema + "'";
  }

  public String getTriggerInfoSQL(String schemaName, String tableName, String triggerName) {
    return "SELECT T.\"_Triggerid\" AS TRIGGER_ID, T.\"_Referstonew\" AS REF_NEW, T.\"_Referstoold\" AS REF_OLD, T.\"_Statementorrow\" AS STAT_OR_ROW" +
        " FROM PUB.\"_Systrigger\" T" +
        " WHERE T.\"_Triggername\" = '" + triggerName + "' AND T.\"_Owner\" = '" + schemaName + "' AND T.\"_Tbl\" ='" + tableName + "'";
  }

  public String getTriggeredActionSQL(String triggerID) {
    return "SELECT text.\"_Proctext\" as TEXT" +
        " FROM PUB.\"_Sysproctext\" text" +
        " WHERE text.\"_Id\" =" + triggerID +
        " ORDER BY text.\"_Seq\" ASC";
  }

  public String getTriggerColumnsSQL(String triggerID) {
    return "SELECT tableColumn.\"_field-rpos\" AS COLUMN_POSITION, tableColumn.\"_Field-Name\" AS COLUMN_NAME" +
      " FROM PUB.\"_Field\" AS tableColumn, PUB.\"_File\" AS tbl, PUB.\"_Systrigcols\" AS trigColumns, PUB.\"_Systrigger\" triggers" +
      " WHERE tbl.\"_File-Name\" =  triggers.\"_Tbl\""  +
      " AND tableColumn.\"_File-recid\" = tbl.rowid" +
      " AND tbl.\"_Owner\" = triggers.\"_Owner\"" +
      " AND triggers.\"_Triggerid\" = " + triggerID +
      " AND triggers.\"_Triggername\" = trigColumns.\"_Triggername\"" +
      " AND trigColumns.\"_Colid\" = tableColumn.\"_field-rpos\"";
  }

  public String getProcedureParametersSQL(String routineName) {
    return "SELECT procColumns.\"_Procid\" as PROC_ID, procColumns.\"_Col\" AS COLUMN_NAME, procColumns.\"_Datatype\" AS DATA_TYPE, procColumns.\"_Width\" AS LENGTH, procColumns.\"_Argtype\" AS PROC_MODE" +
        " FROM PUB.\"_Sysproccolumns\" AS procColumns, PUB.\"_Sysprocedures\" proc" +
        " WHERE procColumns.\"_Procid\" = proc.\"_Procid\"" +
        " AND proc.\"_Procname\" = '" + routineName + "'";
  }

  public String getProcedureSourceSQL(String schemaName, String routineName) {
    return "SELECT SYS_PROCTEXT.\"_Proctext\" AS TEXT" +
        " FROM PUB.\"_Sysproctext\" AS SYS_PROCTEXT, PUB.\"_Sysprocedures\" AS SYS_PROC" +
        " WHERE SYS_PROC.\"_Procid\" = SYS_PROCTEXT.\"_Id\"" +
        " AND SYS_PROC.\"_Owner\" = '" + schemaName + "'" +
        " AND SYS_PROC.\"_Procname\" = '" + routineName + "'" +
        " ORDER BY SYS_PROCTEXT.\"_Seq\"";
  }

  @Override
  public String getCheckConstraintsSQL(String schemaName, String tableName) {
    return "SELECT chk.\"_Tblname\" AS TABLE_NAME, chk.\"_Cnstrname\" AS CHECK_NAME, chk.\"_Chkclause\" AS CHECK_CONDITION" +
        " FROM PUB.\"_Syschkconstrs\" AS chk" +
        " WHERE chk.\"_Tblname\" = '" + tableName + "'" +
        " AND chk.\"_Owner\" = '" + schemaName + "'";
  }

  @Override
  public String getUsersSQL(String dbName) {
    return "SELECT SYS_USR.GRANTEE AS USER_NAME FROM SYSPROGRESS.SYSDBAUTH AS SYS_USR";
  }

  @Override
  public String getRolesSQL() {
    return "SELECT ROLENAME AS ROLE_NAME, ADMIN FROM SYSPROGRESS.SYSROLES";
  }
}
