package com.databasepreservation.modules.sybase;

import com.databasepreservation.modules.SQLHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SybaseHelper extends SQLHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(SybaseHelper.class);

  @Override
  public String getTriggersSQL(String schemaName, String tableName) {
    return "SELECT so2.name AS TRIGGER_NAME FROM sysobjects so1, sysobjects so2 WHERE (so2.id = so1.deltrig OR so2.id = so1.instrig OR so2.id=so1.updtrig OR so2.id=so1.seltrig) AND so1.name='" + tableName + "'";
  }

  @Override
  public String getCheckConstraintsSQL(String schemaName, String tableName) {
    return "SELECT object_name(sys_const.tableid) AS TABLE_NAME, object_name(sys_const.constrid) as CHECK_NAME, sys_comm.text as CHECK_CONDITION" +
    " FROM sysconstraints sys_const, syscomments sys_comm" +
    " WHERE sys_const.status=128 AND sys_const.constrid=sys_comm.id AND object_name(sys_const.tableid)='" + tableName + "'";
  }

  @Override
  public String getUsersSQL(String dbName) {
    return "SELECT name AS USER_NAME FROM dbo.sysusers WHERE uid < 16384";
  }

  @Override
  public String getRolesSQL() {
    return "SELECT name as ROLE_NAME FROM master..sysloginroles AS a, master.dbo.syssrvroles as b where a.srid = b.srid";
  }

  public String getTriggerEventSQL(String schemaName, String tableName) {
    return "SELECT object_name(so2.deltrig) AS TRIGGER_DEL, object_name(so2.instrig) AS TRIGGER_INS, object_name(so2.updtrig) AS TRIGGER_UPD " +
        "FROM sysobjects so1, sysobjects so2 WHERE (so2.id = so1.id) AND so1.name='" + tableName + "' AND (object_name(so2.instrig) IS NOT NULL OR object_name(so2.deltrig) IS NOT NULL OR object_name(so2.updtrig) IS NOT NULL)";
  }

  public String getTriggeredActionSQL(String triggerName) {
    return "SELECT u.name as SCHEMA_NAME, o.name AS TRIGGER_NAME, c.text as TRIGGERED_ACTION " +
        "FROM sysusers u, syscomments c, sysobjects o " +
        "WHERE o.type = 'TR' AND o.id = c.id AND o.uid = u.uid AND o.name = '" + triggerName + "'";
  }

  public String getViewSQL(String viewName) {
    return "SELECT u.name as SCHEMA_NAME, o.name AS VIEW_NAME, c.text as TEXT " +
        "FROM sysusers u, syscomments c, sysobjects o " +
        "WHERE o.type = 'V' AND o.id = c.id AND o.uid = u.uid AND o.name = '" + viewName + "'";
  }

  public String getProcedureSQL(String procName) {
    return "SELECT u.name as SCHEMA_NAME, o.name AS PROC_NAME, c.text as TEXT " +
        "FROM sysusers u, syscomments c, sysobjects o " +
        "WHERE o.type = 'P' AND o.id = c.id AND o.uid = u.uid AND o.name = '" + procName + "'";
  }

  public String getRuleNameSQL(String tableName) {
    return "SELECT DISTINCT(sysobjects.name) AS RULE_NAME" +
    " FROM sysobjects, syscolumns, syscomments" +
    " WHERE object_name(syscolumns.domain)=sysobjects.name AND sysobjects.id = syscomments.id" +
    " AND sysobjects.type='R' AND object_name(syscolumns.id)='" + tableName + "'";
  }

  public String getRuleSQL(String tableName, String ruleName) {
    return "SELECT u.name as SCHEMA_NAME, o.name AS RULE_NAME, object_name(syscolumns.id) as TABLE_NAME, c.text as TEXT" +
        " FROM sysusers u, syscomments c, sysobjects o, syscolumns" +
        " WHERE o.type = 'R' AND o.id = c.id AND o.uid = u.uid and object_name(syscolumns.domain)=o.name and object_name(syscolumns.id)='" + tableName + "' and o.name='" + ruleName + "'";
  }
}
