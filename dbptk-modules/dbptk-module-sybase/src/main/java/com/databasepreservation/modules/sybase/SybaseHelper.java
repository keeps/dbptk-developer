package com.databasepreservation.modules.sybase;

import com.databasepreservation.modules.SQLHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SybaseHelper extends SQLHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(SybaseHelper.class);

  /*@Override
  public String getTriggersSQL(String schemaName, String tableName) {
    return "select so2.name as TRIGGER_NAME from sysobjects so1, sysobjects so2 where (so2.id = so1.deltrig or so2.id = so1.instrig or so2.id=so1.updtrig or so2.id=so1.seltrig) and so1.name='" + tableName +"'";
  }*/

  public String getTriggerEventSQL(String schemaName, String tableName) {
    return "select object_name(so2.deltrig) as TRIGGER_DEL, object_name(so2.instrig) as TRIGGER_INS, object_name(so2.updtrig) as TRIGGER_UPD " +
        "from sysobjects so1, sysobjects so2 where (so2.id = so1.id) and so1.name='" + tableName + "' and (object_name(so2.instrig) is not null or object_name(so2.deltrig) is not null or object_name(so2.updtrig) is not null)";
  }
}
