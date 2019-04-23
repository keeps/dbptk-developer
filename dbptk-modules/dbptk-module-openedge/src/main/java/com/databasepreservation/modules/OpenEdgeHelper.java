package com.databasepreservation.modules;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class OpenEdgeHelper extends  SQLHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenEdgeHelper.class);

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
}
