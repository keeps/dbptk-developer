package com.databasepreservation.modules.sybase.in;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.Trigger;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.sybase.SybaseHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SybaseJDBCImportModule extends JDBCImportModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(SybaseJDBCImportModule.class);

  /**
   * Create a new Sybase import module using the default instance.
   *
   * @param database
   *          the name of the database we'll be accessing
   * @param username
   *          the name of the user to use in the connection
   * @param password
   *          the password of the user to use in the connection
   */
  public SybaseJDBCImportModule(String database, String username, String password, String hostname) {
    super("net.sourceforge.jtds.jdbc.Driver",
        "jdbc:jtds:sybase://" + hostname + "/" + database + ";user=" + username + ";password=" + password,
        new SybaseHelper(), new SybaseDataTypeImporter());
  }

  /**
   * Create a new Sybase import module using the default instance.
   *
   * @param port
   *          the port that sybase is listening
   * @param database
   *          the name of the database we'll be accessing
   * @param username
   *          the name of the user to use in the connection
   * @param password
   *          the password of the user to use in the connection
   */
  public SybaseJDBCImportModule(int port, String database, String username, String password, String hostname) {
    super("net.sourceforge.jtds.jdbc.Driver",
        "jdbc:jtds:sybase://" + hostname + ":" + port + "/" + database + ";user=" + username + ";password=" + password,
        new SybaseHelper(), new SybaseDataTypeImporter());
  }

  @Override
  protected Set<String> getIgnoredImportedSchemas() {
    Set<String> ignored = new HashSet<String>();
    ignored.add("js_.*");
    ignored.add("dtm_tm_role");
    ignored.add("guest");
    ignored.add("ha.*");
    ignored.add("keycustodian_role");
    ignored.add("messaging_role");
    ignored.add("mon_role");
    ignored.add("navigator_role");
    ignored.add("oper_role");
    ignored.add("public");
    ignored.add("replication_maint_role_gp");
    ignored.add("replication_role");
    ignored.add("sa_role");
    ignored.add("sso_role");
    ignored.add("sybase_ts_role");
    ignored.add("usedb_user");
    ignored.add("webservices_role");
    return ignored;
  }

  @Override
  protected Statement getStatement() throws SQLException, ModuleException {
    if (statement == null) {
      statement = getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }
    return statement;
  }

  @Override
  protected Cell rawToCellSimpleTypeBinary(String id, String columnName, Type cellType, ResultSet rawData)
      throws SQLException, ModuleException {
    Cell cell;

    InputStream binaryStream = rawData.getBinaryStream(columnName);

    if (binaryStream != null && !rawData.wasNull()) {
      cell = new BinaryCell(id, binaryStream);
    } else {
      cell = new NullCell(id);
    }
    return cell;
  }

  /**@Override
  protected List<Trigger> getTriggers(String schemaName, String tableName) throws ModuleException {
    List<Trigger> triggers = new ArrayList<Trigger>();

    String query = sqlHelper.getTriggersSQL(schemaName, tableName);
    if (query != null) {
      try (ResultSet rs = getStatement().executeQuery(sqlHelper.getTriggersSQL(schemaName, tableName))) {
        while (rs.next()) {
          Trigger trigger = new Trigger();

          String triggerName;
          try {
            triggerName = rs.getString("TRIGGER_NAME");
          } catch (SQLException e) {
            LOGGER.debug("handled SQLException", e);
            triggerName = "";
          }
          trigger.setName(triggerName);

          /*String actionTime;
          try {
            actionTime = processActionTime(rs.getString("ACTION_TIME"));
          } catch (SQLException e) {
            LOGGER.debug("handled SQLException", e);
            actionTime = "";
          }
          trigger.setActionTime(actionTime);

          String triggerEvent = "";

          String queryForTriggerEvent = ((SybaseHelper) sqlHelper).getTriggerEventSQL(schemaName, tableName);

          try (ResultSet res = getStatement().executeQuery(queryForTriggerEvent)) {
            while(res.next()) {
              String triggerInsert = "";
              String triggerDelete = "";
              String triggerUpdate = "";

              triggerInsert = res.getString("TRIGGER_INS");
              triggerDelete = res.getString("TRIGGER_DEL");
              triggerUpdate = res.getString("TRIGGER_UPD");

              List<String> triggerEvents = new ArrayList<>();

              if (triggerInsert != null) triggerEvents.add("INSERT");
              if (triggerDelete != null) triggerEvents.add("DELETE");
              if (triggerUpdate != null) triggerEvents.add("UPDATE");

              StringBuilder sb = new StringBuilder();

              for (String s : triggerEvents) {
                sb.append(s).append(",");
              }

              triggerEvent = sb.deleteCharAt(sb.length() - 1).toString();
            }
          } catch (SQLException e) {
            LOGGER.error("Error getting trigger event for " + tableName, e);
          }

          System.out.println(triggerEvent);

          trigger.setTriggerEvent(triggerEvent);

          String triggeredAction;
          try {
            triggeredAction = rs.getString("TRIGGERED_ACTION");
          } catch (SQLException e) {
            LOGGER.debug("handled SQLException", e);
            triggeredAction = "";
          }
          trigger.setTriggeredAction(triggeredAction);

          String description;
          try {
            description = rs.getString("REMARKS");
          } catch (SQLException e) {
            LOGGER.debug("handled SQLException", e);
            description = null;
          }
          if (description != null) {
            trigger.setDescription(description);
          }

          triggers.add(trigger);
        }
      } catch (SQLException e) {
        LOGGER.debug("No triggers imported for " + schemaName + "." + tableName, e);
      }
    } else {
      LOGGER.debug("Triggers were not imported: not supported yet on " + getClass().getSimpleName());
    }
    return triggers;
  }
  */
}

