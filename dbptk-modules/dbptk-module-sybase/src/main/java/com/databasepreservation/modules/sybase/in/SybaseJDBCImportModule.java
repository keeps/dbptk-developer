package com.databasepreservation.modules.sybase.in;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.*;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.CloseableUtils;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.sybase.SybaseHelper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  /**
   * Gets the check constraints of a given schema table
   *
   * @param schemaName
   * @param tableName
   * @return
   */
  protected List<CheckConstraint> getCheckConstraints(String schemaName, String tableName) throws ModuleException {
    List<CheckConstraint> checkConstraints = super.getCheckConstraints(schemaName, tableName);

    /* Sybase Rule Binding */
    String queryForRuleNameSQL = ((SybaseHelper) sqlHelper).getRuleNameSQL(tableName);
    try (ResultSet res = getStatement().executeQuery(queryForRuleNameSQL)) {
      while(res.next()) {
        CheckConstraint c = new CheckConstraint();
        String ruleName = res.getString("RULE_NAME");
        String queryForRuleSQL = ((SybaseHelper) sqlHelper).getRuleSQL(tableName, ruleName);

        try (ResultSet rset = getStatement().executeQuery(queryForRuleSQL)) {
          StringBuilder b = new StringBuilder();
          while (rset.next()) {
            b.append(rset.getString("TEXT"));
          }

          c.setName(ruleName);
          c.setCondition(b.toString());
          c.setDescription("Sybase Rule");

          checkConstraints.add(c);
        }
      }
    } catch (SQLException e) {
      LOGGER.debug("Exception trying to get rule SQL in Sybase", e);
    }

    return checkConstraints;
  }

  @Override
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

          trigger.setTriggerEvent(triggerEvent);

          String triggeredAction = "";

          String queryForTriggeredAction = ((SybaseHelper) sqlHelper).getTriggeredActionSQL(triggerName);

          try (ResultSet res = getStatement().executeQuery(queryForTriggeredAction)) {
            StringBuilder b = new StringBuilder();
            while (res.next()) {
              b.append(res.getString("TRIGGERED_ACTION"));
            }

            triggeredAction = b.toString();
          } catch(SQLException | NullPointerException e) {
            LOGGER.error("Error getting triggered action for " + triggerName, e);
            triggeredAction = "";
          }

          trigger.setTriggeredAction(triggeredAction);

          String actionTime;
          Pattern pattern;
          Matcher matcher;

          pattern = Pattern.compile("create trigger (\\S+) on (\\S+) (for|instead of)", Pattern.CASE_INSENSITIVE);

          matcher = pattern.matcher(triggeredAction.replace('\n', ' '));

          if (matcher.find()) {
            String parsedTriggerName = matcher.group(1);
            String parsedTableName = matcher.group(2);
            String parsedActionTime = matcher.group(3);

            actionTime = parsedActionTime.equalsIgnoreCase("for")? "AFTER" : "INSTEAD OF";

          } else {
            actionTime = "";
          }

          trigger.setActionTime(actionTime);

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

  @Override
  protected List<ViewStructure> getViews(String schemaName) throws SQLException, ModuleException {
    List<ViewStructure> views = super.getViews(schemaName);
    for (ViewStructure v : views) {

      String queryForViewSQL = ((SybaseHelper) sqlHelper).getViewSQL(v.getName());
      ResultSet rset = null;
      PreparedStatement statement = null;
      statement = getConnection().prepareStatement(queryForViewSQL);
      try {
        rset = statement.executeQuery();
        StringBuilder b = new StringBuilder();
        while (rset.next()) {
          b.append(rset.getString("TEXT"));
        }

        v.setQueryOriginal(b.toString());
      } catch (SQLException e) {
        LOGGER.debug("Exception trying to get view SQL in Sybase", e);
      } finally {
        CloseableUtils.closeQuietly(rset);
        CloseableUtils.closeQuietly(statement);
      }
    }
    return views;
  }

  @Override
  protected List<RoutineStructure> getRoutines(String schemaName) throws SQLException, ModuleException {
    List<RoutineStructure> routines = new ArrayList<RoutineStructure>();

    ResultSet rset = getMetadata().getProcedures(dbStructure.getName(), schemaName, "%");
    while (rset.next()) {
      String routineName = rset.getString(3);
      RoutineStructure routine = new RoutineStructure();
      routine.setName(routineName);
      if (rset.getString(7) != null) {
        routine.setDescription(rset.getString(7));
      } else {
        if (rset.getShort(8) == 1) {
          routine.setDescription("Routine does not return a result");
        } else if (rset.getShort(8) == 2) {
          routine.setDescription("Routine returns a result");
        }
      }
      String queryForProcedureSQL = ((SybaseHelper) sqlHelper).getProcedureSQL(routineName);
      try (ResultSet res = getStatement().executeQuery(queryForProcedureSQL)) {
        StringBuilder b = new StringBuilder();
        while (res.next()) {
          b.append(res.getString("TEXT"));
        }
        routine.setBody(b.toString());
      } catch (SQLException e) {
        LOGGER.debug("Could not retrieve routine code (as routine).", e);
        routine.setBody("");
      }

      routines.add(routine);
    }
    return routines;
  }
}