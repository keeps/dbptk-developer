/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.sybase.in;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.CheckConstraint;
import com.databasepreservation.model.structure.RoleStructure;
import com.databasepreservation.model.structure.RoutineStructure;
import com.databasepreservation.model.structure.Trigger;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.CloseableUtils;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.sybase.SybaseHelper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Path;
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
   * Creates a new Sybase import module using the default instance.
   * @param hostname
   *          the name of the Sybase server host (e.g. localhost)
   * @param database
   *          the name of the database we'll be accessing
   * @param username
   *          the name of the user to use in the connection
   * @param password
   *          the password of the user to use in the connection
   */
  public SybaseJDBCImportModule(String hostname, String database, String username, String password, Path customViews) throws ModuleException {
    super("net.sourceforge.jtds.jdbc.Driver",
        "jdbc:jtds:sybase://" + hostname + "/" + database + ";user=" + username + ";password=" + password,
        new SybaseHelper(), new SybaseDataTypeImporter(), customViews);
  }

  /**
   * Creates a new Sybase import module using the default instance.
   * @param hostname
   *          the name of the Sybase server host (e.g. localhost)
   * @param port
   *          the port that sybase is listening
   * @param database
   *          the name of the database we'll be accessing
   * @param username
   *          the name of the user to use in the connection
   * @param password
   *          the password of the user to use in the connection
   *
   */
  public SybaseJDBCImportModule(String hostname, int port, String database, String username, String password, Path customViews) throws ModuleException {
    super("net.sourceforge.jtds.jdbc.Driver",
        "jdbc:jtds:sybase://" + hostname + ":" + port + "/" + database + ";user=" + username + ";password=" + password,
        new SybaseHelper(), new SybaseDataTypeImporter(), customViews);
  }

  /**
   * Gets schemas that won't be imported
   * <p>
   * Accepts schema names in as regular expressions I.e. SYS.* will ignore
   * SYSCAT, SYSFUN, etc
   *
   * @return the schema names not to be imported
   */
  @Override
  protected Set<String> getIgnoredImportedSchemas() {
    Set<String> ignored = new HashSet<String>();
    ignored.add("js_.*");
    ignored.add("dtm_tm_role");
    ignored.add("ha.*");
    ignored.add("keycustodian_role");
    ignored.add("messaging_role");
    ignored.add("mon_role");
    ignored.add("navigator_role");
    ignored.add("oper_role");
    ignored.add("replication_maint_role_gp");
    ignored.add("replication_role");
    ignored.add("sa_role");
    ignored.add("sso_role");
    ignored.add("sybase_ts_role");
    ignored.add("webservices_role");
    return ignored;
  }

  /**
   * Returns a {@link Statement} with specific {@link ResultSet} options
   *
   * @return A {@link Statement}
   * @throws SQLException
   * @throws ModuleException
   */
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
   * Sybase rules (http://infocenter.sybase.com/help/index.jsp?topic=/com.sybase.infocenter.dc32300.1570/html/sqlug/X50891.htm)
   * were also treated as check constraints due to its definition.
   *
   * @param schemaName
   * @param tableName
   * @return A list of {@link CheckConstraint}
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
          c.setDescription("Sybase Rule"); // Add on description the source of the constraint

          checkConstraints.add(c);
        }
      }
    } catch (SQLException e) {
      LOGGER.debug("Exception trying to get rule SQL in Sybase", e);
    }

    return checkConstraints;
    
  }

  /**
   * Gets the database roles
   *
   * @return A list of {@link RoleStructure}
   * @throws SQLException
   * @throws ModuleException
   */
  @Override
  protected List<RoleStructure> getRoles() throws SQLException, ModuleException {
    List<RoleStructure> roles = super.getRoles();

    for (RoleStructure role : roles) {
      role.setAdmin("sa");
    }

    return roles;
  }

  /**
   * Gets the triggers of a given schema table
   *
   * @param schemaName
   * @param tableName
   * @return A list of {@link Trigger}
   * @throws ModuleException
   */
  @Override
  protected List<Trigger> getTriggers(String schemaName, String tableName) throws ModuleException {
    List<Trigger> triggers = new ArrayList<Trigger>();

    String query = sqlHelper.getTriggersSQL(schemaName, tableName);

    try (ResultSet rs = getStatement().executeQuery(query)) {
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
          LOGGER.error("Error getting trigger action time for " + triggerName);
          actionTime = "";
        }

        trigger.setActionTime(actionTime);

        triggers.add(trigger);
      }
    } catch (SQLException e) {
      LOGGER.debug("No triggers imported for " + schemaName + "." + tableName, e);
    }

    return triggers;
  }


  /**
   * Gets the views of a given schema
   *
   * @param schemaName
   *          the schema name
   * @return A list of {@link ViewStructure}
   * @throws SQLException
   * @throws ModuleException
   */
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

  /**
   * Gets the routines of a given schema
   *
   *
   * @param schemaName
   *          the schema name
   * @return A list of {@link RoutineStructure}
   * @throws SQLException
   * @throws ModuleException
   */
  @Override
  protected List<RoutineStructure> getRoutines(String schemaName) throws SQLException, ModuleException {
    List<RoutineStructure> routines = super.getRoutines(schemaName);

    for (RoutineStructure routine : routines) {
      String queryForProcedureSQL = ((SybaseHelper) sqlHelper).getProcedureSQL(routine.getName());
      try (ResultSet res = getStatement().executeQuery(queryForProcedureSQL)) {
        StringBuilder b = new StringBuilder();
        while (res.next()) {
          b.append(res.getString("TEXT"));
        }
        routine.setBody(b.toString());
      } catch (SQLException e) {
        LOGGER.debug("Could not retrieve routine code (as routine) for " + routine.getName(), e);
        routine.setBody("");
      }

    }
    return routines;
  }
}