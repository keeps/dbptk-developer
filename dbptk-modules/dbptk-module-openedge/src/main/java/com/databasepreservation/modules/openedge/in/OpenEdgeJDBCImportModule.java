/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.openedge.in;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.RoutineStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.Trigger;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.CloseableUtils;
import com.databasepreservation.modules.OpenEdgeHelper;
import com.databasepreservation.modules.OpenEdgeModuleFactory;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.utils.MapUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class OpenEdgeJDBCImportModule extends JDBCImportModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenEdgeJDBCImportModule.class);
  private static final String HANDLED_SQL_EXCEPTION = "handled SQLException";

  /**
   * Creates a new Progress OpenEdge import module using the default instance.
   *
   * @param hostname
   *          the name of the Progress OpenEdge server host (e.g. localhost)
   * @param port
   *          the port that sybase is listening
   * @param database
   *          the name of the database we'll be accessing
   * @param username
   *          the name of the user to use in the connection
   * @param password
   *          the password of the user to use in the connection
   */
  public OpenEdgeJDBCImportModule(String moduleName, String hostname, int port, String database, String username,
    String password) throws ModuleException {
    super("com.ddtek.jdbc.openedge.OpenEdgeDriver",
      "jdbc:datadirect:openedge://"
        + hostname + ":" + port + ";user=" + username + ";password=" + password + ";DatabaseName=" + database,
      new OpenEdgeHelper(), new OpenEdgeDataTypeImporter(), moduleName,
      MapUtils.buildMapFromObjects(OpenEdgeModuleFactory.PARAMETER_HOSTNAME, hostname,
        OpenEdgeModuleFactory.PARAMETER_PORT_NUMBER, port, OpenEdgeModuleFactory.PARAMETER_USERNAME, username,
        OpenEdgeModuleFactory.PARAMETER_PASSWORD, password, OpenEdgeModuleFactory.PARAMETER_DATABASE, database));
  }

  public OpenEdgeJDBCImportModule(String moduleName, String hostname, int port, String database, String username,
    String password, String sshHost, String sshUser, String sshPassword, String sshPortNumber) throws ModuleException {
    super("com.ddtek.jdbc.openedge.OpenEdgeDriver",
      "jdbc:datadirect:openedge://"
        + hostname + ";user=" + username + ";password=" + password + ";DatabaseName=" + database,
      new OpenEdgeHelper(), new OpenEdgeDataTypeImporter(), moduleName,
      MapUtils.buildMapFromObjects(OpenEdgeModuleFactory.PARAMETER_HOSTNAME, hostname,
        OpenEdgeModuleFactory.PARAMETER_PORT_NUMBER, port, OpenEdgeModuleFactory.PARAMETER_USERNAME, username,
        OpenEdgeModuleFactory.PARAMETER_PASSWORD, password, OpenEdgeModuleFactory.PARAMETER_DATABASE, database),
      MapUtils.buildMapFromObjects(OpenEdgeModuleFactory.PARAMETER_SSH, true, OpenEdgeModuleFactory.PARAMETER_SSH_HOST,
        sshHost, OpenEdgeModuleFactory.PARAMETER_SSH_PORT, sshPortNumber, OpenEdgeModuleFactory.PARAMETER_SSH_USER,
        sshUser, OpenEdgeModuleFactory.PARAMETER_SSH_PASSWORD, sshPassword));
  }

  /**
   * Gets schemas that won't be imported.
   * <p>
   * Accepts schema names in as regular expressions I.e. SYS.* will ignore SYSCAT,
   * SYSFUN, etc
   *
   * @return A {@link Set} of schema names not to be imported
   */
  @Override
  protected Set<String> getIgnoredImportedSchemas() {
    Set<String> ignored = new HashSet<>();

    ignored.add("SYSPROGRESS");

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
   * Gets the views of a given schema
   *
   * @param schemaName
   *          the schema name
   * @return A {@link List} of {@link ViewStructure}
   * @throws SQLException
   * @throws ModuleException
   */
  @Override
  protected List<ViewStructure> getViews(String schemaName) throws SQLException, ModuleException {
    List<ViewStructure> views = super.getViews(schemaName);
    for (ViewStructure v : views) {

      String queryForViewSQL = ((OpenEdgeHelper) sqlHelper).getViewSQL(v.getName());
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
        LOGGER.debug("Exception trying to get view SQL in Progress OpenEdge", e);
      } finally {
        CloseableUtils.closeQuietly(rset);
        CloseableUtils.closeQuietly(statement);
      }
    }
    return views;
  }

  /**
   * Gets the triggers of a given schema and table
   *
   * @param schemaName
   *          the schema name
   * @param tableName
   *          the table name
   * @return A {@link List} of {@link Trigger}
   * @throws ModuleException
   */
  @Override
  protected List<Trigger> getTriggers(String schemaName, String tableName) throws ModuleException {
    List<Trigger> triggers = new ArrayList<>();

    String query = sqlHelper.getTriggersSQL(schemaName, tableName);

    try (ResultSet rs = getStatement().executeQuery(query)) {
      while (rs.next()) {
        Trigger trigger = new Trigger();

        String triggerName = getTriggerName(rs);
        String actionTime = getActionTime(rs);
        String triggerEvent = getTriggerEvent(rs);

        trigger.setName(triggerName);
        trigger.setActionTime(actionTime);
        trigger.setTriggerEvent(triggerEvent);

        getTrigger(schemaName, tableName, trigger);

        triggers.add(trigger);

      }
    } catch (SQLException e) {
      LOGGER.debug("No triggers imported for " + schemaName + "." + tableName, e);
    }

    return triggers;
  }

  private String getTriggerName(ResultSet resultSet) {
    try {
      return resultSet.getString("TRIGGER_NAME");
    } catch (SQLException e) {
      LOGGER.debug(HANDLED_SQL_EXCEPTION, e);
      return "";
    }
  }

  private String getActionTime(ResultSet resultSet) {
    try {
      return processActionTime(resultSet.getString("ACTION_TIME"));
    } catch (SQLException e) {
      LOGGER.debug(HANDLED_SQL_EXCEPTION, e);
      return "";
    }
  }

  private String getTriggerEvent(ResultSet resultSet) {
    try {
      return processTriggerEvent(resultSet.getString("TRIGGER_EVENT"));
    } catch (SQLException e) {
      LOGGER.debug(HANDLED_SQL_EXCEPTION, e);
      return "";
    }
  }

  private void getTrigger(String schemaName, String tableName, Trigger trigger) {
    String queryForTriggerInfo = ((OpenEdgeHelper) sqlHelper).getTriggerInfoSQL(schemaName, tableName,
      trigger.getName());

    try (PreparedStatement statement = getConnection().prepareStatement(queryForTriggerInfo);
      ResultSet resultSet = statement.executeQuery()) {

      String triggerID = "";
      String referencingNew = "";
      String referencingOld = "";
      String statementOrRow = "";
      String javaImportClause = "";
      StringBuilder javaSnippet = new StringBuilder();
      String columns = "";

      if (resultSet.next()) {
        triggerID = resultSet.getString("TRIGGER_ID");
        referencingNew = resultSet.getString("REF_NEW").equals("Y") ? "NEWROW" : "";
        referencingOld = resultSet.getString("REF_OLD").equals("Y") ? "OLDROW" : "";
        statementOrRow = resultSet.getString("STAT_OR_ROW").equals("R") ? "ROW" : "STATEMENT";

        String queryForTriggeredAction = ((OpenEdgeHelper) sqlHelper).getTriggeredActionSQL(triggerID);
        try (PreparedStatement stat = getConnection().prepareStatement(queryForTriggeredAction);
          ResultSet res = stat.executeQuery()) {

          int rowCount = 1;

          while (res.next()) {
            if (rowCount == 1) { // Handle java_import_clause:
              // https://documentation.progress.com/output/ua/OpenEdge_latest/index.html#page/dmsrf/create-trigger.html)
              javaImportClause = res.getString("TEXT");
            }

            if (rowCount > 1) { // Handle java_snippet:
              // https://documentation.progress.com/output/ua/OpenEdge_latest/index.html#page/dmsrf/create-trigger.html
              javaSnippet.append(res.getString("TEXT"));
            }

            rowCount++;
          }
        }

        String queryForTriggerColumns = ((OpenEdgeHelper) sqlHelper).getTriggerColumnsSQL(triggerID);
        try (PreparedStatement statementForTriggerColumns = getConnection().prepareStatement(queryForTriggerColumns);
          ResultSet resultSetForTriggerColumns = statementForTriggerColumns.executeQuery()) {

          StringBuilder columnsBuilder = new StringBuilder();

          while (resultSetForTriggerColumns.next()) {
            columnsBuilder.append(resultSetForTriggerColumns.getString("COLUMN_NAME")).append(",");
          }

          if (columnsBuilder.length() > 0) {
            columns = columnsBuilder.deleteCharAt(columnsBuilder.length() - 1).toString();
          }

          trigger.setTriggeredAction(this.buildCreateTrigger(schemaName, trigger.getName(), trigger.getActionTime(),
            trigger.getTriggerEvent(), tableName, referencingOld, referencingNew, statementOrRow, javaImportClause,
            javaSnippet.toString(), columns));
        }
      }
    } catch (SQLException | ModuleException e) {
      LOGGER.debug("Fail to obtain information for trigger '" + trigger.getName() + "'", e);
    }
  }

  /**
   * Gets the routines of a given schema
   *
   * @param schemaName
   *          The schema name
   * @return A {@link List} of {@link RoutineStructure}
   * @throws SQLException
   */
  @Override
  protected List<RoutineStructure> getRoutines(String schemaName) throws SQLException, ModuleException {
    List<RoutineStructure> routines = super.getRoutines(schemaName);

    for (RoutineStructure routine : routines) {
      String routineName = routine.getName();
      /**
       * Progress(R) DataDirect(R) DataDirect Connect(R) for JDBC The Progress
       * OpenEdge driver does not support returning parameter metadata for stored
       * procedure arguments. Driver version: Release 5.1.4 (F000376.U000183) URL:
       * https://media.datadirect.com/download/docs/jdbc/alljdbc/help.html#page/jdbcconnect%2FStored_Procedures_5.html%23wwID0E3DOV
       * (Visited: 30-04-2019)
       * 
       * String queryForParametersSQL = ((OpenEdgeHelper)
       * sqlHelper).getProcedureParametersSQL(routineName); PreparedStatement
       * statement = null; statement =
       * getConnection().prepareStatement(queryForParametersSQL);
       */
      String queryForProcedureSource = ((OpenEdgeHelper) sqlHelper).getProcedureSourceSQL(schemaName, routineName);
      try (PreparedStatement stat = getConnection().prepareStatement(queryForProcedureSource)) {

        try (ResultSet resultSet = stat.executeQuery()) {
          StringBuilder b = new StringBuilder();
          int rowCount = 1;

          while (resultSet.next()) {
            if (rowCount == 1) { // Handle java_import_clause:
              // https://documentation.progress.com/output/ua/OpenEdge_latest/index.html#page/dmsrf/create-trigger.html)
              b.append("IMPORT: ");
              b.append(resultSet.getString("TEXT"));
              b.append("\n");
            }

            if (rowCount > 1) { // Handle java_snippet:
              // https://documentation.progress.com/output/ua/OpenEdge_latest/index.html#page/dmsrf/create-trigger.html
              b.append(resultSet.getString("TEXT"));
            }
            rowCount++;

          }
          routine.setSource(b.toString());
        }
      } catch (SQLException e) {
        LOGGER.debug("Fail to obtain source code for routine " + routineName, e);
      }
    }

    return routines;
  }

  /**
   * Sanitizes the trigger action time data
   *
   * @param toSanitize
   *          The trigger action time retrieved from the database
   * @return The trigger action time sanitized
   */
  @Override
  protected String processActionTime(String toSanitize) {
    LOGGER.debug("Trigger action time: {}", toSanitize);
    char[] charArray = toSanitize.toCharArray();

    String res = "";
    if (charArray.length > 0 && charArray[0] == 'A') {
      res = "AFTER";
    }
    if (charArray.length > 0 && charArray[0] == 'B') {
      res = "BEFORE";
    }

    // OpenEdge supports only AFTER or BEFORE triggers

    return res;
  }

  /**
   * Sanitizes the trigger event time data
   *
   * @param toSanitize
   *          The trigger event retrieved from the database
   * @return The trigger event sanitized
   */
  @Override
  protected String processTriggerEvent(String toSanitize) {
    LOGGER.debug("Trigger event: {}", toSanitize);
    char[] charArray = toSanitize.toCharArray();
    String res = "";

    if (charArray.length > 0 && charArray[0] == 'I') {
      res = "INSERT";
    }
    if (charArray.length > 0 && charArray[0] == 'D') {
      res = "DELETE";
    }
    if (charArray.length > 0 && charArray[0] == 'U') {
      res = "UPDATE";
    }

    return res;
  }

  /**
   * Gets the description for Progress OpenEdge tables.
   *
   * @param schema
   *          the name of the schema
   * @param tableName
   *          the name of the table
   * @param tableIndex
   *          the table index
   * @param description
   *          the table description
   * @return A {@link TableStructure} with the description added if founded
   * @throws SQLException
   * @throws ModuleException
   */
  @Override
  protected TableStructure getTableStructure(SchemaStructure schema, String tableName, int tableIndex,
    String description, boolean view) throws SQLException, ModuleException {
    TableStructure tableStructure = super.getTableStructure(schema, tableName, tableIndex, description, view);
    tableStructure.setDescription(getDescriptionForTable(schema.getName(), tableName));
    return tableStructure;
  }

  /**
   * Gets the description for Progress OpenEdge columns.
   *
   * @param schemaName
   *          the schema name
   * @param tableName
   *          the table name
   * @return A {@link List} of {@link ColumnStructure} with the description added
   *         if founded
   * @throws SQLException
   * @throws ModuleException
   */
  @Override
  protected List<ColumnStructure> getColumns(String schemaName, String tableName) throws SQLException, ModuleException {
    List<ColumnStructure> columns = super.getColumns(schemaName, tableName);
    for (ColumnStructure column : columns) {
      column.setDescription(getDescriptionForColumn(schemaName, tableName, column.getName()));
    }
    return columns;
  }

  /**
   * Gets the description text for Progress OpenEdge table
   *
   * @param schemaName
   *          the schema name
   * @param tableName
   *          the table name
   * @return the description for the table or {@null} if none found
   * @throws ModuleException
   */
  private String getDescriptionForTable(String schemaName, String tableName) throws ModuleException {
    String description = null;

    String query = ((OpenEdgeHelper) sqlHelper).getTableDescription(schemaName, tableName);

    try (ResultSet res = getStatement().executeQuery(query)) {
      while (res.next()) {
        description = res.getString("DESCRIPTION");
      }
    } catch (SQLException e) {
      LOGGER.error("Error getting description for " + tableName, e);
    }

    return description;
  }

  /**
   * Gets the description text for Progress OpenEdge columns
   *
   * @param schemaName
   *          the schema name
   * @param tableName
   *          the table name
   * @param columnName
   *          the column name
   * @return the description for the column or {@null} if none found
   * @throws ModuleException
   */
  private String getDescriptionForColumn(String schemaName, String tableName, String columnName)
    throws ModuleException {

    String description = null;

    String query = ((OpenEdgeHelper) sqlHelper).getColumnDescription(schemaName, tableName, columnName);

    try (ResultSet res = getStatement().executeQuery(query)) {
      while (res.next()) {
        description = res.getString("DESCRIPTION");
      }
    } catch (SQLException e) {
      LOGGER.error("Error getting description for " + tableName, e);
    }

    return description;
  }

  /**
   * Builds the create trigger statement from the arguments
   *
   * @param schemaName
   *          the name of the schema
   * @param triggerName
   *          the name of the trigger
   * @param actionTime
   *          the trigger action time
   * @param triggerEvent
   *          the trigger event
   * @param tableName
   *          the name of the table
   * @param referencingOld
   *          the referencing old value
   * @param referencingNew
   *          the referencing new value
   * @param statementOrRow
   *          the for each option
   * @param javaImportClause
   *          the import clauses for the trigger
   * @param javaSnippet
   *          the trigger code
   * @param columns
   *          specified columns to activate the trigger
   *
   * @return the create trigger statement for Progress OpenEdge database
   */
  private String buildCreateTrigger(String schemaName, String triggerName, String actionTime, String triggerEvent,
    String tableName, String referencingOld, String referencingNew, String statementOrRow, String javaImportClause,
    String javaSnippet, String columns) {

    StringBuilder b = new StringBuilder("CREATE TRIGGER ");
    b.append(schemaName).append(".").append(triggerName).append(" ").append(actionTime).append(" ");
    b.append(triggerEvent);
    if (!StringUtils.isBlank(columns)) {
      b.append(" OF ").append(columns);
    }
    b.append(" ON ").append(tableName).append("\n");
    b.append("REFERENCING ");
    if (!StringUtils.isBlank(referencingNew) && StringUtils.isBlank(referencingOld)) {
      b.append(referencingNew);
    } else if (StringUtils.isBlank(referencingNew) && !StringUtils.isBlank(referencingOld)) {
      b.append(referencingOld);
    } else if (!StringUtils.isBlank(referencingNew) && !StringUtils.isBlank(referencingOld)) {
      b.append(referencingNew).append(", ").append(referencingOld);
    }
    b.append("\n").append("FOR EACH ").append(statementOrRow).append("\n");
    b.append("IMPORT").append("\n");
    b.append(javaImportClause).append("\n");
    b.append("BEGIN").append("\n");
    b.append(javaSnippet).append("\n");
    b.append("END");

    return b.toString();
  }
}
