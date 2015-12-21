package com.databasepreservation.modules.jdbc.in;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.sql.STRUCT;

import org.apache.commons.lang3.StringUtils;
import org.w3c.util.DateParser;

import pt.gov.dgarq.roda.common.FileFormat;
import pt.gov.dgarq.roda.common.FormatUtility;

import com.databasepreservation.CustomLogger;
import com.databasepreservation.Main;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.ComposedCell;
import com.databasepreservation.model.data.FileItem;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.structure.CandidateKey;
import com.databasepreservation.model.structure.CheckConstraint;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.PrivilegeStructure;
import com.databasepreservation.model.structure.Reference;
import com.databasepreservation.model.structure.RoleStructure;
import com.databasepreservation.model.structure.RoutineStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.Trigger;
import com.databasepreservation.model.structure.UserStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.model.structure.type.ComposedTypeArray;
import com.databasepreservation.model.structure.type.ComposedTypeStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeNumericExact;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.model.structure.type.UnsupportedDataType;
import com.databasepreservation.modules.SQLHelper;

/**
 * @author Luis Faria <lfaria@keep.pt>
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class JDBCImportModule implements DatabaseImportModule {

  // if fetch size is zero, then the driver decides the best fetch size
  protected static final int ROW_FETCH_BLOCK_SIZE = 0;

  protected static final String DEFAULT_DATA_TIMESPAN = "(...)";

  protected static final boolean IGNORE_UNSUPPORTED_DATA_TYPES = true;
  protected final String driverClassName;
  protected final String connectionURL;
  private final CustomLogger logger = CustomLogger.getLogger(JDBCImportModule.class);
  protected Connection connection;

  protected Statement statement;

  protected DatabaseMetaData dbMetadata;

  protected DatabaseStructure dbStructure;

  // the schema object being built
  protected SchemaStructure actualSchema;

  protected SQLHelper sqlHelper;

  protected FormatUtility formatUtility;

  /**
   * Create a new JDBC import module
   *
   * @param driverClassName
   *          the name of the the JDBC driver class
   * @param connectionURL
   *          the connection url to use in the connection
   */
  public JDBCImportModule(String driverClassName, String connectionURL) {
    this(driverClassName, connectionURL, new SQLHelper());
  }

  protected JDBCImportModule(String driverClassName, String connectionURL, SQLHelper sqlHelper) {
    this.driverClassName = driverClassName;
    this.connectionURL = connectionURL;
    this.sqlHelper = sqlHelper;
    connection = null;
    dbMetadata = null;
    dbStructure = null;
  }

  /**
   * Connect to the server using the properties defined in the constructor, or
   * return the existing connection
   *
   * @return the connection
   * @throws SQLException
   * @throws ClassNotFoundException
   *           the JDBC driver could not be found in classpath
   */
  public Connection getConnection() throws SQLException, ClassNotFoundException {
    if (connection == null) {
      logger.debug("Loading JDBC Driver " + driverClassName);
      Class.forName(driverClassName);
      logger.debug("Getting connection");
      connection = DriverManager.getConnection(connectionURL);
      logger.debug("Connected");
    }
    return connection;
  }

  protected Statement getStatement() throws SQLException, ClassNotFoundException {
    if (statement == null) {
      statement = getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
        ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }
    return statement;
  }

  /**
   * Get the database metadata
   *
   * @return the database metadata
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  public DatabaseMetaData getMetadata() throws SQLException, ClassNotFoundException {
    if (dbMetadata == null) {
      dbMetadata = getConnection().getMetaData();
    }
    return dbMetadata;
  }

  /**
   * Close current connection
   *
   * @throws SQLException
   */
  public void closeConnection() throws SQLException {
    if (connection != null) {
      connection.close();
      connection = null;
      dbMetadata = null;
      dbStructure = null;
    }
  }

  /**
   * Some driver may not report correctly (due to cursor setup, etc) the number
   * of the row currently being processed (ResultSet.getRow).
   * <p>
   * If its known that a particular import module doesn't support it,
   * re-implement this method in that particular module to return false
   *
   * @return true if ResultSet.getRow reports correctly the number of the row
   *         being processed; false otherwise
   */
  protected boolean isGetRowAvailable() {
    return true;
  }

  /**
   * @return the database structure
   * @throws SQLException
   * @throws UnknownTypeException
   *           the original data type is unknown
   * @throws ClassNotFoundException
   */
  protected DatabaseStructure getDatabaseStructure() throws SQLException, UnknownTypeException, ClassNotFoundException {
    if (dbStructure == null) {
      logger.debug("Importing structure");
      dbStructure = new DatabaseStructure();
      logger.debug("driver version: " + getMetadata().getDriverVersion());
      dbStructure.setName(getDbName());
      dbStructure.setProductName(getMetadata().getDatabaseProductName());
      dbStructure.setProductVersion(getMetadata().getDatabaseProductVersion());
      dbStructure.setDataOwner(System.getProperty("user.name"));
      dbStructure.setDataOriginTimespan(DEFAULT_DATA_TIMESPAN);
      dbStructure.setProducerApplication(Main.APP_NAME);
      String clientMachine = "";
      try {
        clientMachine = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        logger.debug("UnknownHostException", e);
      }
      dbStructure.setClientMachine(clientMachine);

      dbStructure.setSchemas(getSchemas());
      actualSchema = null;

      dbStructure.setUsers(getUsers());
      dbStructure.setRoles(getRoles());
      dbStructure.setPrivileges(getPrivileges());

      logger.debug("Finishing get dbStructure");
    }
    return dbStructure;
  }

  protected String getDbName() throws SQLException, ClassNotFoundException {
    return getConnection().getCatalog();
  }

  /**
   * Checks if schema name matches the set of schemas to be ignored.
   *
   * @param schemaName
   *          the schema name
   * @return true if schema is ignored; false if it isn't
   */
  protected boolean isIgnoredImportedSchema(String schemaName) {
    boolean ignoredSchema = false;
    for (String s : getIgnoredImportedSchemas()) {
      if (schemaName.matches(s)) {
        ignoredSchema = true;
      }
    }
    return ignoredSchema;
  }

  /**
   * @return the database schemas (not ignored by default and/or user)
   * @throws SQLException
   * @throws ClassNotFoundException
   * @throws UnknownTypeException
   */
  protected List<SchemaStructure> getSchemas() throws SQLException, ClassNotFoundException, UnknownTypeException {
    List<SchemaStructure> schemas = new ArrayList<SchemaStructure>();

    ResultSet rs = getMetadata().getSchemas();
    int schemaIndex = 1;
    while (rs.next()) {
      String schemaName = rs.getString(1);
      // does not import ignored schemas
      if (isIgnoredImportedSchema(schemaName)) {
        continue;
      }
      schemas.add(getSchemaStructure(schemaName, schemaIndex));
      schemaIndex++;
    }
    return schemas;
  }

  /**
   * Get schemas that won't be imported
   * <p>
   * Accepts schemas names in as regular expressions I.e. SYS.* will ignore
   * SYSCAT, SYSFUN, etc
   *
   * @return the schema names not to be imported
   */
  protected Set<String> getIgnoredImportedSchemas() {
    HashSet ignore = new HashSet<String>();
    ignore.add("information_schema");
    ignore.add("pg_catalog");
    return ignore;
  }

  /**
   * @param schemaName
   *          the schema name
   * @return the schema structure of a given schema name
   * @throws ModuleException
   */
  protected SchemaStructure getSchemaStructure(String schemaName, int schemaIndex) throws SQLException,
    UnknownTypeException, ClassNotFoundException {
    actualSchema = new SchemaStructure();
    actualSchema.setName(schemaName);
    actualSchema.setIndex(schemaIndex);
    // actualSchema.setUserDefinedTypes(getUDTs(actualSchema));
    actualSchema.setUserDefinedTypes(new ArrayList<ComposedTypeStructure>());
    actualSchema.setTables(getTables(actualSchema));
    actualSchema.setViews(getViews(schemaName));
    actualSchema.setRoutines(getRoutines(schemaName));

    return actualSchema;
  }

  protected ArrayList<ComposedTypeStructure> getUDTs(SchemaStructure schema) throws SQLException,
    ClassNotFoundException, UnknownTypeException {
    ResultSet udtTypes = getMetadata().getUDTs(dbStructure.getName(), schema.getName(), null, null);

    // possibleUDT because it may also be a table name, which in some cases may
    // also be used as a type
    ArrayList<String> possibleUDTs = new ArrayList<>();

    while (udtTypes.next()) {
      int dataType = udtTypes.getInt(5);
      if (dataType == Types.STRUCT) {
        possibleUDTs.add(udtTypes.getString(3));
      } else {
        StringBuilder debug = new StringBuilder();

        // 1. TYPE_CAT String => the type's catalog (may be null)
        debug.append("\nTYPE_CAT: ").append(udtTypes.getString(1));
        // 2. TYPE_SCHEM String => type's schema (may be null)
        debug.append("\nTYPE_SCHEM: ").append(udtTypes.getString(2));
        // 3. TYPE_NAME String => type name
        debug.append("\nTYPE_NAME: ").append(udtTypes.getString(3));
        // 4. CLASS_NAME String => Java class name
        debug.append("\nCLASS_NAME: ").append(udtTypes.getString(4)).append("\n");
        // 5. DATA_TYPE int => type value defined in java.sql.Types. One of
        // JAVA_OBJECT, STRUCT, or DISTINCT
        switch (dataType) {
          case Types.JAVA_OBJECT:
            debug.append("DATA_TYPE: JAVA_OBJECT");
            break;
          case Types.STRUCT:
            debug.append("DATA_TYPE: STRUCT");
            break;
          case Types.DISTINCT:
            debug.append("DATA_TYPE: DISTINCT");
            break;
          default:
            debug.append("DATA_TYPE: " + dataType + "(unknown)");
        }
        // 6. REMARKS String => explanatory comment on the type
        debug.append("\nREMARKS: ").append(udtTypes.getString(6));
        /*
         * 7. BASE_TYPE short => type code of the source type of a DISTINCT type
         * or the type that implements the user-generated reference type of the
         * SELF_REFERENCING_COLUMN of a structured type as defined in
         * java.sql.Types (null if DATA_TYPE is not DISTINCT or not STRUCT with
         * REFERENCE_GENERATION = USER_DEFINED)
         */
        debug.append("\nBASE_TYPE: ").append(udtTypes.getShort(7));
        logger.debug("Possible UDT is not a STRUCT. " + debug.toString());
        logger.warn("Unsupported UDT found: " + debug.toString());
      }
    }

    ArrayList<ComposedTypeStructure> udts = new ArrayList<>();
    for (String possibleUDT : possibleUDTs) {
      ComposedTypeStructure type = new ComposedTypeStructure(possibleUDT);
      for (ColumnStructure column : getUDTColumns(schema.getName(), possibleUDT)) {
        type.addType(column.getName(), column.getType());
      }
      udts.add(type);
    }

    for (ComposedTypeStructure base : udts) {
      for (ComposedTypeStructure addition : udts) {
        base.completeExistingType(addition);
      }
    }

    for (ComposedTypeStructure udt : udts) {
      // TODO: remove after adding support for LOBs inside UDTs
      if (udt.containsLOBs()) {
        logger.warn("LOBs inside UDTs are not supported yet. Only the first level of hierarchy will be exported. UDT "
          + udt.getOriginalTypeName() + " detected as containing LOBs.", new ModuleException("UDT containing LOBs:"
          + udt.toString()));
      }

      // TODO: remove after adding support for hierarchical UDTs
      if (udt.isHierarchical()) {
        logger.warn("UDTs inside UDTs are not supported yet. Only the first level of hierarchy will be exported. UDT "
          + udt.getOriginalTypeName() + " detected as hierarchical.",
          new ModuleException("hierarchical UDT:" + udt.toString()));
      }

      // all recursive UDTs are hierarchical, so two warnings are expected on
      // recursive types
      // TODO: remove after adding support for recursive UDTs
      if (udt.isRecursive()) {
        logger.warn(
          "Recursive UDTs are not supported yet. Only the first level of data will be exported. UDT "
            + udt.getOriginalTypeName() + " detected as recursive.",
          new ModuleException("recursive UDT:" + udt.toString()));
      }
    }

    return udts;
  }

  /**
   * @param schema
   *          the schema structure
   * @return the database tables of a given schema
   * @throws SQLException
   * @throws ClassNotFoundException
   * @throws UnknownTypeException
   */
  protected List<TableStructure> getTables(SchemaStructure schema) throws SQLException, ClassNotFoundException,
    UnknownTypeException {
    List<TableStructure> tables = new ArrayList<TableStructure>();
    ResultSet rset = getMetadata().getTables(dbStructure.getName(), schema.getName(), "%", new String[] {"TABLE"});
    int tableIndex = 1;
    while (rset.next()) {
      logger.debug("getting table structure for: " + rset.getString(3));
      tables.add(getTableStructure(schema, rset.getString(3), tableIndex));
      tableIndex++;
    }
    return tables;
  }

  /**
   * @param schemaName
   *          the schema name
   * @return the database views of a given schema
   * @throws SQLException
   * @throws ClassNotFoundException
   * @throws UnknownTypeException
   */
  protected List<ViewStructure> getViews(String schemaName) throws SQLException, ClassNotFoundException,
    UnknownTypeException {
    List<ViewStructure> views = new ArrayList<ViewStructure>();
    ResultSet rset = getMetadata().getTables(dbStructure.getName(), schemaName, "%", new String[] {"VIEW"});
    while (rset.next()) {
      String viewName = rset.getString(3);
      ViewStructure view = new ViewStructure();
      view.setName(viewName);
      view.setColumns(getColumns(schemaName, viewName));

      if (view.getColumns().isEmpty()) {
        logger.info("View " + viewName + " in schema " + schemaName + " was ignored because it contains no columns.");
      } else {
        views.add(view);
      }
    }
    return views;
  }

  /**
   * @param schemaName
   * @return
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  protected List<RoutineStructure> getRoutines(String schemaName) throws SQLException, ClassNotFoundException {
    // TODO add optional fields to routine (use getProcedureColumns)
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
          routine.setDescription("Procedure does not " + "return a result");
        } else if (rset.getShort(8) == 2) {
          routine.setDescription("Procedure returns a result");
        }
      }
      routines.add(routine);
    }
    return routines;
  }

  /**
   * @param tableName
   *          the name of the table
   * @return the table structure
   * @throws SQLException
   * @throws UnknownTypeException
   *           the original data type is unknown
   * @throws ClassNotFoundException
   * @throws ModuleException
   */
  protected TableStructure getTableStructure(SchemaStructure schema, String tableName, int tableIndex)
    throws SQLException, UnknownTypeException, ClassNotFoundException {

    TableStructure table = new TableStructure();
    table.setId(schema.getName() + "." + tableName);
    table.setName(tableName);
    table.setSchema(schema);
    table.setIndex(tableIndex);

    table.setColumns(getColumns(schema.getName(), tableName));
    table.setPrimaryKey(getPrimaryKey(schema.getName(), tableName));
    table.setForeignKeys(getForeignKeys(schema.getName(), tableName));
    table.setCandidateKeys(getCandidateKeys(schema.getName(), tableName));
    table.setCheckConstraints(getCheckConstraints(schema.getName(), tableName));
    table.setTriggers(getTriggers(schema.getName(), tableName));

    table.setRows(getRows(schema.getName(), tableName));

    return table;
  }

  private int getRows(String schemaName, String tableName) throws ClassNotFoundException, SQLException {
    String query = sqlHelper.getRowsSQL(schemaName, tableName);
    logger.debug("count query: " + query);
    ResultSet rs = getStatement().executeQuery(query);

    int count = -1;
    if (rs.next())
      count = rs.getInt(1);
    logger.debug("Counted " + count + " rows");

    return count;
  }

  /**
   * Create the column structure
   *
   * @param tableName
   *          the name of the table which the column belongs to
   * @param columnName
   *          the name of the column
   * @param type
   *          the type of the column
   * @param nillable
   *          is the column nillable
   * @param index
   *          the column index
   * @param description
   *          the column description
   * @param defaultValue
   * @param isAutoIncrement
   * @return the column structure
   */
  protected ColumnStructure getColumnStructure(String tableName, String columnName, Type type, Boolean nillable,
    int index, String description, String defaultValue, Boolean isAutoIncrement) {
    ColumnStructure column = new ColumnStructure(tableName + "." + columnName, columnName, type, nillable, description,
      defaultValue, isAutoIncrement);
    return column;
  }

  protected List<UserStructure> getUsers() throws SQLException, ClassNotFoundException {
    List<UserStructure> users = new ArrayList<UserStructure>();
    String query = sqlHelper.getUsersSQL(getDbName());
    if (query != null) {
      ResultSet rs = getStatement().executeQuery(query);
      while (rs.next()) {
        UserStructure user = new UserStructure();
        user.setName(rs.getString("USER_NAME"));
        users.add(user);
      }
    } else {
      users.add(new UserStructure("UNDEFINED_USER", "DESCRIPTION"));
      logger.warn("Users were not imported: not supported yet on " + getClass().getSimpleName() + "\n"
        + "UNDEFINED_USER will be set as user name");
    }
    return users;
  }

  /**
   * @return the database roles
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  protected List<RoleStructure> getRoles() throws SQLException, ClassNotFoundException {
    List<RoleStructure> roles = new ArrayList<RoleStructure>();
    String query = sqlHelper.getRolesSQL();
    if (query != null) {
      ResultSet rs = getStatement().executeQuery(query);
      while (rs.next()) {
        RoleStructure role = new RoleStructure();
        String roleName;
        try {
          roleName = rs.getString("ROLE_NAME");
        } catch (SQLException e) {
          logger.debug("handled SQLException", e);
          roleName = "";
        }
        role.setName(roleName);

        String admin;
        try {
          admin = rs.getString("ADMIN");
        } catch (SQLException e) {
          logger.trace("handled SQLException", e);
          admin = "";
        }
        role.setAdmin(admin);

        roles.add(role);
      }
    } else {
      logger.info("Roles were not imported: not supported yet on " + getClass().getSimpleName());
    }
    return roles;
  }

  /**
   * @return the database privileges
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  protected List<PrivilegeStructure> getPrivileges() throws SQLException, ClassNotFoundException {
    List<PrivilegeStructure> privileges = new ArrayList<PrivilegeStructure>();

    for (SchemaStructure schema : dbStructure.getSchemas()) {
      for (TableStructure table : schema.getTables()) {
        ResultSet rs;
        try {
          rs = getMetadata().getTablePrivileges(dbStructure.getName(), schema.getName(), table.getName());
        } catch (SQLException e) {
          logger
            .warn("It was not possible to retrieve the list of all database permissions. Please ensure the current user has permissions to list all database permissions.");
          logger
            .debug(
              "It was not possible to retrieve the list of all database permissions. Please ensure the current user has permissions to list all database permissions.",
              e);
          break;
        }
        while (rs.next()) {
          PrivilegeStructure privilege = new PrivilegeStructure();
          String grantor = rs.getString("GRANTOR");
          if (grantor == null) {
            grantor = "";
          }
          privilege.setGrantor(grantor);

          String grantee = rs.getString("GRANTEE");
          if (grantee == null) {
            grantee = "";
          }
          privilege.setGrantee(grantee);
          privilege.setType(rs.getString("PRIVILEGE"));

          String option = "false";
          String isGrantable = rs.getString("IS_GRANTABLE");
          if (isGrantable != null) {
            if ("yes".equalsIgnoreCase(isGrantable)) {
              option = "true";
            }
          }
          privilege.setOption(option);
          privilege.setObject("TABLE \"" + schema.getName() + "\".\"" + table.getName() + "\"");

          privileges.add(privilege);
        }
      }
    }
    return privileges;
  }

  /**
   * @param schemaName
   *          the schema name
   * @param udtName
   *          the UDT name
   * @return the columns of a given schema.table
   * @throws SQLException
   * @throws ClassNotFoundException
   * @throws UnknownTypeException
   */
  protected List<ColumnStructure> getUDTColumns(String schemaName, String udtName) throws SQLException,
    ClassNotFoundException, UnknownTypeException {

    // logger.debug("id: " + schemaName + "." + udtName);
    List<ColumnStructure> columns = new ArrayList<ColumnStructure>();
    ResultSet rs = getMetadata().getColumns(dbStructure.getName(), schemaName, udtName, "%");
    logger.debug("Getting structure of (possible) UDT " + schemaName + "." + udtName);

    while (rs.next()) {
      StringBuilder cLogMessage = new StringBuilder();
      // 1. Table catalog (may be null)
      // String tableCatalog = rs.getString(1);
      // 2. Table schema (may be null)
      // String tableSchema = rs.getString(2);
      // 3. Table name
      // String udtName = rs.getString(3);
      // 4. Column name
      String columnName = rs.getString(4);
      // cLogMessage.append("Column name: " + columnName + "\n");
      // 5. SQL type from java.sql.Types
      int dataType = rs.getInt(5);
      cLogMessage.append("Data type: " + dataType + "\n");
      // 6. Data source dependent type name, for a UDT the type name is
      // fully qualified
      String typeName = rs.getString(6);
      cLogMessage.append("Type name: " + typeName + "\n");
      // 7. Column size
      // The COLUMN_SIZE column specifies the column size for the given
      // column. For numeric data, this is the maximum precision. For
      // character data, this is the length in characters. For datetime
      // datatypes, this is the length in characters of the String
      // representation (assuming the maximum allowed precision of the
      // fractional seconds component). For binary data, this is the
      // length in bytes. For the ROWID datatype, this is the length in
      // bytes. Null is returned for data types where the column size is
      // not applicable.
      int columnSize = rs.getInt(7);
      cLogMessage.append("Column size: ").append(columnSize).append("\n");
      // 8. BUFFER_LENGTH is not used.
      // 9. the number of fractional digits. Null is returned for data
      // types where DECIMAL_DIGITS is not applicable.
      int decimalDigits = rs.getInt(9);
      cLogMessage.append("Decimal digits: ").append(decimalDigits).append("\n");
      // 10. Radix (typically either 10 or 2)
      int numPrecRadix = rs.getInt(10);
      cLogMessage.append("Radix: ").append(numPrecRadix).append("\n");
      // 11. is NULL allowed (using 18. instead)

      // 12. comment describing column (may be null)
      String remarks = rs.getString(12);
      cLogMessage.append("Remarks: ").append(remarks).append("\n");
      // 13. default value for the column, which should be interpreted as
      // a string when the value is enclosed in single quotes (may be
      // null)
      String defaultValue = rs.getString(13);
      cLogMessage.append("Default value: ").append(defaultValue).append("\n");
      // 14. SQL_DATA_TYPE int => unused
      // 15. SQL_DATETIME_SUB int => unused
      // 16. CHAR_OCTET_LENGTH int => for char types the maximum number of
      // bytes in the column
      // 17. index of column in table (starting at 1)
      int index = rs.getInt(17);
      cLogMessage.append("Index: ").append(index).append("\n");
      // 18. ISO rules are used to determine the nullability for a column.
      // YES --- if the column can include NULLs
      // NO --- if the column cannot include NULLs
      // empty string --- if the nullability for the column is unknown
      Boolean isNullable = "YES".equals(rs.getString(18));
      cLogMessage.append("Is Nullable: ").append(isNullable).append("\n");
      // 20. SCOPE_SCHEMA String => schema of table that is the scope of a
      // reference attribute (null if the DATA_TYPE isn't REF)
      // 21. SCOPE_TABLE String => table name that this the scope of a
      // reference attribute (null if the DATA_TYPE isn't REF)
      // 22. SOURCE_DATA_TYPE short => source type of a distinct type or
      // user-generated Ref type, SQL type from java.sql.Types (null if
      // DATA_TYPE isn't DISTINCT or user-generated REF)
      // 23. IS_AUTOINCREMENT String => Indicates whether this column is
      // auto incremented
      // YES --- if the column is auto incremented
      // NO --- if the column is not auto incremented
      // empty string --- if it cannot be determined whether the column is
      // auto incremented
      Boolean isAutoIncrement = "YES".equals(rs.getString(23));
      cLogMessage.append("Is auto increment: ").append(isAutoIncrement).append("\n");
      // 24. IS_GENERATEDCOLUMN String => Indicates whether this is a
      // generated column
      // YES --- if this a generated column
      // NO --- if this not a generated column
      // empty string --- if it cannot be determined whether this is a
      // generated column
      // Boolean isGeneratedColumn = rs.getString(24) == "YES";

      // cLog.append("(" + udtName + ") " + "colName: " + columnName
      // + "; dataType: " + dataType + "; typeName: " + typeName);
      Type columnType = getType(schemaName, udtName, columnName, dataType, typeName, columnSize, decimalDigits,
        numPrecRadix);

      cLogMessage.append("Calculated type: ").append(columnType.getClass().getSimpleName()).append("\n");

      ColumnStructure column = getColumnStructure(udtName, columnName, columnType, isNullable, index, remarks,
        defaultValue, isAutoIncrement);

      cLogMessage.append("ColumnType hash: ").append(column.getType().hashCode()).append("\n");
      logger.debug(cLogMessage);

      columns.add(column);
    }

    return columns;
  }

  /**
   * @param schemaName
   *          the schema name
   * @param tableName
   *          the table name
   * @return the columns of a given schema.table
   * @throws SQLException
   * @throws ClassNotFoundException
   * @throws UnknownTypeException
   */
  protected List<ColumnStructure> getColumns(String schemaName, String tableName) throws SQLException,
    ClassNotFoundException, UnknownTypeException {

    // logger.debug("id: " + schemaName + "." + tableName);
    List<ColumnStructure> columns = new ArrayList<ColumnStructure>();
    ResultSet rs = getMetadata().getColumns(dbStructure.getName(), schemaName, tableName, "%");
    logger.debug("Getting structure of table " + schemaName + "." + tableName);

    while (rs.next()) {
      StringBuilder cLogMessage = new StringBuilder();
      // 1. Table catalog (may be null)
      // String tableCatalog = rs.getString(1);
      // 2. Table schema (may be null)
      // String tableSchema = rs.getString(2);
      // 3. Table name
      // String tableName = rs.getString(3);
      // 4. Column name
      String columnName = rs.getString(4);
      // cLogMessage.append("Column name: " + columnName + "\n");
      // 5. SQL type from java.sql.Types
      int dataType = rs.getInt(5);
      cLogMessage.append("Data type: " + dataType + "\n");
      // 6. Data source dependent type name, for a UDT the type name is
      // fully qualified
      String typeName = rs.getString(6);
      cLogMessage.append("Type name: " + typeName + "\n");
      // 7. Column size
      // The COLUMN_SIZE column specifies the column size for the given
      // column. For numeric data, this is the maximum precision. For
      // character data, this is the length in characters. For datetime
      // datatypes, this is the length in characters of the String
      // representation (assuming the maximum allowed precision of the
      // fractional seconds component). For binary data, this is the
      // length in bytes. For the ROWID datatype, this is the length in
      // bytes. Null is returned for data types where the column size is
      // not applicable.
      int columnSize = rs.getInt(7);
      cLogMessage.append("Column size: ").append(columnSize).append("\n");
      // 8. BUFFER_LENGTH is not used.
      // 9. the number of fractional digits. Null is returned for data
      // types where DECIMAL_DIGITS is not applicable.
      int decimalDigits = rs.getInt(9);
      cLogMessage.append("Decimal digits: ").append(decimalDigits).append("\n");
      // 10. Radix (typically either 10 or 2)
      int numPrecRadix = rs.getInt(10);
      cLogMessage.append("Radix: ").append(numPrecRadix).append("\n");
      // 11. is NULL allowed (using 18. instead)

      // 12. comment describing column (may be null)
      String remarks = rs.getString(12);
      cLogMessage.append("Remarks: ").append(remarks).append("\n");
      // 13. default value for the column, which should be interpreted as
      // a string when the value is enclosed in single quotes (may be
      // null)
      String defaultValue = rs.getString(13);
      cLogMessage.append("Default value: ").append(defaultValue).append("\n");
      // 14. SQL_DATA_TYPE int => unused
      // 15. SQL_DATETIME_SUB int => unused
      // 16. CHAR_OCTET_LENGTH int => for char types the maximum number of
      // bytes in the column
      // 17. index of column in table (starting at 1)
      int index = rs.getInt(17);
      cLogMessage.append("Index: ").append(index).append("\n");
      // 18. ISO rules are used to determine the nullability for a column.
      // YES --- if the column can include NULLs
      // NO --- if the column cannot include NULLs
      // empty string --- if the nullability for the column is unknown
      Boolean isNullable = "YES".equals(rs.getString(18));
      cLogMessage.append("Is Nullable: ").append(isNullable).append("\n");
      // 20. SCOPE_SCHEMA String => schema of table that is the scope of a
      // reference attribute (null if the DATA_TYPE isn't REF)
      // 21. SCOPE_TABLE String => table name that this the scope of a
      // reference attribute (null if the DATA_TYPE isn't REF)
      // 22. SOURCE_DATA_TYPE short => source type of a distinct type or
      // user-generated Ref type, SQL type from java.sql.Types (null if
      // DATA_TYPE isn't DISTINCT or user-generated REF)
      // 23. IS_AUTOINCREMENT String => Indicates whether this column is
      // auto incremented
      // YES --- if the column is auto incremented
      // NO --- if the column is not auto incremented
      // empty string --- if it cannot be determined whether the column is
      // auto incremented
      Boolean isAutoIncrement = "YES".equals(rs.getString(23));
      cLogMessage.append("Is auto increment: ").append(isAutoIncrement).append("\n");
      // 24. IS_GENERATEDCOLUMN String => Indicates whether this is a
      // generated column
      // YES --- if this a generated column
      // NO --- if this not a generated column
      // empty string --- if it cannot be determined whether this is a
      // generated column
      // Boolean isGeneratedColumn = rs.getString(24) == "YES";

      // cLog.append("(" + tableName + ") " + "colName: " + columnName
      // + "; dataType: " + dataType + "; typeName: " + typeName);
      Type columnType = getType(schemaName, tableName, columnName, dataType, typeName, columnSize, decimalDigits,
        numPrecRadix);

      cLogMessage.append("Calculated type: ").append(columnType.getClass().getSimpleName()).append("\n");

      ColumnStructure column = getColumnStructure(tableName, columnName, columnType, isNullable, index, remarks,
        defaultValue, isAutoIncrement);

      cLogMessage.append("ColumnType hash: ").append(column.getType().hashCode()).append("\n");
      logger.debug(cLogMessage);

      columns.add(column);
    }

    return columns;
  }

  /**
   * Map the original type to the normalized type model saving the appropriate
   * SQL:99 data type info
   *
   * @param schemaName
   *          The name of the associated schema, needed to resolve user defined
   *          data types
   * @param tableName
   *          The name of the associated table, needed to resolve user defined
   *          data types
   * @param columnName
   *          The name of the associated column, needed to resolve user defined
   *          data types
   * @param dataType
   *          the JDBC identifier of the original datatype
   * @param typeName
   *          the name of the original data type
   * @param columnSize
   *          the column size for the type
   * @param decimalDigits
   *          the number of decimal digits for the type
   * @param numPrecRadix
   *          Indicates the numeric radix of this data type, which is usually 2
   *          or 10
   * @return the normalized type
   * @throws UnknownTypeException
   *           the original type is unknown and cannot be mapped
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  protected Type getType(String schemaName, String tableName, String columnName, int dataType, String typeName,
    int columnSize, int decimalDigits, int numPrecRadix) throws UnknownTypeException, SQLException,
    ClassNotFoundException {
    Type type;
    switch (dataType) {
      case Types.BIGINT:
        type = new SimpleTypeNumericExact(columnSize, decimalDigits);
        if (decimalDigits > 0) {
          type.setSql99TypeName("NUMERIC", columnSize, decimalDigits);
          type.setSql2003TypeName("NUMERIC", columnSize, decimalDigits);
        } else {
          type.setSql99TypeName("NUMERIC", columnSize);
          type.setSql2003TypeName("NUMERIC", columnSize);
        }
        break;
      case Types.BINARY:
        type = getBinaryType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.BIT:
        if (columnSize > 1) {
          type = getBinaryType(typeName, columnSize, decimalDigits, numPrecRadix);
        } else {
          type = new SimpleTypeBoolean();
          type.setSql99TypeName("BOOLEAN");
          type.setSql2003TypeName("BOOLEAN");
        }
        break;
      case Types.BLOB:
        type = new SimpleTypeBinary(columnSize);
        type.setSql99TypeName("BINARY LARGE OBJECT");
        type.setSql2003TypeName("BINARY LARGE OBJECT");
        break;
      case Types.BOOLEAN:
        type = new SimpleTypeBoolean();
        type.setSql99TypeName("BOOLEAN");
        type.setSql2003TypeName("BOOLEAN");
        break;
      case Types.CHAR:
        type = new SimpleTypeString(columnSize, false);
        type.setSql99TypeName("CHARACTER", columnSize);
        type.setSql2003TypeName("CHARACTER", columnSize);
        type.setOriginalTypeName("CHARACTER", columnSize);
        break;
      case Types.NCHAR:
        // TODO add charset
        type = new SimpleTypeString(columnSize, false);
        type.setSql99TypeName("CHARACTER", columnSize);
        type.setSql2003TypeName("CHARACTER", columnSize);
        break;
      case Types.CLOB:
        type = new SimpleTypeString(columnSize, true);
        type.setSql99TypeName("CHARACTER LARGE OBJECT");
        type.setSql2003TypeName("CHARACTER LARGE OBJECT");
        break;
      case Types.DATE:
        type = getDateType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.DECIMAL:
        type = getDecimalType(typeName, columnSize, decimalDigits, numPrecRadix);
        if (StringUtils.isBlank(type.getOriginalTypeName())) {
          type.setOriginalTypeName(typeName, columnSize, decimalDigits);
        }
        break;
      case Types.DOUBLE:
        type = getDoubleType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.FLOAT:
        type = getFloatType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.INTEGER:
        type = new SimpleTypeNumericExact(columnSize, decimalDigits);
        type.setSql99TypeName("INTEGER");
        type.setSql2003TypeName("INTEGER");
        break;
      case Types.LONGVARBINARY:
        type = getLongvarbinaryType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.LONGVARCHAR:
        type = getLongvarcharType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.LONGNVARCHAR:
        type = new SimpleTypeString(columnSize, true);
        type.setSql99TypeName("CHARACTER LARGE OBJECT");
        type.setSql2003TypeName("CHARACTER LARGE OBJECT");
        break;
      case Types.NUMERIC:
        type = getNumericType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.REAL:
        type = getRealType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.SMALLINT:
        type = new SimpleTypeNumericExact(columnSize, decimalDigits);
        type.setSql99TypeName("SMALLINT");
        type.setSql2003TypeName("SMALLINT");
        break;
      case Types.TIME:
        type = getTimeType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.TIMESTAMP:
        type = getTimestampType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.TINYINT:
        type = new SimpleTypeNumericExact(columnSize, decimalDigits);
        type.setSql99TypeName("SMALLINT");
        type.setSql2003TypeName("SMALLINT");
        break;
      case Types.VARBINARY:
        type = getVarbinaryType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.VARCHAR:
        type = getVarcharType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.NVARCHAR:
        // TODO add charset
        type = new SimpleTypeString(columnSize, true);
        type.setSql99TypeName("CHARACTER VARYING", columnSize);
        type.setSql2003TypeName("CHARACTER VARYING", columnSize);
        break;
      case Types.ARRAY:
        Type subtype = getArraySubTypeFromTypeName(typeName, columnSize, numPrecRadix, numPrecRadix);
        type = new ComposedTypeArray(subtype);
        break;

      case Types.STRUCT:
        type = getComposedTypeStructure(schemaName, dataType, typeName);
        break;

      case Types.OTHER:
        logger.debug("Got an OTHER type, beware!");
        type = getOtherType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
        break;

      default:
        // tries to get specific DBMS data types
        type = getSpecificType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
        break;
    }

    if (StringUtils.isBlank(type.getOriginalTypeName())) {
      type.setOriginalTypeName(typeName);
    }

    return type;
  }

  protected Type getComposedTypeStructure(String schemaName, int dataType, String typeName) {
    ComposedTypeStructure type = null;

    SchemaStructure schema = actualSchema;
    if (schema == null) {
      schema = dbStructure.getSchemaByName(schemaName);
    }

    if (schema != null) {
      for (ComposedTypeStructure udt : schema.getUserDefinedTypes()) {
        if (udt.getOriginalTypeName().equalsIgnoreCase(typeName)) {
          type = udt;
          break;
        }
      }
    }

    if (type == null) {
      logger.warn("Struct type could not be identified! Note that it may still be found later on. Schema name: "
        + schemaName + ", data type: " + dataType + ", type name=" + typeName);
      return ComposedTypeStructure.empty;
    } else {
      return type;
    }
  }

  protected Type getVarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeBinary(columnSize);
    type.setSql99TypeName("BIT VARYING", columnSize);
    type.setSql2003TypeName("BINARY LARGE OBJECT");
    logger.info("using BIT VARYING(" + columnSize + ")");
    return type;
  }

  protected Type getRealType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeNumericApproximate(columnSize);
    type.setSql99TypeName("REAL");
    type.setSql2003TypeName("REAL");
    return type;
  }

  private Type getArraySubTypeFromTypeName(String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    Type subtype;
    if ("_char".equals(typeName)) {
      subtype = new SimpleTypeString(columnSize, false);
      subtype.setSql99TypeName("CHARACTER");
      subtype.setSql2003TypeName("CHARACTER");

    } else if ("_abstime".equals(typeName)) {
      subtype = getTimeType(typeName, columnSize, decimalDigits, numPrecRadix);
    } else {
      if (IGNORE_UNSUPPORTED_DATA_TYPES) {
        return new UnsupportedDataType(Types.ARRAY, typeName, columnSize, decimalDigits, numPrecRadix);
      } else {
        throw new UnknownTypeException("Unsupported JDBC type, code: " + typeName);
      }
    }
    return subtype;
  }

  protected Type getBinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeBinary(columnSize);
    type.setSql99TypeName("BIT");
    type.setSql2003TypeName("BIT");
    type.setOriginalTypeName(typeName, columnSize);
    return type;
  }

  protected Type getDateType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeDateTime(false, false);
    type.setSql99TypeName("DATE");
    type.setSql2003TypeName("DATE");
    return type;
  }

  protected Type getDecimalType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeNumericExact(columnSize, decimalDigits);
    if (decimalDigits > 0) {
      type.setSql99TypeName("DECIMAL", columnSize, decimalDigits);
      type.setSql2003TypeName("DECIMAL", columnSize, decimalDigits);
    } else {
      type.setSql99TypeName("DECIMAL", columnSize);
      type.setSql2003TypeName("DECIMAL", columnSize);
    }
    return type;
  }

  protected Type getNumericType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeNumericExact(columnSize, decimalDigits);
    if (decimalDigits > 0) {
      type.setSql99TypeName("NUMERIC", columnSize, decimalDigits);
      type.setSql2003TypeName("NUMERIC", columnSize, decimalDigits);
    } else {
      type.setSql99TypeName("NUMERIC", columnSize);
      type.setSql2003TypeName("NUMERIC", columnSize);
    }
    return type;
  }

  protected Type getDoubleType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeNumericApproximate(columnSize);
    type.setSql99TypeName("DOUBLE PRECISION");
    type.setSql2003TypeName("DOUBLE PRECISION");
    return type;
  }

  protected Type getFloatType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeNumericApproximate(columnSize);
    type.setSql99TypeName("FLOAT");
    type.setSql2003TypeName("FLOAT");
    return type;
  }

  protected Type getLongvarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeBinary(columnSize);
    type.setSql99TypeName("BINARY LARGE OBJECT");
    type.setSql2003TypeName("BINARY LARGE OBJECT");
    return type;
  }

  protected Type getLongvarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    Type type = new SimpleTypeString(columnSize, true);
    type.setSql99TypeName("CHARACTER LARGE OBJECT");
    type.setSql2003TypeName("CHARACTER LARGE OBJECT");
    return type;
  }

  protected Type getTimeType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeDateTime(true, false);
    type.setSql99TypeName("TIME");
    type.setSql2003TypeName("TIME");
    return type;
  }

  protected Type getTimestampType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeDateTime(true, false);
    type.setSql99TypeName("TIMESTAMP");
    type.setSql2003TypeName("TIMESTAMP");
    return type;
  }

  protected Type getVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeString(columnSize, true);
    type.setSql99TypeName("CHARACTER VARYING", columnSize);
    type.setSql2003TypeName("CHARACTER VARYING", columnSize);
    return type;
  }

  /**
   * Gets data types defined as Types.OTHER. The data type is inferred by
   * typeName, sometimes specific to each DBMS
   *
   * @param dataType
   * @param typeName
   * @param columnSize
   * @param numPrecRadix
   * @param decimalDigits
   * @return the inferred data type
   * @throws UnknownTypeException
   */
  protected Type getOtherType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    return getUnsupportedDataType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
  }

  /**
   * Gets specific DBMS data types. E.g.:OracleTypes.BINARY_DOUBLE
   *
   * @param dataType
   * @param typeName
   * @param columnSize
   * @param numPrecRadix
   * @param decimalDigits
   * @return the inferred data type
   * @throws UnknownTypeException
   */
  protected Type getSpecificType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    return getUnsupportedDataType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
  }

  /**
   * Gets the UnsupportedDataType. This data type is a placeholder for
   * unsupported data types
   *
   * @param dataType
   * @param typeName
   * @param columnSize
   * @param decimalDigits
   * @param numPrecRadix
   * @return
   * @throws UnknownTypeException
   */
  protected Type getUnsupportedDataType(int dataType, String typeName, int columnSize, int decimalDigits,
    int numPrecRadix) throws UnknownTypeException {
    if (IGNORE_UNSUPPORTED_DATA_TYPES) {
      return new UnsupportedDataType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
    } else {
      throw new UnknownTypeException("Unsupported JDBC type, code: " + dataType);
    }
  }

  /**
   * Get the table primary key
   *
   * @param tableName
   *          the name of the table
   * @return the primary key
   * @throws SQLException
   * @throws UnknownTypeException
   * @throws ClassNotFoundException
   * @throws ModuleException
   */
  protected PrimaryKey getPrimaryKey(String schemaName, String tableName) throws SQLException, UnknownTypeException,
    ClassNotFoundException {
    String pkName = null;
    List<String> pkColumns = new ArrayList<String>();

    ResultSet rs = getMetadata().getPrimaryKeys(getDatabaseStructure().getName(), schemaName, tableName);

    while (rs.next()) {
      pkName = rs.getString(6);
      pkColumns.add(rs.getString(4));
    }

    if (pkName == null) {
      pkName = tableName + "_pkey";
    }

    PrimaryKey pk = new PrimaryKey();
    pk.setName(pkName);
    pk.setColumnNames(pkColumns);
    return !pkColumns.isEmpty() ? pk : null;
  }

  /**
   * Get the table foreign keys
   *
   * @param schemaName
   *          the name of the schema
   * @param tableName
   *          the name of the table
   * @return the foreign keys
   * @throws SQLException
   * @throws UnknownTypeException
   * @throws ClassNotFoundException
   * @throws ModuleException
   */
  protected List<ForeignKey> getForeignKeys(String schemaName, String tableName) throws SQLException,
    UnknownTypeException, ClassNotFoundException {

    List<ForeignKey> foreignKeys = new ArrayList<ForeignKey>();

    ResultSet rs = getMetadata().getImportedKeys(getDatabaseStructure().getName(), schemaName, tableName);
    while (rs.next()) {
      List<Reference> references = new ArrayList<Reference>();
      boolean found = false;
      Reference reference = new Reference(rs.getString("FKCOLUMN_NAME"), rs.getString("PKCOLUMN_NAME"));

      String fkeyName = rs.getString("FK_NAME");
      if (fkeyName == null) {
        fkeyName = "FK_" + rs.getString("FKCOLUMN_NAME");
      }

      for (ForeignKey key : foreignKeys) {
        if (key.getName().equals(fkeyName)) {
          references = key.getReferences();
          references.add(reference);
          key.setReferences(references);
          found = true;
          break;
        }
      }

      if (!found) {
        ForeignKey fkey = new ForeignKey();
        fkey.setId(tableName + "." + rs.getString("FKCOLUMN_NAME"));
        fkey.setName(fkeyName);
        fkey.setReferencedSchema(getReferencedSchema(rs.getString("PKTABLE_SCHEM")));
        fkey.setReferencedTable(rs.getString("PKTABLE_NAME"));
        references.add(reference);
        fkey.setReferences(references);
        // TODO add: fkey.setMatchType(??);
        fkey.setUpdateAction(getUpdateRule(rs.getShort("UPDATE_RULE")));
        fkey.setDeleteAction(getDeleteRule(rs.getShort("DELETE_RULE")));
        foreignKeys.add(fkey);
      }
    }
    return foreignKeys;
  }

  protected String getReferencedSchema(String s) throws SQLException, ClassNotFoundException {
    return s;
  }

  /**
   * Gets the name of the update rule
   *
   * @param value
   * @return
   */
  protected String getUpdateRule(Short value) {
    String rule = null;
    switch (value) {
      case 0:
        rule = "CASCADE";
        break;
      case 1:
        rule = "RESTRICT";
        break;
      case 2:
        rule = "SET NULL";
        break;
      case 3:
        rule = "NO ACTION";
        break;
      case 4:
        rule = "SET DEFAULT";
        break;
      default:
        rule = "SET DEFAULT";
        break;
    }
    return rule;
  }

  /**
   * Gets the name of the delete rule
   *
   * @param value
   * @return
   */
  protected String getDeleteRule(Short value) {
    return getUpdateRule(value);
  }

  /**
   * Gets the candidate keys of a given schema table
   *
   * @param schemaName
   * @param tableName
   * @return
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  // VERIFY adding PKs
  protected List<CandidateKey> getCandidateKeys(String schemaName, String tableName) throws SQLException,
    ClassNotFoundException {
    List<CandidateKey> candidateKeys = new ArrayList<CandidateKey>();

    ResultSet rs = getMetadata().getIndexInfo(dbStructure.getName(), schemaName, tableName, true, true);
    while (rs.next()) {
      List<String> columns = new ArrayList<String>();
      boolean found = false;

      for (CandidateKey key : candidateKeys) {
        if (key.getName().equals(rs.getString(6))) {
          columns = key.getColumns();
          columns.add(rs.getString(9));
          key.setColumns(columns);
          found = true;
          break;
        }
      }

      if (!found) {
        if (rs.getString(6) != null) {
          CandidateKey candidateKey = new CandidateKey();
          candidateKey.setName(rs.getString(6));
          columns.add(rs.getString(9));
          candidateKey.setColumns(columns);
          candidateKeys.add(candidateKey);
        }
      }
    }
    return candidateKeys;
  }

  /**
   * Gets the check constraints of a given schema table
   *
   * @param schemaName
   * @param tableName
   * @return
   * @throws ClassNotFoundException
   */
  protected List<CheckConstraint> getCheckConstraints(String schemaName, String tableName)
    throws ClassNotFoundException {
    List<CheckConstraint> checkConstraints = new ArrayList<CheckConstraint>();

    String query = sqlHelper.getCheckConstraintsSQL(schemaName, tableName);
    if (query != null) {
      try {
        ResultSet rs = getStatement().executeQuery(query);
        while (rs.next()) {
          CheckConstraint checkConst = new CheckConstraint();

          String checkName;
          try {
            checkName = rs.getString("CHECK_NAME");
          } catch (SQLException e) {
            logger.debug("handled SQLException", e);
            checkName = "";
          }
          checkConst.setName(checkName);

          String checkCondition;
          try {
            checkCondition = rs.getString("CHECK_CONDITION");
          } catch (SQLException e) {
            logger.debug("handled SQLException", e);
            checkCondition = "UNKNOWN";
          }
          checkConst.setCondition(checkCondition);

          String checkDescription;
          try {
            checkDescription = rs.getString("CHECK_DESCRIPTION");
          } catch (SQLException e) {
            logger.debug("handled SQLException", e);
            checkDescription = null;
          }
          if (checkDescription != null) {
            checkConst.setDescription(checkDescription);
          }
          checkConstraints.add(checkConst);
        }
      } catch (SQLException e) {
        String message = "Check constraints were not imported for " + schemaName + "." + tableName + ". ";
        if (StringUtils.isBlank(query)) {
          message += "Not supported by " + sqlHelper.getName();
        } else {
          message += "An error occurred!";
        }
        logger.debug(message, e);
      }
    } else {
      logger.info("Check constraints were not imported: not supported yet on " + getClass().getSimpleName());
    }
    return checkConstraints;
  }

  /**
   * Gets the triggers of a given schema table
   *
   * @param schemaName
   * @param tableName
   * @return
   * @throws ClassNotFoundException
   */
  protected List<Trigger> getTriggers(String schemaName, String tableName) throws ClassNotFoundException {
    List<Trigger> triggers = new ArrayList<Trigger>();

    String query = sqlHelper.getTriggersSQL(schemaName, tableName);
    if (query != null) {
      try {
        ResultSet rs = getStatement().executeQuery(sqlHelper.getTriggersSQL(schemaName, tableName));

        while (rs.next()) {
          Trigger trigger = new Trigger();

          String triggerName;
          try {
            triggerName = rs.getString("TRIGGER_NAME");
          } catch (SQLException e) {
            logger.debug("handled SQLException", e);
            triggerName = "";
          }
          trigger.setName(triggerName);

          String actionTime;
          try {
            actionTime = processActionTime(rs.getString("ACTION_TIME"));
          } catch (SQLException e) {
            logger.debug("handled SQLException", e);
            actionTime = "";
          }
          trigger.setActionTime(actionTime);

          String triggerEvent;
          try {
            triggerEvent = processTriggerEvent(rs.getString("TRIGGER_EVENT"));
          } catch (SQLException e) {
            logger.debug("handled SQLException", e);
            triggerEvent = "";
          }
          trigger.setTriggerEvent(triggerEvent);

          String triggeredAction;
          try {
            triggeredAction = rs.getString("TRIGGERED_ACTION");
          } catch (SQLException e) {
            logger.debug("handled SQLException", e);
            triggeredAction = "";
          }
          trigger.setTriggeredAction(triggeredAction);

          String description;
          try {
            description = rs.getString("REMARKS");
          } catch (SQLException e) {
            logger.debug("handled SQLException", e);
            description = null;
          }
          if (description != null) {
            trigger.setDescription(description);
          }

          triggers.add(trigger);
        }
      } catch (SQLException e) {
        logger.debug("No triggers imported for " + schemaName + "." + tableName, e);
      }
    } else {
      logger.debug("Triggers were not imported: not supported yet on " + getClass().getSimpleName());
    }
    return triggers;
  }

  /**
   * Sanitizes the trigger event data
   *
   * @param string
   * @return
   */
  protected String processTriggerEvent(String string) {
    return string;
  }

  /**
   * Sanitizes the trigger action time data
   *
   * @param string
   * @return
   */
  protected String processActionTime(String string) {
    return string;
  }

  protected Row convertRawToRow(ResultSet rawData, TableStructure tableStructure) throws InvalidDataException,
    SQLException, ClassNotFoundException, ModuleException {
    Row row = null;
    if (isRowValid(rawData, tableStructure)) {
      List<Cell> cells = new ArrayList<Cell>(tableStructure.getColumns().size());

      long currentRow = tableStructure.getCurrentRow();
      if (isGetRowAvailable()) {
        currentRow = rawData.getRow();
      }

      for (int i = 0; i < tableStructure.getColumns().size(); i++) {
        ColumnStructure colStruct = tableStructure.getColumns().get(i);
        cells.add(convertRawToCell(tableStructure.getName(), colStruct.getName(), i + 1, currentRow,
          colStruct.getType(), rawData));
      }
      row = new Row(currentRow, cells);
    }
    tableStructure.incrementCurrentRow();
    return row;
  }

  protected Cell convertRawToCell(String tableName, String columnName, int columnIndex, long rowIndex, Type cellType,
    ResultSet rawData) throws SQLException, InvalidDataException, ClassNotFoundException, ModuleException {
    Cell cell;
    String id = tableName + "." + columnName + "." + rowIndex;
    if (cellType instanceof ComposedTypeArray) {
      ComposedTypeArray composedTypeArray = (ComposedTypeArray) cellType;
      Array array = rawData.getArray(columnName);
      logger.debug("Parsing array of subtype " + composedTypeArray.getElementType().getClass().getSimpleName());
      List<Cell> cells = parseArray(id, array);
      cell = new ComposedCell(id, cells);
    } else if (cellType instanceof ComposedTypeStructure) {
      cell = rawToCellComposedTypeStructure(id, columnName, cellType, rawData);
    } else if (cellType instanceof SimpleTypeBoolean) {
      boolean booleanValue = rawData.getBoolean(columnName);
      boolean wasNull = rawData.wasNull();
      if (wasNull) {
        cell = new SimpleCell(id, null);
      } else {
        cell = new SimpleCell(id, booleanValue ? "true" : "false");
      }

    } else if (cellType instanceof SimpleTypeNumericApproximate) {
      cell = rawToCellSimpleTypeNumericApproximate(id, columnName, cellType, rawData);
    } else if (cellType instanceof SimpleTypeDateTime) {
      cell = rawToCellSimpleTypeDateTime(id, columnName, cellType, rawData);
    } else if (cellType instanceof SimpleTypeBinary) {
      cell = rawToCellSimpleTypeBinary(id, columnName, cellType, rawData);
    } else if (cellType instanceof UnsupportedDataType) {
      try {
        cell = new SimpleCell(id, rawData.getString(columnName));
      } catch (SQLException e) {
        logger.warn("Could not export cell of unsupported datatype: OTHER", e);
        cell = new SimpleCell(id);
      }
    } else if (cellType instanceof SimpleTypeNumericExact) {
      cell = rawToCellSimpleTypeNumericExact(id, columnName, cellType, rawData);
    } else {
      try {
        cell = new SimpleCell(id, rawData.getString(columnName));
      } catch (SQLException e) {
        logger.warn("Could not export cell of unknown/undefined datatype", e);
        cell = new SimpleCell(id);
      } catch (NoClassDefFoundError e) {
        try {
          Object[] aStruct = ((STRUCT) rawData.getObject(columnName)).getAttributes();

          StringBuilder value = new StringBuilder("(");
          String separator = "";
          for (Object o : aStruct) {
            value.append(separator).append(o.toString());
            separator = ",";
          }
          value.append(")");

          cell = new SimpleCell(id, value.toString());
        } catch (SQLException e1) {
          logger.warn("Could not export cell of unknown/undefined datatype", e);
          cell = new SimpleCell(id);
        }
      }
    }
    return cell;
  }

  protected Cell rawToCellComposedTypeStructure(String id, String columnName, Type cellType, ResultSet rawData)
    throws InvalidDataException {
    throw new InvalidDataException("Convert data of struct type not yet supported");
  }

  protected Cell rawToCellSimpleTypeNumericExact(String id, String columnName, Type cellType, ResultSet rawData)
    throws SQLException {
    return new SimpleCell(id, rawData.getString(columnName));
  }

  protected List<Cell> parseArray(String baseid, Array array) throws SQLException, InvalidDataException {
    if (array == null) {
      return null;
    }
    int baseType = array.getBaseType();
    List<Cell> cells = new ArrayList<Cell>();
    Object[] items;
    try {
      items = (Object[]) array.getArray();

      switch (baseType) {
        case Types.CHAR:
        case Types.VARCHAR:
          for (int i = 0; i < items.length; i++) {
            String item = (String) items[i];
            cells.add(new SimpleCell(baseid + "." + i, item));
          }
          break;
        case Types.BIT:
          for (int i = 0; i < items.length; i++) {
            Boolean item = (Boolean) items[i];
            cells.add(new SimpleCell(baseid + "." + i, item.toString()));
          }
          break;
        case Types.DATE:
          for (int i = 0; i < items.length; i++) {
            Date item = (Date) items[i];
            // XXX should date be formatted as ISO8601?
            cells.add(new SimpleCell(baseid + "." + i, item.toString()));
          }
          break;
        case Types.INTEGER:
          for (int i = 0; i < items.length; i++) {
            Integer item = (Integer) items[i];
            cells.add(new SimpleCell(baseid + "." + i, item.toString()));
          }
          break;
        case Types.DOUBLE:
          for (int i = 0; i < items.length; i++) {
            Double item = (Double) items[i];
            cells.add(new SimpleCell(baseid + "." + i, item.toString()));
          }
          break;
        default:
          throw new InvalidDataException("Convert data of array of base type '" + baseType
            + "' not yet supported! Java class type is " + items.getClass());
      }
    } catch (SQLFeatureNotSupportedException e) {
      logger.warn("Got a problem getting Array value: " + e.getMessage(), e);
      logger.warn("Trying via result set, string representations will not be controlled!");
      ResultSet rs = array.getResultSet();
      while (rs.next()) {
        String item = rs.getString(1);
        cells.add(new SimpleCell(baseid, item));
      }
    }

    return cells;
  }

  protected Cell rawToCellSimpleTypeNumericApproximate(String id, String columnName, Type cellType, ResultSet rawData)
    throws SQLException {

    String stringValue = rawData.getString(columnName);
    boolean wasNull = rawData.wasNull();
    SimpleCell cell;
    if (wasNull) {
      cell = new SimpleCell(id, null);
    } else {
      cell = new SimpleCell(id, stringValue);
    }
    logger.trace("rawToCellSimpleTypeNumericApproximate cell: " + (cell.getSimpledata()));

    return cell;
  }

  protected Cell rawToCellSimpleTypeDateTime(String id, String columnName, Type cellType, ResultSet rawData)
    throws SQLException {
    Cell cell = null;
    SimpleTypeDateTime undefinedDate = (SimpleTypeDateTime) cellType;
    if (undefinedDate.getTimeDefined()) {
      if ("TIME".equalsIgnoreCase(cellType.getSql99TypeName())
        || "TIME WITH TIME ZONE".equalsIgnoreCase(cellType.getSql99TypeName())) {
        Time time = rawData.getTime(columnName);
        if (time != null) {
          cell = new SimpleCell(id, time.toString());
        } else {
          cell = new SimpleCell(id, null);
        }
      } else {
        Timestamp timestamp = rawData.getTimestamp(columnName);
        if (timestamp != null) {
          String isoDate = DateParser.getIsoDate(timestamp);
          cell = new SimpleCell(id, isoDate);
        } else {
          cell = new SimpleCell(id, null);
        }
      }
    } else {
      Date date = rawData.getDate(columnName);
      if (date != null) {
        cell = new SimpleCell(id, date.toString());
      } else {
        cell = new SimpleCell(id, null);
      }
    }
    logger.trace("rawToCellSimpleTypeDateTime cell: " + (((SimpleCell) cell).getSimpledata()));
    return cell;
  }

  protected Cell rawToCellSimpleTypeBinary(String id, String columnName, Type cellType, ResultSet rawData)
    throws SQLException, ModuleException {
    Cell cell;
    InputStream binaryStream = rawData.getBinaryStream(columnName);
    if (binaryStream != null) {
      FileItem fileItem = new FileItem(binaryStream);
      FileFormat fileFormat = FormatUtility.getFileFormat(fileItem.getFile());
      List<FileFormat> formats = new ArrayList<FileFormat>();
      formats.add(fileFormat);
      cell = new BinaryCell(id, fileItem, formats);

      try {
        binaryStream.close();
      } catch (IOException e) {
        logger.error(e);
      }
    } else {
      cell = new BinaryCell(id);
    }
    return cell;
  }

  protected boolean isRowValid(ResultSet raw, TableStructure structure) throws InvalidDataException, SQLException {
    boolean ret;
    ResultSetMetaData metadata = raw.getMetaData();
    if (metadata.getColumnCount() == structure.getColumns().size()) {
      ret = true;
    } else {
      ret = false;
      throw new InvalidDataException("table: " + structure.getName() + " row number: " + raw.getRow()
        + " error: different column number from structure " + metadata.getColumnCount() + "!="
        + structure.getColumns().size());
    }
    return ret;
  }

  protected ResultSet getTableRawData(TableStructure table) throws SQLException, ClassNotFoundException,
    ModuleException {
    logger.debug("query: " + sqlHelper.selectTableSQL(table.getId()));
    ResultSet set = getStatement().executeQuery(sqlHelper.selectTableSQL(table.getId()));
    set.setFetchSize(ROW_FETCH_BLOCK_SIZE);
    return set;
  }

  /**
   * Gets the schemas that won't be exported.
   * <p>
   * Accepts schemas names in as regular expressions I.e. SYS.* will ignore
   * SYSCAT, SYSFUN, etc
   *
   * @return the schemas to be ignored at export
   */
  protected Set<String> getIgnoredExportedSchemas() {
    HashSet ignore = new HashSet<String>();
    ignore.add("information_schema");
    ignore.add("pg_catalog");
    return ignore;
  }

  @Override
  public void getDatabase(DatabaseExportModule handler) throws ModuleException, UnknownTypeException,
    InvalidDataException {
    try {
      logger.debug("initializing database");
      handler.initDatabase();
      // sets schemas won't be exported
      handler.setIgnoredSchemas(getIgnoredExportedSchemas());
      logger.info("STARTED: Getting the database structure.");
      handler.handleStructure(getDatabaseStructure());
      logger.info("FINISHED: Getting the database structure.");
      // logger.debug("db struct: " + getDatabaseStructure().toString());
      for (SchemaStructure schema : getDatabaseStructure().getSchemas()) {
        handler.handleDataOpenSchema(schema.getName());
        for (TableStructure table : schema.getTables()) {
          logger.info("STARTED: Getting data of table: " + table.getId());
          ResultSet tableRawData = getTableRawData(table);
          handler.handleDataOpenTable(table.getId());
          int nRows = 0;
          while (tableRawData.next()) {
            handler.handleDataRow(convertRawToRow(tableRawData, table));
            nRows++;
            if (nRows % 1000 == 0) {
              logger.info(nRows + " rows processed");
            }
          }
          logger.info("Total of " + nRows + " row(s) processed");
          getDatabaseStructure().lookupTableStructure(table.getId()).setRows(nRows);
          handler.handleDataCloseTable(table.getId());
          logger.info("FINISHED: Getting data of table: " + table.getId());
        }
        handler.handleDataCloseSchema(schema.getName());
      }
      logger.debug("finishing database");
      handler.finishDatabase();
    } catch (SQLException e) {
      throw new ModuleException("SQL error while conecting", e);
    } catch (ClassNotFoundException e) {
      throw new ModuleException("JDBC driver class could not be found", e);
    } finally {
      try {
        closeConnection();
      } catch (SQLException e) {
        throw new ModuleException("Error while closing connection", e);
      }
    }
  }

}
