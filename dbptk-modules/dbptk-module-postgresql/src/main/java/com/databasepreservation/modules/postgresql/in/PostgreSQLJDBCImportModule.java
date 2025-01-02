/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
/**
 *
 */
package com.databasepreservation.modules.postgresql.in;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.databasepreservation.Constants;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.PGConnection;
import org.postgresql.core.Oid;
import org.postgresql.jdbc.PgResultSet;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.common.io.providers.InputStreamProvider;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.ComposedCell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.CheckConstraint;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.RoleStructure;
import com.databasepreservation.model.structure.RoutineStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.Trigger;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.model.structure.type.ComposedTypeStructure;
import com.databasepreservation.model.structure.type.ComposedTypeStructure.SubType;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.CloseableUtils;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.postgresql.PostgreSQLExceptionNormalizer;
import com.databasepreservation.modules.postgresql.PostgreSQLHelper;
import com.databasepreservation.modules.postgresql.PostgreSQLModuleFactory;
import com.databasepreservation.utils.MapUtils;
import com.databasepreservation.utils.RemoteConnectionUtils;

/**
 * <p>
 * Module to import data from a PostgreSQL database management system via JDBC
 * driver. The postgresql-8.3-603.jdbc3.jar driver supports PostgreSQL version
 * 7.4 to 8.3.
 * </p>
 * <p>
 * <p>
 * To use this module, the PostgreSQL server must be configured:
 * </p>
 * <ol>
 * <li>Server must be configured to accept TCP/IP connections. This can be done
 * by setting <code>listen_addresses = 'localhost'</code> (or
 * <code>tcpip_socket = true</code> in older versions) in the postgresql.conf
 * file.</li>
 * <li>The client authentication setup in the pg_hba.conf file may need to be
 * configured, adding a line like
 * <code>host all all 127.0.0.1 255.0.0.0 trust</code>. The JDBC driver supports
 * the trust, ident, password, md5, and crypt authentication methods.</li>
 * </ol>
 *
 * @author Luis Faria <lfaria@keep.pt>
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class PostgreSQLJDBCImportModule extends JDBCImportModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLJDBCImportModule.class);

  /**
   * Create a new PostgreSQL JDBC import module
   *
   * @param hostname
   *          the name of the PostgreSQL server host (e.g. localhost)
   * @param port
   *          the port of where the PostgreSQL server is listening, default is
   *          5432
   * @param database
   *          the name of the database to connect to
   * @param username
   *          the name of the user to use in connection
   * @param password
   *          the password of the user to use in connection
   * @param encrypt
   *          encrypt connection
   */
  public PostgreSQLJDBCImportModule(String moduleName, String hostname, int port, String database, String username,
    String password, boolean encrypt) {
    super("org.postgresql.Driver",
      "jdbc:postgresql://" + hostname + ":" + port + "/" + database + (encrypt ? "?ssl=true" : ""),
      new PostgreSQLHelper(), new PostgreSQLJDBCDatatypeImporter(), moduleName,
      MapUtils.buildMapFromObjects(PostgreSQLModuleFactory.PARAMETER_HOSTNAME, hostname,
        PostgreSQLModuleFactory.PARAMETER_PORT_NUMBER, port, PostgreSQLModuleFactory.PARAMETER_USERNAME, username,
        PostgreSQLModuleFactory.PARAMETER_PASSWORD, password, PostgreSQLModuleFactory.PARAMETER_DATABASE, database,
        PostgreSQLModuleFactory.PARAMETER_DISABLE_ENCRYPTION, !encrypt));

    createCredentialsProperty(username, password);
  }

  public PostgreSQLJDBCImportModule(String moduleName, String hostname, int port, String database, String username,
    String password, boolean encrypt, String sshHost, String sshUser, String sshPassword, String sshPortNumber)
    throws ModuleException {
    super("org.postgresql.Driver",
      "jdbc:postgresql://" + hostname + ":" + port + "/" + database + (encrypt ? "?ssl=true" : ""),
      new PostgreSQLHelper(), new PostgreSQLJDBCDatatypeImporter(), moduleName,
      MapUtils.buildMapFromObjects(PostgreSQLModuleFactory.PARAMETER_HOSTNAME, hostname,
        PostgreSQLModuleFactory.PARAMETER_PORT_NUMBER, port, PostgreSQLModuleFactory.PARAMETER_USERNAME, username,
        PostgreSQLModuleFactory.PARAMETER_PASSWORD, password, PostgreSQLModuleFactory.PARAMETER_DATABASE, database,
        PostgreSQLModuleFactory.PARAMETER_DISABLE_ENCRYPTION, !encrypt),
      MapUtils.buildMapFromObjects(PostgreSQLModuleFactory.PARAMETER_SSH, true,
        PostgreSQLModuleFactory.PARAMETER_SSH_HOST, sshHost, PostgreSQLModuleFactory.PARAMETER_SSH_PORT, sshPortNumber,
        PostgreSQLModuleFactory.PARAMETER_SSH_USER, sshUser, PostgreSQLModuleFactory.PARAMETER_SSH_PASSWORD,
        sshPassword));

    createCredentialsProperty(username, password);
  }

  @Override
  protected Connection createConnection() throws ModuleException {
    Connection connection;
    try {
      if (ssh) {
        connectionURL = RemoteConnectionUtils.replaceHostAndPort(connectionURL);
      }
      connection = DriverManager.getConnection(connectionURL, getCredentials());
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      throw normalizeException(e, null);
    }
    LOGGER.debug("Connected");
    return connection;
  }

  @Override
  protected Statement getStatement() throws SQLException, ModuleException {
    if (statement == null) {
      statement = getConnection().createStatement();
    }
    return statement;
  }

  @Override
  protected List<ViewStructure> getViews(String schemaName) throws SQLException, ModuleException {
    List<ViewStructure> views = super.getViews(schemaName);
    for (ViewStructure v : views) {
      Statement statement = null;
      ResultSet rset = null;
      String viewQueryOriginal = "";
      if (v.getQueryOriginal() == null || v.getQueryOriginal().isEmpty()) {
        try {
          statement = getConnection().createStatement();
          String query = "";

          if (v.getViewType().equals(Constants.TYPE_MATERIALIZED_VIEW)) {
            query = ((PostgreSQLHelper) sqlHelper).getMaterializedViewQueryOriginal(schemaName, v.getName());
          } else if (v.getViewType().equals(Constants.TYPE_VIEW)) {
            query = ((PostgreSQLHelper) sqlHelper).getViewQueryOriginal(schemaName, v.getName());
          }

          rset = statement.executeQuery(query);
          rset.next(); // Returns only one tuple

          viewQueryOriginal = rset.getString(1);
        } catch (Exception e) {
          LOGGER.debug("Exception trying to get view SQL in PostgreSQL", e);
        } finally {
          CloseableUtils.closeQuietly(rset);
          CloseableUtils.closeQuietly(statement);
        }

        if (StringUtils.isBlank(viewQueryOriginal)) {
          reporter.customMessage("PostgreSQLJDBCImportModule",
            "Could not obtain SQL statement for view " + sqlHelper.escapeViewName(schemaName, v.getName()));
        } else {
          v.setQueryOriginal(viewQueryOriginal);
        }
      }
    }
    return views;
  }

  @Override
  protected ResultSet getTableRawData(TableStructure table) throws SQLException, ModuleException {

    // builds a query like "SELECT field1, field2, field3 FROM table"
    HashSet<String> columnNames = new HashSet<>();
    StringBuilder query = new StringBuilder("SELECT ");
    ArrayList<ColumnStructure> udtColumns = new ArrayList<>();
    String separator = "";
    for (ColumnStructure column : table.getColumns()) {
      if (column.getType() instanceof ComposedTypeStructure) {
        udtColumns.add(column);
      }
      columnNames.add(column.getName());
      query.append(separator).append(sqlHelper.escapeTableName(column.getName()));
      separator = ", ";
    }

    for (ColumnStructure column : udtColumns) {
      query.append(separator).append(getFieldNamesFromComposedTypeStructure(column.getId(),
        (ComposedTypeStructure) column.getType(), table, columnNames));
    }

    query.append(" FROM ").append(sqlHelper.escapeTableId(table.getId()));

    String resultQuery = query.toString();

    String whereClause = getModuleConfiguration().getWhere(table.getSchema(), table.getName(), !table.isFromView());
    String orderByClause = getModuleConfiguration().getOrderBy(table.getSchema(), table.getName(), !table.isFromView());
    if (whereClause != null) {
      resultQuery = sqlHelper.appendWhereClause(resultQuery, whereClause);
    }

    if (orderByClause != null) {
      resultQuery = sqlHelper.appendOrderByClause(resultQuery, orderByClause);
    }

    LOGGER.debug("query: " + resultQuery);

    // use a high fetchSize, reducing it if it proves to be too high
    // since you are reading this, you might as well check if this related pull
    // request https://github.com/pgjdbc/pgjdbc/pull/675 has already been merged
    return getTableRawData(resultQuery, table.getId());
  }

  /**
   * Explores a composed type and retrieves complete names for all simple types
   * inside the composed type, using those complete names in a query allows the
   * lossless retrieval of all the data contained inside the UDT cell
   */
  private String getFieldNamesFromComposedTypeStructure(String columnId,
    ComposedTypeStructure baseComposedTypeStructure, TableStructure table, HashSet<String> columnNames) {
    // TODO: use getNonComposedSubTypes to get all non-composed subtypes in
    // a hierarchy
    // ArrayList<SubType> subtypes =
    // baseComposedTypeStructure.getNonComposedSubTypes(columnId);
    List<SubType> subtypes = baseComposedTypeStructure.getDirectDescendantSubTypes(columnId);

    StringBuilder sb = new StringBuilder();
    String separator = "";
    for (SubType subtype : subtypes) {
      List<String> names = subtype.getPath();

      if (names.size() < 2) {
        LOGGER.debug("UDT type hierarchy is too small. columnId: " + columnId + ", " + subtype.toString());
      }

      sb.append(separator);

      StringBuilder name = new StringBuilder();
      name.append("(").append(names.get(0)).append(")");
      for (int i = 1; i < names.size(); i++) {
        name.append(".").append(names.get(i));
      }
      sb.append(name);

      String alias = RandomStringUtils.random(15, "abcdefghijklmnopqrstuvwxyz");
      while (columnNames.contains(alias)) {
        LOGGER.debug("random alias: column name '" + alias + "' exists.");
        alias = RandomStringUtils.random(15, "abcdefghijklmnopqrstuvwxyz");
      }
      columnNames.add(alias);
      table.addUDTAlias(name.toString(), alias);

      sb.append(" AS " + alias);

      separator = ", ";
    }

    return sb.toString();
  }

  @Override
  protected Row convertRawToRow(ResultSet rawData, TableStructure tableStructure)
    throws InvalidDataException, SQLException, ModuleException {
    Row row = null;
    if (isRowValid(rawData, tableStructure)) {
      List<Cell> cells = new ArrayList<Cell>(tableStructure.getColumns().size());

      long currentRow = tableStructure.getCurrentRow();
      if (isGetRowAvailable()) {
        currentRow = rawData.getRow();
      }

      ArrayList<Integer> udtColumnsIndexes = new ArrayList<>();
      int i;
      for (i = 0; i < tableStructure.getColumns().size(); i++) {
        ColumnStructure colStruct = tableStructure.getColumns().get(i);

        if (colStruct.getType() instanceof ComposedTypeStructure) {
          // build composed types later
          udtColumnsIndexes.add(i);
          cells.add(null);
        } else {
          Cell cell;
          try {
            cell = convertRawToCell(tableStructure.getName(), colStruct.getName(), i + 1, currentRow,
              colStruct.getType(), rawData);
          } catch (Exception e) {
            cell = new SimpleCell(tableStructure.getName() + "." + colStruct.getName() + "." + (i + 1), null);
            reporter.cellProcessingUsedNull(tableStructure, colStruct, currentRow, e);
          }
          cells.add(cell);
        }
      }

      // build composed types
      for (Integer udtColumnIndex : udtColumnsIndexes) {
        List<Cell> udtCells = new ArrayList<>();
        ColumnStructure udtColumn = tableStructure.getColumns().get(udtColumnIndex);
        ComposedTypeStructure udtType = (ComposedTypeStructure) udtColumn.getType();

        // TODO: use getNonComposedSubTypes to get all non-composed subtypes in
        // a hierarchy
        List<SubType> subtypes = udtType.getDirectDescendantSubTypes(udtColumn.getId());
        for (SubType subtype : subtypes) {
          List<String> names = subtype.getPath();
          StringBuilder extraColumnName = new StringBuilder();
          extraColumnName.append("(").append(names.get(0)).append(")");
          for (int namesIndex = 1; namesIndex < names.size(); namesIndex++) {
            extraColumnName.append(".").append(names.get(namesIndex));
          }

          String aliasColumnName = tableStructure.getUDTAlias(extraColumnName.toString());

          Cell cell;
          try {
            cell = convertRawToCell(tableStructure.getName(), aliasColumnName, udtColumnIndex + 1, currentRow,
              subtype.getType(), rawData);
          } catch (Exception e) {
            cell = new SimpleCell(tableStructure.getName() + "." + aliasColumnName + "." + (udtColumnIndex + 1), null);
            reporter.cellProcessingUsedNull(tableStructure, udtColumn, currentRow, e);
          }
          udtCells.add(cell);

          i++;
        }

        cells.set(udtColumnIndex,
          new ComposedCell(tableStructure.getName() + "." + udtColumn.getName() + "." + currentRow, udtCells));
      }

      row = new Row(currentRow, cells);
    } else {
      // insert null in all fields
      List<Cell> cells = new ArrayList<Cell>(tableStructure.getColumns().size());
      for (int i = 0; i < tableStructure.getColumns().size(); i++) {
        ColumnStructure colStruct = tableStructure.getColumns().get(i);
        cells.add(new SimpleCell(tableStructure.getName() + "." + colStruct.getName() + "." + (i + 1), null));
      }
      row = new Row(tableStructure.getCurrentRow(), cells);

      reporter.rowProcessingUsedNull(tableStructure, tableStructure.getCurrentRow(),
        new ModuleException().withMessage("isRowValid returned false"));
    }
    tableStructure.incrementCurrentRow();
    return row;
  }

  @Override
  protected boolean isRowValid(ResultSet raw, TableStructure structure) throws InvalidDataException, SQLException {
    boolean ret;
    ResultSetMetaData metadata = raw.getMetaData();

    // number of columns is not enough because UDTs are expanded into fields
    List<ColumnStructure> columns = structure.getColumns();
    int colNumber = columns.size();

    for (ColumnStructure column : columns) {
      if (column.getType() instanceof ComposedTypeStructure) {
        ComposedTypeStructure type = (ComposedTypeStructure) column.getType();
        // TODO: use getNonComposedSubTypes to get all non-composed subtypes in
        // a hierarchy
        colNumber += type.getDirectDescendantSubTypes(column.getName()).size();
      }
    }

    if (metadata.getColumnCount() == colNumber) {
      ret = true;
    } else {
      ret = false;
      LOGGER.debug("Invalid row",
        new InvalidDataException("table: " + structure.getName() + " row number: " + raw.getRow()
          + " error: different column number from structure. Got " + metadata.getColumnCount()
          + " columns and expected " + structure.getColumns().size() + " (or " + colNumber + " with UDT fields)"));
    }
    return ret;
  }

  /**
   * Gets the schemas that won't be exported. Defaults to PostgreSQL are
   * information_schema and all pg_XXX
   */
  @Override
  public Set<String> getIgnoredExportedSchemas() {
    Set<String> ignoredSchemas = new HashSet<String>();
    ignoredSchemas.add("information_schema");
    ignoredSchemas.add("pg_.*");

    return ignoredSchemas;
  }

  /**
   * Drops money currency
   */
  @Override
  protected Cell rawToCellSimpleTypeNumericApproximate(String id, String columnName, Type cellType, ResultSet rawData)
    throws SQLException {
    Cell cell = null;
    if ("MONEY".equalsIgnoreCase(cellType.getOriginalTypeName())) {
      String data = rawData.getString(columnName);
      if (data != null) {
        String parts[] = data.split(" ");
        if (parts[1] != null) {
          LOGGER.warn("Money currency lost: " + parts[1]);
        }
        cell = new SimpleCell(id, parts[0]);
      } else {
        cell = new SimpleCell(id, null);
      }

    } else {
      String value;
      if ("float4".equalsIgnoreCase(cellType.getOriginalTypeName())) {
        Float f = rawData.getFloat(columnName);
        value = rawData.wasNull() ? null : f.toString();
      } else {
        Double d = rawData.getDouble(columnName);
        value = rawData.wasNull() ? null : d.toString();
      }
      cell = new SimpleCell(id, value);
    }
    return cell;
  }

  /**
   * Treats bit strings, as the default behavior does not build PostgreSQL byte
   * streams correctly
   */
  @Override
  protected Cell rawToCellSimpleTypeBinary(String id, String columnName, Type cellType, ResultSet genericRawData)
    throws SQLException, ModuleException {
    PgResultSet rawData = (PgResultSet) genericRawData;
    Cell cell;
    InputStream binaryStream;
    if ("bit".equalsIgnoreCase(cellType.getOriginalTypeName())
      || "varbit".equalsIgnoreCase(cellType.getOriginalTypeName())) {
      String bitString = rawData.getString(columnName);
      String hexString = new BigInteger(bitString, 2).toString(16);
      if ((hexString.length() % 2) != 0) {
        hexString = "0" + hexString;
      }

      if (hexString.isEmpty()) {
        cell = new NullCell(id);
      } else {
        byte[] bytes = hexStringToByteArray(hexString);
        binaryStream = new ByteArrayInputStream(bytes);
        cell = new BinaryCell(id, binaryStream);
      }
    } else if ("bytea".equalsIgnoreCase(cellType.getOriginalTypeName())) {
      // bferreira, 2016-12-27 - do not use getBinaryStream in PostgreSQL,
      // because the driver has a memory leak in that method (and then it uses
      // some getBytes-equivalent method to fetch the data)
      byte[] bytes = rawData.getBytes(columnName);
      if (bytes != null) {
        binaryStream = new ByteArrayInputStream(bytes);
        cell = new BinaryCell(id, binaryStream);
      } else {
        cell = new NullCell(id);
      }
    } else if (rawData.getColumnOID(rawData.findColumn(columnName)) == Oid.OID) {
      long lobObjectID = rawData.getLong(columnName);
      if (rawData.wasNull()) {
        cell = new NullCell(id);
      } else {
        cell = new PostgresBinaryCell(id, getConnection(), lobObjectID);
      }
    } else {
      cell = super.rawToCellSimpleTypeBinary(id, columnName, cellType, genericRawData);
    }

    return cell;
  }

  @Override
  protected Cell rawToCellSimpleTypeDateTime(String id, String columnName, Type cellType, ResultSet rawData)
    throws SQLException {
    Cell cell = null;
    SimpleTypeDateTime undefinedDate = (SimpleTypeDateTime) cellType;
    if (undefinedDate.getTimeDefined()) {
      if ("TIME WITH TIME ZONE".equalsIgnoreCase(cellType.getSql99TypeName())) {
        String time_string = rawData.getString(columnName);
        if (time_string.matches("^\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{2}$")) {
          cell = new SimpleCell(id, time_string + ":00");
          LOGGER.trace("rawToCellSimpleTypeDateTime cell: " + (((SimpleCell) cell).getSimpleData()));
        } else if (time_string.matches("^\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{2}:\\d{2}$")) {
          cell = new SimpleCell(id, time_string);
          LOGGER.trace("rawToCellSimpleTypeDateTime cell: " + (((SimpleCell) cell).getSimpleData()));
        } else {
          cell = super.rawToCellSimpleTypeDateTime(id, columnName, cellType, rawData);
        }
      } else if ("TIMESTAMP WITH TIME ZONE".equalsIgnoreCase(cellType.getSql99TypeName())) {
        final Timestamp timestamp = rawData.getTimestamp(columnName);
        if (timestamp != null) {
          cell = new SimpleCell(id, timestamp.toInstant().toString());
        } else {
          cell = super.rawToCellSimpleTypeDateTime(id, columnName, cellType, rawData);
        }
      } else {
        cell = super.rawToCellSimpleTypeDateTime(id, columnName, cellType, rawData);
      }
    } else {
      cell = super.rawToCellSimpleTypeDateTime(id, columnName, cellType, rawData);
    }
    return cell;
  }

  @Override
  protected Cell rawToCellComposedTypeStructure(String id, String columnName, Type cellType, ResultSet rawData)
    throws InvalidDataException {
    LOGGER.debug("postgresql, rawToCellComposedTypeStructure, ignored composed type cell");
    return null;
  }

  private static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    LOGGER.debug("len: " + len / 2);
    byte[] data = new byte[len / 2];
    LOGGER.debug("data length: " + data.length);
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  /**
   * This binary cell uses PostgreSQL specific code to postpone obtaining the LOB
   * until it is really necessary and then provides streams to read the lob.
   *
   * Using PostgreSQL default implementation and BinaryCell, the LOB would be
   * loaded to memory, written to a temporary file, then read from the temporary
   * file to whatever destination the export module had prepared.
   *
   * This implementation does not avoid loading the whole LOB to memory but avoids
   * wasting time and resources by skipping the temporary file and providing the
   * export module with a stream coming directly from the LOB representation
   * provided by PostgreSQL.
   */
  private static class PostgresBinaryCell extends BinaryCell {
    public PostgresBinaryCell(String id, Connection connection, final Long objectId) {
      super(id, new PostgresInputStreamProvider(connection, objectId));
    }
  }

  private static class PostgresInputStreamProvider implements InputStreamProvider {
    private final Long objectId;
    private final Connection connection;

    public PostgresInputStreamProvider(Connection connection, Long objectId) {
      this.objectId = objectId;
      this.connection = connection;
    }

    @Override
    public InputStream createInputStream() throws ModuleException {
      try {
        LargeObjectManager largeObjectManager = ((PGConnection) connection).getLargeObjectAPI();
        final LargeObject largeObject = largeObjectManager.open(objectId);
        final InputStream largeObjectInputStream = largeObject.getInputStream();
        return new InputStream() {
          @Override
          public int read() throws IOException {
            return largeObjectInputStream.read();
          }

          @Override
          public void close() throws IOException {
            super.close();
            // after closing the largeObject the stream becomes unreadable
            try {
              largeObject.close();
            } catch (SQLException e) {
              LOGGER.debug("Could not close large object", e);
            }
          }
        };
      } catch (SQLException e) {
        throw new ModuleException().withMessage("Could not open blob stream").withCause(e);
      }
    }

    @Override
    public void cleanResources() {
      // nothing to do here. closing the stream cleans the largeObject
    }

    @Override
    public long getSize() throws ModuleException {
      LargeObject largeObject = null;
      long ret = 0;
      try {
        LargeObjectManager largeObjectManager = ((PGConnection) connection).getLargeObjectAPI();
        largeObject = largeObjectManager.open(objectId);
        ret = largeObject.size64();
      } catch (SQLException e) {
        throw new ModuleException().withMessage("Could not get blob size").withCause(e);
      } finally {
        if (largeObject != null) {
          try {
            largeObject.close();
          } catch (SQLException e) {
            LOGGER.debug("Could not close large object", e);
          }
        }
      }
      return ret;
    }
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    ModuleException moduleException = PostgreSQLExceptionNormalizer.getInstance().normalizeException(exception,
      contextMessage);

    // in case the exception normalizer could not handle this exception
    if (moduleException == null) {
      moduleException = super.normalizeException(exception, contextMessage);
    }

    return moduleException;
  }

  @Override
  protected List<CheckConstraint> getCheckConstraints(String schemaName, String tableName) throws ModuleException {
    List<CheckConstraint> checkConstraints = super.getCheckConstraints(schemaName, tableName);

    try (PreparedStatement ps = getConnection().prepareStatement(
      "SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conname = ? AND conrelid = ?::regclass")) {
      for (CheckConstraint checkConstraint : checkConstraints) {
        ps.setString(1, checkConstraint.getName());
        ps.setString(2, sqlHelper.escapeSchemaName(schemaName) + '.' + sqlHelper.escapeTableName(tableName));

        try (ResultSet rs = ps.execute() ? ps.getResultSet() : null) {
          if (rs != null && rs.next()) {
            checkConstraint.setDescription(rs.getString(1));
          }
        } catch (SQLException e) {
          reporter.failedToGetDescription(e, "constraint", schemaName, tableName, checkConstraint.getName());
        }
      }
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "constraints", schemaName, tableName);
    }

    return checkConstraints;
  }

  @Override
  protected List<ForeignKey> getForeignKeys(String schemaName, String tableName) throws SQLException, ModuleException {
    List<ForeignKey> foreignKeys = super.getForeignKeys(schemaName, tableName);

    try (PreparedStatement ps = getConnection().prepareStatement(
      "SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conname = ? AND conrelid = ?::regclass")) {
      for (ForeignKey foreignKey : foreignKeys) {
        ps.setString(1, foreignKey.getName());
        ps.setString(2, sqlHelper.escapeSchemaName(schemaName) + '.' + sqlHelper.escapeTableName(tableName));

        try (ResultSet rs = ps.execute() ? ps.getResultSet() : null) {
          if (rs != null && rs.next()) {
            foreignKey.setDescription(rs.getString(1));
          }
        } catch (SQLException e) {
          reporter.failedToGetDescription(e, "table foreign key", schemaName, tableName, foreignKey.getName());
        }
      }
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "table foreign keys", schemaName, tableName);
    }

    return foreignKeys;
  }

  @Override
  protected List<RoutineStructure> getRoutines(String schemaName) throws SQLException, ModuleException {
    List<RoutineStructure> routines = super.getRoutines(schemaName);

    try (PreparedStatement ps = getConnection().prepareStatement(
      "SELECT d.description FROM pg_proc p INNER JOIN pg_namespace n ON n.oid = p.pronamespace LEFT JOIN pg_description As d ON (d.objoid = p.oid ) WHERE n.nspname = ? and p.proname = ? LIMIT 1")) {
      for (RoutineStructure routine : routines) {
        ps.setString(1, sqlHelper.escapeSchemaName(schemaName));
        ps.setString(2, routine.getName());

        try (ResultSet rs = ps.execute() ? ps.getResultSet() : null) {
          if (rs != null && rs.next()) {
            routine.setDescription(rs.getString(1));
          }
        } catch (SQLException e) {
          reporter.failedToGetDescription(e, "routine", schemaName, routine.getName());
        }
      }
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "routines", schemaName);
    }

    return routines;
  }

  @Override
  protected List<RoleStructure> getRoles() throws SQLException, ModuleException {
    List<RoleStructure> roles = super.getRoles();

    try (PreparedStatement ps = getConnection().prepareStatement(
      "SELECT pg_catalog.shobj_description(r.oid, 'pg_authid') AS description FROM pg_catalog.pg_roles r where r.rolname = ? LIMIT 1")) {
      for (RoleStructure role : roles) {
        ps.setString(1, role.getName());

        try (ResultSet rs = ps.execute() ? ps.getResultSet() : null) {
          if (rs != null && rs.next()) {
            role.setDescription(rs.getString(1));
          }
        } catch (SQLException e) {
          reporter.failedToGetDescription(e, "role", role.getName());
        }
      }
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "roles");
    }

    return roles;
  }

  @Override
  protected List<SchemaStructure> getSchemas() throws SQLException, ModuleException {
    List<SchemaStructure> schemas = super.getSchemas();

    try (PreparedStatement ps = getConnection().prepareStatement(
      "SELECT pg_catalog.obj_description(n.oid, 'pg_namespace') FROM pg_catalog.pg_namespace n WHERE n.nspname = ? LIMIT 1")) {
      for (SchemaStructure schema : schemas) {
        ps.setString(1, sqlHelper.escapeSchemaName(schema.getName()));

        try (ResultSet rs = ps.execute() ? ps.getResultSet() : null) {
          if (rs != null && rs.next()) {
            schema.setDescription(rs.getString(1));
          }
        } catch (SQLException e) {
          reporter.failedToGetDescription(e, "schema", schema.getName());
        }
      }
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "schemas");
    }

    return schemas;
  }

  @Override
  protected String getDatabaseDescription(String name) throws ModuleException {
    String description = null;

    try (PreparedStatement ps = getConnection().prepareStatement(
      "SELECT pg_catalog.shobj_description(oid, 'pg_database') FROM pg_catalog.pg_database WHERE datname = ? LIMIT 1")) {
      ps.setString(1, name);
      try (ResultSet rs = ps.execute() ? ps.getResultSet() : null) {
        if (rs != null && rs.next()) {
          description = rs.getString(1);
        }
      }
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "database", name);
    }

    return description;
  }

  @Override
  protected List<Trigger> getTriggers(String schemaName, String tableName) throws ModuleException {
    List<Trigger> triggers = super.getTriggers(schemaName, tableName);

    try (PreparedStatement ps = getConnection().prepareStatement(
      "select d.description from pg_description as d inner join pg_trigger t on t.oid = d.objoid where tgname = ?")) {
      for (Trigger trigger : triggers) {
        ps.setString(1, trigger.getName());

        try (ResultSet rs = ps.execute() ? ps.getResultSet() : null) {
          if (rs != null && rs.next()) {
            trigger.setDescription(rs.getString(1));
          }
        } catch (SQLException e) {
          reporter.failedToGetDescription(e, "trigger", schemaName, tableName, trigger.getName());
        }
      }
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "triggers", schemaName, tableName);
    }

    return triggers;
  }
}
