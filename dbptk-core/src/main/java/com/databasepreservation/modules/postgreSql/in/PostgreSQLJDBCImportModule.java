/**
 *
 */
package com.databasepreservation.modules.postgreSql.in;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.postgresql.util.PGobject;

import pt.gov.dgarq.roda.common.FileFormat;
import pt.gov.dgarq.roda.common.FormatUtility;

import com.databasepreservation.CustomLogger;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.ComposedCell;
import com.databasepreservation.model.data.FileItem;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.ComposedTypeStructure;
import com.databasepreservation.model.structure.type.ComposedTypeStructure.SubType;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.postgreSql.PostgreSQLHelper;
import com.databasepreservation.modules.siard.SIARDHelper;

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

  private final CustomLogger logger = CustomLogger.getLogger(PostgreSQLJDBCImportModule.class);

  /**
   * Create a new PostgreSQL JDBC import module
   *
   * @param hostname
   *          the name of the PostgreSQL server host (e.g. localhost)
   * @param database
   *          the name of the database to connect to
   * @param username
   *          the name of the user to use in connection
   * @param password
   *          the password of the user to use in connection
   * @param encrypt
   *          encrypt connection
   */
  public PostgreSQLJDBCImportModule(String hostname, String database, String username, String password, boolean encrypt) {
    super("org.postgresql.Driver", "jdbc:postgresql://" + hostname + "/" + database + "?user=" + username
      + "&password=" + password + (encrypt ? "&ssl=true" : ""), new PostgreSQLHelper());
  }

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
  public PostgreSQLJDBCImportModule(String hostname, int port, String database, String username, String password,
    boolean encrypt) {
    super("org.postgresql.Driver", "jdbc:postgresql://" + hostname + ":" + port + "/" + database + "?user=" + username
      + "&password=" + password + (encrypt ? "&ssl=true" : ""), new PostgreSQLHelper());
  }

  @Override
  public Connection getConnection() throws SQLException, ClassNotFoundException {
    if (connection == null) {
      logger.debug("Loading JDBC Driver " + driverClassName);
      Class.forName(driverClassName);
      logger.debug("Getting connection");
      connection = DriverManager.getConnection(connectionURL);
      connection.setAutoCommit(false);
      logger.debug("Connected");

    }
    return connection;
  }

  @Override
  protected Statement getStatement() throws SQLException, ClassNotFoundException {
    if (statement == null) {
      statement = getConnection().createStatement();
    }
    return statement;
  }

  @Override
  protected ResultSet getTableRawData(TableStructure table) throws SQLException, ClassNotFoundException,
    ModuleException {

    // builds a query like "SELECT field1, field2, field3 FROM table"
    HashSet<String> columnNames = new HashSet<>();
    StringBuilder query = new StringBuilder("SELECT ");
    ArrayList<ColumnStructure> udtColumns = new ArrayList<>();
    String separator = "";
    for (ColumnStructure column : table.getColumns()) {
      if (column.getType() instanceof ComposedTypeStructure) {
        udtColumns.add(column);
      }
      columnNames.add(column.getName().toLowerCase());
      query.append(separator).append(column.getName());
      separator = ", ";
    }

    for (ColumnStructure column : udtColumns) {
      query.append(separator).append(
        getFieldNamesFromComposedTypeStructure(column.getId(), (ComposedTypeStructure) column.getType(), table,
          columnNames));
    }

    query.append(" FROM ").append(table.getId());

    logger.debug("query: " + query.toString());

    Statement st = getStatement();
    st.setFetchSize(200);
    ResultSet set = st.executeQuery(query.toString());
    return set;
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
        logger.debug("UDT type hierarchy is too small. columnId: " + columnId + ", " + subtype.toString());
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
        logger.debug("random alias: column name '" + alias + "' exists.");
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
  protected Row convertRawToRow(ResultSet rawData, TableStructure tableStructure) throws InvalidDataException,
    SQLException, ClassNotFoundException, ModuleException {
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
          // handle composed types later
          udtColumnsIndexes.add(i);
          cells.add(null);
        } else {
          cells.add(convertRawToCell(tableStructure.getName(), colStruct.getName(), i + 1, currentRow,
            colStruct.getType(), rawData));
        }
      }

      // handle composed types
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

          udtCells.add(convertRawToCell(tableStructure.getName(), aliasColumnName, udtColumnIndex + 1, currentRow,
            subtype.getType(), rawData));

          i++;
        }

        cells.set(udtColumnIndex, new ComposedCell(tableStructure.getName() + "." + udtColumn.getName() + "."
          + currentRow, udtCells));
      }

      row = new Row(currentRow, cells);
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
      throw new InvalidDataException("table: " + structure.getName() + " row number: " + raw.getRow()
        + " error: different column number from structure. Got " + metadata.getColumnCount() + " columns and expected "
        + structure.getColumns().size() + " (or " + colNumber + " with UDT fields)");
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

  @Override
  protected Type getBinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeBinary(columnSize);
    if ("bytea".equalsIgnoreCase(typeName)) {
      type.setSql99TypeName("BINARY LARGE OBJECT");
      type.setSql2003TypeName("BINARY LARGE OBJECT");
    } else {
      type.setSql99TypeName("BIT");
      type.setSql2003TypeName("BIT");
    }
    return type;
  }

  @Override
  protected Type getDoubleType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    if ("MONEY".equalsIgnoreCase(typeName) || "FLOAT8".equalsIgnoreCase(typeName)) {
      logger.warn("Setting Money column size to 53");
      columnSize = 53;
    }
    Type type = new SimpleTypeNumericApproximate(columnSize);
    type.setSql99TypeName("DOUBLE PRECISION");
    type.setSql2003TypeName("DOUBLE PRECISION");
    return type;
  }

  @Override
  protected Type getTimeType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;
    if ("TIMETZ".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeDateTime(true, true);
      type.setSql99TypeName("TIME WITH TIME ZONE");
      type.setSql2003TypeName("TIME WITH TIME ZONE");
    } else {
      type = new SimpleTypeDateTime(true, false);
      type.setSql99TypeName("TIME");
      type.setSql2003TypeName("TIME");
    }

    return type;
  }

  @Override
  protected Type getTimestampType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;
    if ("TIMESTAMPTZ".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeDateTime(true, true);
      type.setSql99TypeName("TIMESTAMP WITH TIME ZONE");
      type.setSql2003TypeName("TIMESTAMP WITH TIME ZONE");
    } else {
      type = new SimpleTypeDateTime(true, false);
      type.setSql99TypeName("TIMESTAMP");
      type.setSql2003TypeName("TIMESTAMP");
    }

    return type;
  }

  @Override
  protected Type getVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeString(columnSize, true);
    if ("text".equalsIgnoreCase(typeName)) {
      type.setSql99TypeName("CHARACTER LARGE OBJECT");
      type.setSql2003TypeName("CHARACTER LARGE OBJECT");
    } else {
      type.setSql99TypeName("CHARACTER VARYING", columnSize);
      type.setSql2003TypeName("CHARACTER VARYING", columnSize);
    }
    return type;
  }

  @Override
  protected Type getSpecificType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    Type type;
    logger.debug("Specific type name: " + typeName);
    logger.debug("------\n");
    switch (dataType) {
      case 2009: // XML Data type
        type = new SimpleTypeString(columnSize, true);
        type.setSql99TypeName("CHARACTER LARGE OBJECT");
        type.setSql2003TypeName("CHARACTER LARGE OBJECT");
        break;
      default:
        type = super.getSpecificType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
        break;
    }
    return type;
  }

  @Override protected Type getComposedTypeStructure(String schemaName, int dataType, String typeName) {
    Type type = new SimpleTypeString(65535, true);
    type.setSql99TypeName("CHARACTER LARGE OBJECT");
    type.setSql2003TypeName("CHARACTER LARGE OBJECT");
    return type;
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
          logger.warn("Money currency lost: " + parts[1]);
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
   * Treats bit strings, as the default behavior does not handle PostgreSQL byte
   * streams correctly
   */
  @Override
  protected Cell rawToCellSimpleTypeBinary(String id, String columnName, Type cellType, ResultSet rawData)
    throws SQLException, ModuleException {
    Cell cell;
    InputStream binaryStream;
    if ("bit".equalsIgnoreCase(cellType.getOriginalTypeName())) {
      String bitString = rawData.getString(columnName);
      String hexString = new BigInteger(bitString, 2).toString(16);
      if ((hexString.length() % 2) != 0) {
        hexString = "0" + hexString;
      }
      byte[] bytes = SIARDHelper.hexStringToByteArray(hexString);
      binaryStream = new ByteArrayInputStream(bytes);
    } else {
      binaryStream = rawData.getBinaryStream(columnName);
    }
    if (binaryStream != null) {
      FileItem fileItem = new FileItem(binaryStream);
      FileFormat fileFormat = FormatUtility.getFileFormat(fileItem.getFile());
      List<FileFormat> formats = new ArrayList<FileFormat>();
      formats.add(fileFormat);
      cell = new BinaryCell(id, fileItem, formats);
    } else {
      cell = new BinaryCell(id);
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
          logger.trace("rawToCellSimpleTypeDateTime cell: " + (((SimpleCell) cell).getSimpledata()));
        } else if (time_string.matches("^\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{2}:\\d{2}$")) {
          cell = new SimpleCell(id, time_string);
          logger.trace("rawToCellSimpleTypeDateTime cell: " + (((SimpleCell) cell).getSimpledata()));
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
    logger.debug("postgresql, rawToCellComposedTypeStructure, ignored composed type cell");
    return null;
  }

  class PGTime extends PGobject {

    /**
                 *
                 */
    private static final long serialVersionUID = 1L;

  }

}
