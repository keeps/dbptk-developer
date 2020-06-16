/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.oracle.in;

import java.io.InputStream;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.modules.CloseableUtils;
import com.databasepreservation.utils.RemoteConnectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.geotools.data.oracle.sdo.GeometryConverter;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.gml2.GMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.common.io.providers.InputStreamProviderImpl;
import com.databasepreservation.model.data.ArrayCell;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.RoutineStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.oracle.Oracle12cModuleFactory;
import com.databasepreservation.modules.oracle.OracleExceptionNormalizer;
import com.databasepreservation.modules.oracle.OracleHelper;
import com.databasepreservation.utils.ConfigUtils;
import com.databasepreservation.utils.MapUtils;

import oracle.jdbc.OracleArray;
import oracle.jdbc.OracleBfile;
import oracle.jdbc.OracleResultSet;
import oracle.jdbc.OracleStatement;
import oracle.sql.STRUCT;

/**
 * Microsoft SQL Server JDBC import module.
 *
 * @author Luis Faria <lfaria@keep.pt>
 */
public class Oracle12cJDBCImportModule extends JDBCImportModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(Oracle12cJDBCImportModule.class);

  private static final Integer DEFAULT_LOB_PREFETCH_SIZE = ConfigUtils.getProperty(4000,
    "dbptk.jdbc.oracle.lobPrefetchSize");

  /**
   * Create a new Oracle12c import module
   *
   * @param serverName
   *          the name (host name) of the server
   * @param instance
   *          the name of the instance we'll be accessing
   * @param username
   *          the name of the user to use in the connection
   * @param password
   *          the password of the user to use in the connection
   */
  public Oracle12cJDBCImportModule(String moduleName, String serverName, int port, String instance, String username,
    String password) {

    super("oracle.jdbc.driver.OracleDriver",
      "jdbc:oracle:thin:" + username + "/" + password + "@//" + serverName + ":" + port + "/" + instance,
      new OracleHelper(), new Oracle12cJDBCDatatypeImporter(), moduleName,
      MapUtils.buildMapFromObjects(Oracle12cModuleFactory.PARAMETER_SERVER_NAME, serverName,
        Oracle12cModuleFactory.PARAMETER_PORT_NUMBER, port, Oracle12cModuleFactory.PARAMETER_INSTANCE, instance,
        Oracle12cModuleFactory.PARAMETER_USERNAME, username, Oracle12cModuleFactory.PARAMETER_PASSWORD, password,
        Oracle12cModuleFactory.PARAMETER_ACCEPT_LICENSE, true));

    LOGGER.debug("jdbc:oracle:thin:<username>/<password>@//{}:{}/{}", serverName, port, instance);
  }

  public Oracle12cJDBCImportModule(String moduleName, String serverName, int port, String instance, String username,
    String password, String sshHost, String sshUser, String sshPassword, String sshPortNumber) throws ModuleException {

    super("oracle.jdbc.driver.OracleDriver",
      "jdbc:oracle:thin:" + username + "/" + password + "@//" + serverName + ":" + port + "/" + instance,
      new OracleHelper(), new Oracle12cJDBCDatatypeImporter(), moduleName,
      MapUtils.buildMapFromObjects(Oracle12cModuleFactory.PARAMETER_SERVER_NAME, serverName,
        Oracle12cModuleFactory.PARAMETER_PORT_NUMBER, port, Oracle12cModuleFactory.PARAMETER_INSTANCE, instance,
        Oracle12cModuleFactory.PARAMETER_USERNAME, username, Oracle12cModuleFactory.PARAMETER_PASSWORD, password,
        Oracle12cModuleFactory.PARAMETER_ACCEPT_LICENSE, true),
      MapUtils.buildMapFromObjects(Oracle12cModuleFactory.PARAMETER_SSH, true,
        Oracle12cModuleFactory.PARAMETER_SSH_HOST, sshHost, Oracle12cModuleFactory.PARAMETER_SSH_PORT, sshPortNumber,
        Oracle12cModuleFactory.PARAMETER_SSH_USER, sshUser, Oracle12cModuleFactory.PARAMETER_SSH_PASSWORD,
        sshPassword));

    LOGGER.debug("jdbc:oracle:thin:<username>/<password>@//{}:{}/{}", serverName, port, instance);
  }

  @Override
  protected Connection createConnection() throws ModuleException {
    Connection connection;
    try {
      if (ssh) {
        connectionURL = RemoteConnectionUtils.replaceHostAndPort(connectionURL);
      }
      connection = DriverManager.getConnection(connectionURL);
      connection.setAutoCommit(false);
      connection.setReadOnly(true);
    } catch (SQLException e) {
      closeConnection();
      throw normalizeException(e, null);
    }
    LOGGER.debug("Connected");
    return connection;
  }

  @Override
  protected Statement getStatement() throws SQLException, ModuleException {
    if (statement == null) {
      statement = getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      if (statement instanceof OracleStatement) {
        ((OracleStatement) statement).setLobPrefetchSize(DEFAULT_LOB_PREFETCH_SIZE);
      }
    }

    return statement;
  }

  @Override
  protected String getDatabaseName() throws SQLException, ModuleException {
    return getMetadata().getUserName();
  }

  @Override
  protected List<SchemaStructure> getSchemas() throws SQLException, ModuleException {
    List<SchemaStructure> schemas = new ArrayList<>();
    String schemaName = getMetadata().getUserName();
    schemas.add(getSchemaStructure(schemaName, 1));
    return schemas;
  }

  @Override
  protected String processActionTime(String string) {
    String[] parts = string.split("\\s+");
    String res = parts[0];
    if ("INSTEAD".equalsIgnoreCase(res)) {
      res += " OF";
    }
    return res;
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    ModuleException moduleException = OracleExceptionNormalizer.getInstance().normalizeException(exception,
      contextMessage);

    // in case the exception normalizer could not handle this exception
    if (moduleException == null) {
      moduleException = super.normalizeException(exception, contextMessage);
    }

    return moduleException;
  }

  @Override
  protected TableStructure getTableStructure(SchemaStructure schema, String tableName, int tableIndex,
    String description, boolean view) throws SQLException, ModuleException {
    TableStructure tableStructure = super.getTableStructure(schema, tableName, tableIndex, description, view);

    try (PreparedStatement statement = getConnection()
      .prepareStatement("SELECT COMMENTS FROM ALL_TAB_COMMENTS WHERE OWNER = ? AND TABLE_NAME = ?")) {

      statement.setString(1, schema.getName());
      statement.setString(2, tableName);
      statement.execute();

      try (ResultSet rs = statement.getResultSet()) {
        if (rs.next()) {
          String comment = rs.getString(1);
          if (StringUtils.isBlank(comment)) {
            tableStructure.setDescription("");
          } else {
            tableStructure.setDescription(comment);
          }
        }
      }
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "table", schema.getName(), tableName);
    }

    return tableStructure;
  }

  @Override
  protected List<ColumnStructure> getColumns(String schemaName, String tableName) throws SQLException, ModuleException {
    List<ColumnStructure> columns = super.getColumns(schemaName, tableName);

    try (PreparedStatement statement = getConnection().prepareStatement(
      "SELECT COMMENTS FROM ALL_COL_COMMENTS WHERE OWNER = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?")) {

      for (ColumnStructure column : columns) {
        statement.setString(1, schemaName);
        statement.setString(2, tableName);
        statement.setString(3, column.getName());
        statement.execute();

        getColumnDescription(schemaName, tableName, statement, column);
      }
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "columns", schemaName, tableName);
    }

    return columns;
  }

  private void getColumnDescription(String schemaName, String tableName, PreparedStatement statement,
    ColumnStructure column) {
    try (ResultSet rs = statement.getResultSet()) {
      if (rs.next()) {
        column.setDescription(rs.getString(1));
      }
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "column", schemaName, tableName, column.getName());
    }
  }

  @Override
  protected Cell rawToCellSimpleTypeBinary(String id, String columnName, Type cellType, ResultSet rawData)
    throws SQLException {
    Cell cell;

    if (cellType instanceof SimpleTypeBinary && ((SimpleTypeBinary) cellType).isOutsideDatabase()) {
      cell = new SimpleCell(id, rawData.getString(columnName));
    } else if (cellType.getOriginalTypeName().equals("BFILE")) {
      if (rawData instanceof OracleResultSet) {
        final OracleBfile bfile = ((OracleResultSet) rawData).getBfile(columnName);
        if (bfile == null) {
          cell = new NullCell(id);
        } else {
          bfile.openFile();
          final InputStream binaryStream = bfile.getBinaryStream();
          cell = new BinaryCell(id, new InputStreamProviderImpl(binaryStream));
          bfile.closeFile();
        }
      } else {
        cell = new NullCell(id);
      }
    } else {
      Blob blob = rawData.getBlob(columnName);
      if (blob != null && !rawData.wasNull()) {
        cell = new BinaryCell(id, blob);
      } else {
        cell = new NullCell(id);
      }
    }
    return cell;
  }

  @Override
  protected Cell convertRawToCell(String tableName, String columnName, int columnIndex, long rowIndex, Type cellType,
    ResultSet rawData) throws SQLException, ModuleException {
    if ("SDO_GEOMETRY".equalsIgnoreCase(cellType.getOriginalTypeName())) {
      String id = tableName + "." + columnName + "." + rowIndex;

      GeometryConverter geometryConverter = new GeometryConverter(null);
      STRUCT asStruct = ((OracleResultSet) rawData).getSTRUCT(columnName);

      try {
        Geometry geometry = geometryConverter.asGeometry(asStruct);
        GMLWriter gmlWriter = new GMLWriter();
        if (geometry == null) {
          return new NullCell(id);
        }
        return new SimpleCell(id, gmlWriter.write(geometry));
      } catch (Exception e) {
        throw normalizeException(e, "Could not convert SDO_GEOMETRY to GML");
      }
    } else {
      return super.convertRawToCell(tableName, columnName, columnIndex, rowIndex, cellType, rawData);
    }
  }

  @Override
  protected Cell parseArray(String baseId, Array array) throws SQLException, InvalidDataException {
    if (array == null) {
      return new NullCell(baseId);
    }

    ArrayCell cell = new ArrayCell(baseId);

    if (array instanceof OracleArray) {
      int counter = 1;
      try (ResultSet arrayData = array.getResultSet()) {
        while (arrayData.next()) {
          SimpleCell subCell = new SimpleCell(baseId + "." + counter, arrayData.getString(2));
          cell.put(subCell, counter);
          counter++;
        }
      }
      return cell;
    } else {
      throw new InvalidDataException("unexpected array instance type");
    }
  }

  @Override
  protected List<RoutineStructure> getRoutines(String schemaName) throws ModuleException {
    List<RoutineStructure> routines = new ArrayList<>();

    try (ResultSet resultSet = getStatement()
      .executeQuery("SELECT UNIQUE name FROM user_source WHERE type='PROCEDURE' or type='FUNCTION'")) {
      List<String> routineNames = new ArrayList<>();
      while (resultSet.next()) {
        routineNames.add(resultSet.getString(1));
      }

      for (String routineName : routineNames) {
        RoutineStructure routine = new RoutineStructure();
        LOGGER.info("Obtaining routine {}", routineName);
        routine.setName(routineName);

        getRoutineBody(routineName, routine);
        routines.add(routine);
      }
    } catch (SQLException e) {
      LOGGER.debug("Could not retrieve routines.", e);
    }
    return routines;
  }

  private void getRoutineBody(String routineName, RoutineStructure routine) throws ModuleException {
    try (ResultSet rsetCode = getStatement()
      .executeQuery("SELECT text FROM user_source WHERE name='" + routineName + "' ORDER BY line")) {
      StringBuilder sb = new StringBuilder();
      while (rsetCode.next()) {
        sb.append(rsetCode.getString("TEXT"));
      }
      routine.setBody(sb.toString());
    } catch (SQLException e) {
      LOGGER.debug("Could not retrieve routine code (as routine).", e);
    }
  }

  @Override
  protected List<ViewStructure> getViews(String schemaName) throws SQLException, ModuleException {
    final List<ViewStructure> views = super.getViews(schemaName);

    for (ViewStructure view : views) {

      String queryForViewSQL = ((OracleHelper) sqlHelper).getViewSQL(view.getName(), schemaName);
      ResultSet rset = null;
      PreparedStatement statement = getConnection().prepareStatement(queryForViewSQL);
      try {
        rset = statement.executeQuery();
        StringBuilder b = new StringBuilder();
        while (rset.next()) {
          b.append(rset.getString("TEXT"));
        }

        view.setQueryOriginal(b.toString());
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
  public String escapeObjectName(String objectName) {
    return "\"" + objectName + "\"";
  }
}
