/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.oracle.in;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import oracle.sql.STRUCT;
import org.geotools.data.oracle.sdo.GeometryConverter;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.gml2.GMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.oracle.OracleExceptionManager;
import com.databasepreservation.modules.oracle.OracleHelper;

import oracle.jdbc.OracleResultSet;

/**
 * Microsoft SQL Server JDBC import module.
 *
 * @author Luis Faria <lfaria@keep.pt>
 */
public class Oracle12cJDBCImportModule extends JDBCImportModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(Oracle12cJDBCImportModule.class);

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
  public Oracle12cJDBCImportModule(String serverName, int port, String instance, String username, String password) {

    super("oracle.jdbc.driver.OracleDriver",
      "jdbc:oracle:thin:" + username + "/" + password + "@//" + serverName + ":" + port + "/" + instance,
      new OracleHelper(), new Oracle12cJDBCDatatypeImporter());

    LOGGER.debug("jdbc:oracle:thin:<username>/<password>@//" + serverName + ":" + port + "/" + instance);
  }

  @Override
  protected Statement getStatement() throws SQLException, ModuleException {
    if (statement == null) {
      statement = getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }
    return statement;
  }

  @Override
  protected String getDatabaseName() throws SQLException, ModuleException {
    return getMetadata().getUserName();
  }

  @Override
  protected List<SchemaStructure> getSchemas() throws SQLException, ModuleException {
    List<SchemaStructure> schemas = new ArrayList<SchemaStructure>();
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
    ModuleException moduleException = OracleExceptionManager.getInstance().normalizeException(exception,
      contextMessage);

    // in case the exception normalizer could not handle this exception
    if (moduleException == null) {
      moduleException = super.normalizeException(exception, contextMessage);
    }

    return moduleException;
  }

  @Override
  protected TableStructure getTableStructure(SchemaStructure schema, String tableName, int tableIndex,
    String description) throws SQLException, ModuleException {
    TableStructure tableStructure = super.getTableStructure(schema, tableName, tableIndex, description);

    try (PreparedStatement statement = getConnection()
      .prepareStatement("SELECT COMMENTS FROM ALL_TAB_COMMENTS WHERE OWNER = ? AND TABLE_NAME = ?")) {

      statement.setString(1, schema.getName());
      statement.setString(2, tableName);
      statement.execute();

      try (ResultSet rs = statement.getResultSet()) {
        if (rs.next()) {
          tableStructure.setDescription(rs.getString(1));
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

        try (ResultSet rs = statement.getResultSet()) {
          if (rs.next()) {
            column.setDescription(rs.getString(1));
          }
        } catch (SQLException e) {
          reporter.failedToGetDescription(e, "column", schemaName, tableName, column.getName());
        }
      }
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "columns", schemaName, tableName);
    }

    return columns;
  }

  @Override
  protected Cell convertRawToCell(String tableName, String columnName, int columnIndex, long rowIndex, Type cellType,
    ResultSet rawData) throws SQLException, ModuleException {
    if ("SDO_GEOMETRY".equalsIgnoreCase(cellType.getOriginalTypeName())) {
      String id = tableName + "." + columnName + "." + rowIndex;

      GeometryConverter geometryConverter = new GeometryConverter(null);
      STRUCT asStruct = ((OracleResultSet) rawData).getSTRUCT(columnName);
      Geometry geometry = geometryConverter.asGeometry(asStruct);

      GMLWriter gmlWriter = new GMLWriter();

      return new SimpleCell(id, gmlWriter.write(geometry));

    } else {
      return super.convertRawToCell(tableName, columnName, columnIndex, rowIndex, cellType, rawData);
    }
  }
}
