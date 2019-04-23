package com.databasepreservation.modules.in;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.OpenEdgeHelper;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class OpenEdgeJDBCImportModule extends JDBCImportModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenEdgeJDBCImportModule.class);

  /**
   * Create a new Progress OpenEdge import module using the default instance.
   *
   * @param database
   *          the name of the database we'll be accessing
   * @param username
   *          the name of the user to use in the connection
   * @param password
   *          the password of the user to use in the connection
   */
  public OpenEdgeJDBCImportModule(String database, String username, String password, String hostname) {
    super("com.ddtek.jdbc.openedge.OpenEdgeDriver",
        "jdbc:datadirect:openedge://" + hostname + ";user=" + username + ";password=" + password + ";DatabaseName=" + database,
        new OpenEdgeHelper(), new OpenEdgeDataTypeImporter());
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
  public OpenEdgeJDBCImportModule(int port, String database, String username, String password, String hostname) {
    super("com.ddtek.jdbc.openedge.OpenEdgeDriver",
        "jdbc:datadirect:openedge://" + hostname + ":" + port + ";user=" + username + ";password=" + password + ";DatabaseName=" + database,
        new OpenEdgeHelper(), new OpenEdgeDataTypeImporter());
  }

  @Override
  protected Set<String> getIgnoredImportedSchemas() {
    Set<String> ignored = new HashSet<String>();

    ignored.add("SYSPROGRESS");

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

  private String getDescriptionForTable(String schemaName, String tableName) throws ModuleException {
    try {
      return getDescriptionTable(schemaName, tableName);
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "table", schemaName, tableName);
    }
    return null;
  }

  private String getDescriptionForColumn(String schemaName, String tableName, String columnName) throws ModuleException {
    try {
      return getDescriptionColumn(schemaName, tableName, columnName);
    } catch (SQLException e) {
      reporter.failedToGetDescription(e, "column", schemaName, tableName, columnName);
    }
    return null;
  }

  private String getDescriptionTable(String schemaName, String tableName) throws ModuleException, SQLException {

    String description = null;

    String query = ((OpenEdgeHelper) sqlHelper).getTableDescription(schemaName, tableName);

    try (ResultSet res = getStatement().executeQuery(query)) {
      while(res.next()) {
        description = res.getString("DESCRIPTION");
      }
    } catch (SQLException e) {
      LOGGER.error("Error getting description for " + tableName, e);
    }

    return description;
  }

  private String getDescriptionColumn(String schemaName, String tableName, String columnName) throws ModuleException, SQLException {

    String description = null;

    String query = ((OpenEdgeHelper) sqlHelper).getColumnDescription(schemaName, tableName, columnName);

    try (ResultSet res = getStatement().executeQuery(query)) {
      while(res.next()) {
        description = res.getString("DESCRIPTION");
      }
    } catch (SQLException e) {
      LOGGER.error("Error getting description for " + tableName, e);
    }

    return description;
  }


  @Override
  protected TableStructure getTableStructure(SchemaStructure schema, String tableName, int tableIndex,
                                             String description) throws SQLException, ModuleException {
    TableStructure tableStructure = super.getTableStructure(schema, tableName, tableIndex, description);
    tableStructure.setDescription(getDescriptionForTable(schema.getName(), tableName));
    return tableStructure;
  }

  @Override
  protected List<ColumnStructure> getColumns(String schemaName, String tableName) throws SQLException, ModuleException {
    List<ColumnStructure> columns = super.getColumns(schemaName, tableName);
    for (ColumnStructure column : columns) {
      column.setDescription(getDescriptionForColumn(schemaName, tableName, column.getName()));
    }
    return columns;
  }
}
