package com.databasepreservation.modules.msAccess.in;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.PrivilegeStructure;
import com.databasepreservation.model.structure.RoutineStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.msAccess.MsAccessHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 * @author Luis Faria <lfaria@keep.pt>
 */
public class MsAccessUCanAccessImportModule extends JDBCImportModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(MsAccessUCanAccessImportModule.class);

  public MsAccessUCanAccessImportModule(File msAccessFile) {
    super("net.ucanaccess.jdbc.UcanaccessDriver", "jdbc:ucanaccess://" + msAccessFile.getAbsolutePath()
      + ";showSchema=true;", new MsAccessHelper(), new MsAccessUCanAccessDatatypeImporter());
  }

  public MsAccessUCanAccessImportModule(String accessFilePath) {
    this(new File(accessFilePath));
  }

  @Override
  public Connection getConnection() throws SQLException, ClassNotFoundException {
    if (connection == null) {
      LOGGER.debug("Loading JDBC Driver " + driverClassName);
      Class.forName(driverClassName);
      LOGGER.debug("Getting connection");
      connection = DriverManager.getConnection(connectionURL); // , "admin",
                                                               // "admin");
      LOGGER.debug("Connected");
    }
    return connection;
  }

  @Override
  protected ResultSet getTableRawData(TableStructure table) throws SQLException, ClassNotFoundException,
    ModuleException {
    String tableId;
    ResultSet set = null;
    tableId = table.getId();
    LOGGER.debug("query: " + sqlHelper.selectTableSQL(tableId));
    set = getStatement().executeQuery(sqlHelper.selectTableSQL(tableId));
    set.setFetchSize(ROW_FETCH_BLOCK_SIZE);

    return set;
  }

  /**
   * @param schemaName
   * @return
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  @Override
  protected List<RoutineStructure> getRoutines(String schemaName) throws SQLException, ClassNotFoundException {
    // TODO add optional fields to routine (use getProcedureColumns)
    Set<RoutineStructure> routines = new HashSet<RoutineStructure>();

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
    List<RoutineStructure> newRoutines = new ArrayList<RoutineStructure>(routines);
    return newRoutines;
  }

  /**
   * Drops money currency
   */
  @Override
  protected Cell rawToCellSimpleTypeNumericApproximate(String id, String columnName, Type cellType, ResultSet rawData)
    throws SQLException {
    Cell cell = null;
    if ("DOUBLE".equalsIgnoreCase(cellType.getOriginalTypeName())) {
      String data = rawData.getString(columnName);
      if(data != null) {
        String parts[] = data.split("E");
        if (parts.length > 1 && parts[1] != null) {
          LOGGER.warn("Double exponent lost: " + parts[1] + ". From " + data + " -> " + parts[0]);
        }
        cell = new SimpleCell(id, parts[0]);
      }else{
        cell = new NullCell(id);
      }
    } else {
      String value;
      if ("float4".equalsIgnoreCase(cellType.getOriginalTypeName())) {
        Float f = rawData.getFloat(columnName);
        value = f.toString();
      } else {
        Double d = rawData.getDouble(columnName);
        value = d.toString();
      }
      if(rawData.wasNull()){
        cell = new NullCell(id);
      }else {
        cell = new SimpleCell(id, value);
      }
    }
    return cell;
  }

  /**
   * Gets the schemas that won't be imported. Defaults to MsAccess are all
   * INFORMATION_SCHEMA_XX
   *
   * @return the schemas to be ignored at import
   */
  @Override
  protected Set<String> getIgnoredImportedSchemas() {
    Set<String> ignoredSchemas = new HashSet<String>();
    // ignoredSchemas.add("INFORMATION_SCHEMA.*");

    return ignoredSchemas;
  }

  /**
   * @return the database privileges
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  @Override
  protected List<PrivilegeStructure> getPrivileges() throws SQLException, ClassNotFoundException {
    Reporter.notYetSupported("roles importing", "MS Access import module");
    return new ArrayList<PrivilegeStructure>();
  }
}
