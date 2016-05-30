package com.databasepreservation.modules.msAccess.in;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCDatatypeImporter;
import com.databasepreservation.modules.msAccess.MsAccessHelper;
import com.databasepreservation.modules.odbc.in.ODBCImportModule;

/**
 * @author Luis Faria
 */
public class MsAccessImportModule extends ODBCImportModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(MsAccessImportModule.class);
  private final DateFormat accessDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

  /**
   * Create a new Microsoft Access import module
   *
   * @param msAccessFile
   */
  public MsAccessImportModule(File msAccessFile) {
    super("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + msAccessFile.getAbsolutePath(),
      new MsAccessHelper(), new JDBCDatatypeImporter());
  }

  @Override
  public Connection getConnection() throws SQLException, ClassNotFoundException {
    if (connection == null) {
      LOGGER.debug("Loading JDBC Driver " + driverClassName);
      Class.forName(driverClassName);
      LOGGER.debug("Getting connection");
      connection = DriverManager.getConnection(connectionURL, "admin", "");
      LOGGER.debug("Connected");
    }
    return connection;
  }

  @Override
  protected PrimaryKey getPrimaryKey(String schemaName, String tableName) throws SQLException, ClassNotFoundException {
    String key_colname = null;

    // get the primary key information
    ResultSet rset = getMetadata().getIndexInfo(null, null, tableName, true, true);
    while (rset.next()) {
      String idx = rset.getString(6);
      if (idx != null) {
        // Note: index "PrimaryKey" is Access DB specific
        // other db server has diff. index syntax.
        if ("PrimaryKey".equalsIgnoreCase(idx)) {
          key_colname = rset.getString(9);
        }
      }
    }

    // TODO add name & description
    PrimaryKey pk = new PrimaryKey();
    pk.setColumnNames(Arrays.asList(new String[] {key_colname}));
    return pk;
  }

  @Override
  protected Statement getStatement() throws SQLException, ClassNotFoundException {
    if (statement == null) {
      statement = getConnection().createStatement();
    }
    return statement;
  }

  @Override
  protected List<ForeignKey> getForeignKeys(String schemaName, String tableName) throws SQLException,
    ClassNotFoundException {
    List<ForeignKey> fKeys = new ArrayList<ForeignKey>();

    ResultSet foreignKeys = getStatement().executeQuery(
      "SELECT  szRelationship, szReferencedObject, szColumn, szReferencedColumn FROM MSysRelationships WHERE szObject like '"
        + tableName + "'");

    while (foreignKeys.next()) {
      // FK name
      String id = foreignKeys.getString(1);
      // if FK has no name, use foreign table name instead
      if (id == null) {
        id = foreignKeys.getString(2);
      }

      // local column
      String name = foreignKeys.getString(3);

      // foreign table
      String refTable = foreignKeys.getString(2);
      // foreign column
      String refColumn = foreignKeys.getString(4);
      fKeys.add(new ForeignKey(tableName + "." + id, name, refTable, refColumn));
    }

    return fKeys;
  }

  @Override
  protected Cell convertRawToCell(String tableName, String columnName, int columnIndex, long rowIndex, Type cellType,
    ResultSet rawData) throws SQLException, InvalidDataException, ClassNotFoundException, ModuleException {
    Cell cell;
    String id = tableName + "." + columnName + "." + rowIndex;
    if (cellType instanceof SimpleTypeDateTime) {
      String dateString = rawData.getString(columnName);
      if (dateString == null) {
        cell = new NullCell(id);
      } else {
        try {
          DateTime isoDateTime = new DateTime(accessDateFormat.parse(dateString));
          cell = new SimpleCell(id, isoDateTime.toString());
        } catch (ParseException e) {
          throw new InvalidDataException("Invalid date found in cell " + id + ": " + dateString);
        }
      }
    } else {
      cell = super.convertRawToCell(tableName, columnName, columnIndex, rowIndex, cellType, rawData);
    }
    return cell;
  }

}
