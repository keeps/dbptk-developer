/**
 *
 */
package com.databasepreservation.modules;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.ComposedTypeArray;
import com.databasepreservation.model.structure.type.ComposedTypeStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeEnumeration;
import com.databasepreservation.model.structure.type.SimpleTypeInterval;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeNumericExact;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;

/**
 * @author Luis Faria <lfaria@keep.pt>
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SQLHelper {

  private String name = "";

  private String startQuote = "";

  private String endQuote = "";

  private String separatorSchemaTable = ".";

  public String getName() {
    return name;
  }

  public String getStartQuote() {
    return startQuote;
  }

  public String getEndQuote() {
    return endQuote;
  }

  public String getSeparatorSchemaTable() {
    return separatorSchemaTable;
  }

  /**
   * SQL to get all rows from a table
   *
   * @param tableId
   *          the table ID
   * @return the SQL
   * @throws ModuleException
   */
  public String selectTableSQL(String tableId) throws ModuleException {
    return "SELECT * FROM " + escapeTableId(tableId);
  }

  /**
   * SQL to create the database
   *
   * @param dbName
   *          the database name
   * @return the SQL
   */
  public String createDatabaseSQL(String dbName) {
    return "CREATE DATABASE " + escapeDatabaseName(dbName);
  }

  /**
   * SQL to create a schema
   *
   * @param schema
   *          the schema structure
   * @return the SQL
   */
  public String createSchemaSQL(SchemaStructure schema) {
    return "CREATE SCHEMA " + escapeSchemaName(schema.getName());
  }

  /**
   * SQL to create a table from table structure
   *
   * @param table
   *          the table structure
   * @return the SQL
   * @throws UnknownTypeException
   */
  public String createTableSQL(TableStructure table) throws UnknownTypeException, ModuleException {
    return new StringBuilder().append("CREATE TABLE ").append(escapeTableId(table.getId())).append(" (")
      .append(createColumnsSQL(table.getColumns(), table.getPrimaryKey(), table.getForeignKeys())).append(")")
      .toString();
  }

  protected String createColumnsSQL(List<ColumnStructure> columns, PrimaryKey pkey, List<ForeignKey> fkeys)
    throws UnknownTypeException {
    String ret = "";
    int index = 0;
    for (ColumnStructure column : columns) {
      boolean isPkey = pkey != null && pkey.getColumnNames().contains(column.getName());
      boolean isFkey = false;
      for (ForeignKey fkey : fkeys) {
        isFkey |= fkey.getName().equals(column.getName());
      }
      String columnTypeSQL = createColumnSQL(column, isPkey, isFkey);
      ret += (index > 0 ? ", " : "") + columnTypeSQL;

      index++;
    }
    return ret;
  }

  protected String createColumnSQL(ColumnStructure column, boolean isPrimaryKey, boolean isForeignKey)
    throws UnknownTypeException {
    String sqlType = createTypeSQL(column.getType(), isPrimaryKey, isForeignKey);

    if (sqlType.equalsIgnoreCase(column.getType().getOriginalTypeName())) {
      Reporter.dataTypeChangedOnExport(this.getClass().getName(), column, sqlType);
    }

    StringBuilder result = new StringBuilder().append(escapeColumnName(column.getName())).append(" ").append(sqlType);

    if (column.isNillable() != null && !column.isNillable()) {
      result.append(" NOT");
    }

    result.append(" NULL");

    return result.toString();
  }

  /**
   * Convert a column type (from model) to the database type. This method should
   * be overridden for each specific DBMS export module implementation to use
   * the specific data types.
   *
   * @param type
   *          the column type
   * @return the string representation of the database type
   * @throws UnknownTypeException
   */
  protected String createTypeSQL(Type type, boolean isPrimaryKey, boolean isForeignKey) throws UnknownTypeException {
    String ret;
    if (type instanceof SimpleTypeString) {
      SimpleTypeString string = (SimpleTypeString) type;
      if (string.isLengthVariable()) {
        ret = "varchar(" + string.getLength() + ")";
      } else {
        ret = "char(" + string.getLength() + ")";
      }
    } else if (type instanceof SimpleTypeNumericExact) {
      if ("INTEGER".equalsIgnoreCase(type.getSql99TypeName())) {
        ret = "integer";
      } else if ("SMALLINT".equalsIgnoreCase(type.getSql99TypeName())) {
        ret = "smallint";
      } else if ("DECIMAL".equalsIgnoreCase(type.getSql99TypeName())) {
        ret = "decimal";
        if (getNumericExactPrecision(type, 30) > 0) {
          ret += "(" + getNumericExactPrecision(type, 30);
          if (getNumericExactScale(type, 30) > 0) {
            ret += "," + getNumericExactScale(type, 30);
          }
          ret += ")";
        }
      } else {
        ret = "numeric";
        if (getNumericExactPrecision(type, 30) > 0) {
          ret += "(" + getNumericExactPrecision(type, 30);
          if (getNumericExactScale(type, 30) > 0) {
            ret += "," + getNumericExactScale(type, 30);
          }
          ret += ")";
        }
      }
    } else if (type instanceof SimpleTypeNumericApproximate) {
      SimpleTypeNumericApproximate numericApproximate = (SimpleTypeNumericApproximate) type;
      Integer precision = numericApproximate.getPrecision();
      if (precision > 53) {
        precision = 53;
      }
      ret = "float(" + precision + ")";
    } else if (type instanceof SimpleTypeBoolean) {
      ret = "boolean";
    } else if (type instanceof SimpleTypeDateTime) {
      SimpleTypeDateTime dateTime = (SimpleTypeDateTime) type;
      if (!dateTime.getTimeDefined() && !dateTime.getTimeZoneDefined()) {
        ret = "date";
      } else {
        if ("TIME".equalsIgnoreCase(type.getSql99TypeName())) {
          ret = "time";
        } else {
          ret = "timestamp";
        }
      }
    } else if (type instanceof SimpleTypeInterval) {
      throw new UnknownTypeException("Interval type not yet supported");
    } else if (type instanceof SimpleTypeEnumeration) {
      throw new UnknownTypeException("Enumeration type not yet supported");
    } else if (type instanceof SimpleTypeBinary) {
      ret = "blob";
    } else if (type instanceof ComposedTypeArray) {
      throw new UnknownTypeException("Array type not yet supported");
    } else if (type instanceof ComposedTypeStructure) {
      throw new UnknownTypeException("Structure type not yet supported");
    } else {
      throw new UnknownTypeException(type.getClass().getName() + " not yet supported");
    }
    return ret;
  }

  protected Integer getNumericExactPrecision(Type type, Integer max) {
    SimpleTypeNumericExact numericExact = (SimpleTypeNumericExact) type;
    Integer precision;
    if (max == null) {
      precision = numericExact.getPrecision();
    } else {
      precision = Math.min(numericExact.getPrecision(), max);
    }
    return precision;
  }

  protected Integer getNumericExactScale(Type type, Integer max) {
    SimpleTypeNumericExact numericExact = (SimpleTypeNumericExact) type;
    Integer scale;
    if (max == null || numericExact.getScale() == 0) {
      scale = numericExact.getScale();
    } else {
      scale = numericExact.getScale();
      if (scale < 0) {
        scale = 0;
      }
      scale = scale - numericExact.getPrecision() + getNumericExactPrecision(type, max);
    }
    return scale;
  }

  /**
   * SQL to create the primary key, altering the already created table
   *
   * @param tableId
   *          the ID of the table
   * @param pkey
   *          the primary key
   * @return the SQL
   * @throws ModuleException
   */
  public String createPrimaryKeySQL(String tableId, PrimaryKey pkey) throws ModuleException {
    StringBuilder ret = new StringBuilder();
    if (pkey != null) {

      ret.append("ALTER TABLE ").append(escapeTableId(tableId));
      if (StringUtils.isBlank(pkey.getName())) {
        ret.append(" ADD PRIMARY KEY (");
      } else {
        ret.append(" ADD CONSTRAINT ").append(escapePrimaryKeyName(pkey.getName())).append(" PRIMARY KEY (");
      }

      boolean comma = false;
      for (String field : pkey.getColumnNames()) {
        if (comma) {
          ret.append(", ");
        }
        ret.append(escapeColumnName(field));
        comma = true;
      }
      ret.append(")");
    }
    String result = ret.toString();
    return StringUtils.isBlank(result) ? null : result;
  }

  /**
   * SQL to create a foreign key (relation), altering the already created table
   *
   * @param table
   *          the table structure
   * @param fkey
   *          the foreign key
   * @return the SQL
   * @throws ModuleException
   */
  public String createForeignKeySQL(TableStructure table, ForeignKey fkey) throws ModuleException {
    String ret = "ALTER TABLE " + escapeTableId(table.getId()) + " ADD FOREIGN KEY (";

    for (int i = 0; i < fkey.getReferences().size(); i++) {
      if (i > 0) {
        ret += ", ";
      }
      ret += escapeColumnName(fkey.getReferences().get(i).getColumn());
    }

    ret += ") REFERENCES " + escapeSchemaName(fkey.getReferencedSchema()) + "."
      + escapeTableName(fkey.getReferencedTable()) + " (";

    for (int i = 0; i < fkey.getReferences().size(); i++) {
      if (i > 0) {
        ret += ", ";
      }
      ret += escapeColumnName(fkey.getReferences().get(i).getReferenced());
    }

    ret += ")";
    return ret;
  }

  /**
   * SQL to insert row on the table.
   *
   * @param table
   *          the table structure
   * @param row
   *          the row
   * @param cellSQLHandler
   *          handler that will transform the cell in SQL
   * @return the prepared statement SQL
   * @throws ModuleException
   * @throws InvalidDataException
   */
  public byte[] createRowSQL(TableStructure table, Row row, CellSQLHandler cellSQLHandler) throws InvalidDataException,
    ModuleException {
    ByteArrayOutputStream sqlOut = new ByteArrayOutputStream();
    try {
      sqlOut.write(("INSERT INTO " + escapeTableName(table.getName()) + " VALUES (").getBytes());
      int i = 0;
      Iterator<ColumnStructure> columnIt = table.getColumns().iterator();
      for (Cell cell : row.getCells()) {
        ColumnStructure column = columnIt.next();
        if (i++ > 0) {
          sqlOut.write(", ".getBytes());
        }
        sqlOut.write(cellSQLHandler.createCellSQL(cell, column));
      }
      sqlOut.write(")".getBytes());
    } catch (IOException e) {
      throw new ModuleException("Error creating row SQL", e);
    }
    return sqlOut.toByteArray();
  }

  /**
   * Prepared SQL statement to insert row on the table.
   *
   * @param table
   *          the table structure
   * @return the prepared SQL statement
   * @throws ModuleException
   */
  public String createRowSQL(TableStructure table) throws ModuleException {
    String ret = "INSERT INTO " + escapeTableId(table.getId()) + " VALUES (";
    for (int i = 0; i < table.getColumns().size(); i++) {
      if (i > 0) {
        ret += ", ";
      }
      ret += "?";
    }
    ret += ")";

    return ret;
  }

  protected String escapeDatabaseName(String database) {
    return getStartQuote() + database + getEndQuote();
  }

  public String escapeSchemaName(String schema) {
    return getStartQuote() + schema + getEndQuote();
  }

  protected String escapeTableId(String tableId) throws ModuleException {
    String[] parts = splitTableId(tableId);
    String schema = parts[0];
    String table = parts[1];
    return escapeSchemaName(schema) + getSeparatorSchemaTable() + escapeTableName(table);
  }

  protected String getEscapedTableNameFromId(String tableId) throws ModuleException {
    String[] parts = splitTableId(tableId);
    // String schema = parts[0];
    String table = parts[1];
    return escapeTableName(table);
  }

  protected String escapeTableName(String table) {
    return getStartQuote() + table + getEndQuote();
  }

  public String escapeViewName(String viewName) {
    return getStartQuote() + viewName + getEndQuote();
  }

  protected String escapeColumnName(String column) {
    return getStartQuote() + column + getEndQuote();
  }

  protected String escapePrimaryKeyName(String pkey_name) {
    return getStartQuote() + pkey_name + getEndQuote();
  }

  protected String[] splitTableId(String tableId) throws ModuleException {
    String[] parts = tableId.split("\\.");
    if (parts.length < 2) {
      throw new ModuleException("An error ocurred while spliting table id: " + "tableId is malformed");
    }
    return parts;
  }

  /**
   * @param schemaName
   *          The schema name
   * @param tableName
   *          The table name
   * @return the SQL to get check constraints
   */
  public String getCheckConstraintsSQL(String schemaName, String tableName) {
    return null;
  }

  /**
   * @param schemaName
   *          The schema name
   * @param tableName
   *          The table name
   * @return the SQL to get table triggers
   */
  public String getTriggersSQL(String schemaName, String tableName) {
    return null;
  }

  /**
   * @param schemaName
   *          The schema name
   * @param tableName
   *          The table name
   * @return the SQL to get table triggers
   */
  public String getRowsSQL(String schemaName, String tableName) {
    StringBuilder sb = new StringBuilder("SELECT COUNT(*) FROM ").append(escapeSchemaName(schemaName))
      .append(getSeparatorSchemaTable()).append(escapeTableName(tableName));
    return sb.toString();
  }

  /**
   * @param dbName
   *          the database name
   * @return the SQL to get all users
   */
  public String getUsersSQL(String dbName) {
    return null;
  }

  /**
   * @return the SQL to get all roles
   */
  public String getRolesSQL() {
    return null;
  }

  /**
   * @param database
   *          the database name required for some DBMSs
   * @return the SQL to get all databases
   */
  public String getDatabases(String database) {
    return null;
  }

  /**
   * @param database
   *          the database name
   * @return the SQL to drop the database
   */
  public String dropDatabase(String database) {
    return null;
  }

  /**
   * Interface of handlers that will transform a cell in SQL
   */
  public interface CellSQLHandler {
    /**
     * Transform a cell in SQL
     *
     * @param cell
     *          the cell to transform
     * @param column
     *          the column to which this cell belongs to
     * @return the SQL in a byte array. The byte array is to allow having binary
     *         directly on the SQL.
     * @throws InvalidDataException
     * @throws ModuleException
     */
    public byte[] createCellSQL(Cell cell, ColumnStructure column) throws InvalidDataException, ModuleException;
  }
}
