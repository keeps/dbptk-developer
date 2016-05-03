/**
 *
 */
package com.databasepreservation.modules.mySql;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.databasepreservation.model.Reporter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.SQLHelper;

/**
 * @author Luis Faria
 */
public class MySQLHelper extends SQLHelper {
  private static final Set<String> MYSQL_TYPES = new HashSet<String>(Arrays.asList("BLOB", "MEDIUMBLOB", "LONGBLOB",
    "TIMESTAMP", "TINYBLOB", "TINYTEXT", "TEXT", "MEDIUMTEXT"));
  private static final Logger logger = LoggerFactory.getLogger(MySQLHelper.class);
  private String name = "MySQL";

  private String startQuote = "`";

  private String endQuote = "`";

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getStartQuote() {
    return startQuote;
  }

  @Override
  public String getEndQuote() {
    return endQuote;
  }

  @Override
  public String createTableSQL(TableStructure table) throws UnknownTypeException, ModuleException {
    return super.createTableSQL(table) + " ENGINE=INNODB";
  }

  @Override
  protected String createColumnSQL(ColumnStructure column, boolean isPrimaryKey, boolean isForeignKey)
    throws UnknownTypeException {
    return super.createColumnSQL(column, isPrimaryKey, isForeignKey)
      + (column.getDescription() != null ? " COMMENT '" + escapeComment(column.getDescription()) + "'" : "");
  }

  @Override
  protected String createTypeSQL(Type type, boolean isPkey, boolean isFkey) throws UnknownTypeException {
    String ret;

    logger.debug("Checking MySQL type " + type.getOriginalTypeName());
    if (MYSQL_TYPES.contains(type.getOriginalTypeName())) {
      // TODO verify if original database is also mysql
      ret = type.getOriginalTypeName();
      logger.debug("Using MySQL original type " + ret);
    } else if (type instanceof SimpleTypeString) {
      SimpleTypeString string = (SimpleTypeString) type;
      if (isPkey) {
        int length = string.getLength().intValue();
        if (length >= 65535) {
          logger.warn("Resizing column length to 333 " + "so it can be a primary key");
          length = 333;
        }
        ret = "varchar(" + length + ")";
      } else if (string.isLengthVariable()) {
        if (string.getLength().intValue() >= 65535) {
          ret = "longtext";
        } else {
          ret = "varchar(" + string.getLength() + ")";
        }
      } else {
        if (string.getLength().intValue() > 255) {
          ret = "text";
        } else {
          ret = "char(" + string.getLength() + ")";
        }
      }
    } else if (type instanceof SimpleTypeNumericApproximate) {
      SimpleTypeNumericApproximate numericApprox = (SimpleTypeNumericApproximate) type;
      if ("REAL".equalsIgnoreCase(type.getSql99TypeName())) {
        ret = "float";
      } else if (StringUtils.startsWithIgnoreCase(type.getSql99TypeName(), "DOUBLE")) {
        ret = "double";
      } else {
        logger.debug("float precision: " + numericApprox.getPrecision());
        ret = "float(" + numericApprox.getPrecision() + ")";
      }
    } else if (type instanceof SimpleTypeBoolean) {
      ret = "bit(1)";
    } else if (type instanceof SimpleTypeDateTime) {
      SimpleTypeDateTime dateTime = (SimpleTypeDateTime) type;
      if (!dateTime.getTimeDefined() && !dateTime.getTimeZoneDefined()) {
        ret = "date";
      } else if ("TIME".equalsIgnoreCase(type.getSql99TypeName())) {
        if (dateTime.getTimeZoneDefined()) {
          logger.warn("Timezone not supported on MySQL: " + "defining type as 'time'");
        }
        ret = "time";
      } else {
        if (dateTime.getTimeZoneDefined()) {
          logger.warn("Timezone not supported on MySQL: " + "defining type as 'datetime'");
        }
        ret = "datetime";
      }
    } else if (type instanceof SimpleTypeBinary) {
      SimpleTypeBinary binary = (SimpleTypeBinary) type;
      Integer length = binary.getLength();
      if (length != null) {
        if ("BIT".equalsIgnoreCase(type.getSql99TypeName())
          || StringUtils.startsWithIgnoreCase(type.getSql99TypeName(), "BIT(")) {
          if ("BIT".equalsIgnoreCase(type.getOriginalTypeName())
            || StringUtils.startsWithIgnoreCase(type.getOriginalTypeName(), "BIT(")) {
            ret = "bit(" + length + ")";
          } else {
            ret = "binary(" + (((length / 8.0) % 1 == 0) ? (length / 8) : ((length / 8) + 1)) + ")";
          }
        } else if ("BIT VARYING".equalsIgnoreCase(type.getSql99TypeName())) {
          ret = "varbinary(" + (((length / 8.0) % 1 == 0) ? (length / 8) : ((length / 8) + 1)) + ")";
        } else {
          ret = "longblob";
        }
      } else {
        ret = "longblob";
      }
    } else {
      ret = super.createTypeSQL(type, isPkey, isFkey);
    }
    return ret;
  }

  /**
   * SQL to create a foreign key (relation), altering the already created table
   *
   * @param table
   *          the table
   * @param fkey
   *          the foreign key
   * @param addConstraint
   *          if an extra constraint info is need at creation of fkey
   * @param plus
   *          a plus factor that makes a foreign key name unique
   * @return the SQL
   * @throws ModuleException
   */
  public String createForeignKeySQL(TableStructure table, ForeignKey fkey, boolean addConstraint, int plus)
    throws ModuleException {

    String foreignRefs = "";
    for (int i = 0; i < fkey.getReferences().size(); i++) {
      if (i > 0) {
        foreignRefs += ", ";
      }
      foreignRefs += escapeColumnName(fkey.getReferences().get(i).getColumn());
      fkey.getReferences().get(i).getColumn();
    }

    String foreignReferenced = "";
    for (int i = 0; i < fkey.getReferences().size(); i++) {
      if (i > 0) {
        foreignReferenced += ", ";
      }
      foreignReferenced += escapeColumnName(fkey.getReferences().get(i).getReferenced());
      fkey.getReferences().get(i).getReferenced();
    }

    String constraint = "";
    if (addConstraint) {
      constraint = " ADD CONSTRAINT `dbpres_" + System.currentTimeMillis() + plus + "`";
    }
    String ret = "ALTER TABLE " + escapeTableName(table.getName()) + (addConstraint ? constraint : " ADD")
      + " FOREIGN KEY (" + foreignRefs + ") REFERENCES " + escapeTableName(fkey.getReferencedTable()) + " ("
      + foreignReferenced + ")";
    return ret;
  }

  // MySQL does not support check constraints (returns an empty SQL query)
  @Override
  public String getCheckConstraintsSQL(String schemaName, String tableName) {
    return null;
  }

  @Override
  public String getTriggersSQL(String schemaName, String tableName) {
    return "SELECT " + "trigger_name AS TRIGGER_NAME, " + "action_timing AS ACTION_TIME, "
      + "event_manipulation AS TRIGGER_EVENT, " + "action_statement AS TRIGGERED_ACTION "
      + "FROM information_schema.triggers " + "WHERE trigger_schema='" + schemaName + "' " + "AND event_object_table='"
      + tableName + "'";
  }

  @Override
  public String getUsersSQL(String dbName) {
    return "SELECT * FROM `mysql`.`user`";
  }

  protected String escapeComment(String description) {
    return description.replaceAll("'", "''");
  }

  @Override
  public String getDatabases(String database) {
    return "SHOW DATABASES LIKE '" + database + "';";
  }

  @Override
  public String dropDatabase(String database) {
    return "DROP DATABASE IF EXISTS " + database;
  }

  @Override
  protected String escapePrimaryKeyName(String pkey_name) {
    if ("PRIMARY".equals(pkey_name)) {
      logger.debug("Cannot set primary key name to reserved name PRIMARY, renaming it");
      pkey_name += "_pkey";
    }

    return super.escapePrimaryKeyName(pkey_name);
  }

}
