/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.metadata;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.Reference;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.common.adapters.SIARDDKAdapter;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.content.LOBsTracker;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDKTableIndexFileStrategy implements IndexFileStrategy {

  // LOBsTracker used to get the locations of functionalDescriptions
  private LOBsTracker lobsTracker;
  private String regex;
  private SIARDDKAdapter siarddkBinding;

  public SIARDDKTableIndexFileStrategy(LOBsTracker lobsTracker, SIARDDKAdapter siarddkAdapter) {
    this.lobsTracker = lobsTracker;
    this.siarddkBinding = siarddkAdapter;
    regex = "(\\p{L}(_|\\w)*)|(\".*\")";
  }

  private static final Logger logger = LoggerFactory.getLogger(SIARDDKTableIndexFileStrategy.class);

  @Override
  public Object generateXML(DatabaseStructure dbStructure) throws ModuleException {

    // Set version - mandatory
    siarddkBinding.setVersion("1.0");

    // Set dbName - mandatory
    if (dbStructure.getDbOriginalName() != null) {
      siarddkBinding.setDbName(dbStructure.getDbOriginalName());
    } else {
      siarddkBinding.setDbName(dbStructure.getName());
    }

    // Set databaseProduct
    if (StringUtils.isNotBlank(dbStructure.getProductName())) {
      siarddkBinding.setDatabaseProduct(dbStructure.getProductName());
    }

    // Set tables - mandatory
    int tableCounter = 1;
    List<SchemaStructure> schemas = dbStructure.getSchemas();
    // System.out.println(schemas.get(0));

    // Check that all tables have primary keys
    List<String> tablesWithNoPrimaryKeys = new ArrayList<String>();
    StringBuilder tableListBuilder = new StringBuilder();
    for (SchemaStructure schemaStructure : schemas) {
      if (!schemaStructure.getTables().isEmpty()) {
        for (TableStructure tableStructure : schemaStructure.getTables()) {
          PrimaryKey primaryKey = tableStructure.getPrimaryKey();
          if (primaryKey == null) {
            tablesWithNoPrimaryKeys.add(tableStructure.getName());
            tableListBuilder.append(schemaStructure.getName()).append(".").append(tableStructure.getName())
              .append(", ");
          }
        }
      }
    }
    if (!tablesWithNoPrimaryKeys.isEmpty()) {
      logger.warn(
        "No primary keys in the following table(s): " + tableListBuilder.substring(0, tableListBuilder.length() - 2));
    }

    for (SchemaStructure schemaStructure : schemas) {
      if (schemaStructure.getTables().isEmpty()) {
        logger.info("No tables found in this schema: " + schemaStructure.getName());
        continue;
      } else {
        for (TableStructure tableStructure : schemaStructure.getTables()) {
          // Set name - mandatory
          String tableName = escapeString(tableStructure.getName());

          // Set folder - mandatory
          String tableFolder = "table" + Integer.toString(tableCounter);

          // Set description
          String tableDescription = "Description should be entered manually";
          if (tableStructure.getDescription() != null && !tableStructure.getDescription().trim().isEmpty()) {
            tableDescription = tableStructure.getDescription().trim();
          }
          siarddkBinding.addTable(tableName, tableFolder, tableDescription);

          // Set columns - mandatory
          int columnCounter = 1;
          for (ColumnStructure columnStructure : tableStructure.getColumns()) {

            // Set column - mandatory
            Type type = columnStructure.getType();

            // Set column name - mandatory
            String columnName = escapeString(columnStructure.getName());

            // Set columnID - mandatory
            String columnID = "c" + Integer.toString(columnCounter);

            // Set type - mandatory
            String sql99DataType = type.getSql99TypeName();
            String columType = type.getSql99TypeName();
            if (sql99DataType.equals(SIARDDKConstants.BINARY_LARGE_OBJECT)) {
              columType = "INTEGER";
            } else if (sql99DataType.equals(SIARDDKConstants.CHARACTER_LARGE_OBJECT)) {

              if (lobsTracker.getMaxClobLength(tableCounter, columnCounter) > 0) {
                columType = SIARDDKConstants.DEFAULT_CLOB_TYPE + "("
                  + lobsTracker.getMaxClobLength(tableCounter, columnCounter) + ")";
              } else {
                columType = SIARDDKConstants.DEFAULT_CLOB_TYPE + "(1)";
              }
            } else if (sql99DataType.startsWith("BIT VARYING") || sql99DataType.startsWith("BINARY VARYING")) {

              // Convert BIT VARYING/BINARY VARYING TO CHARACTER VARYING

              String length = sql99DataType.split("\\(")[1].trim();
              length = length.substring(0, length.length() - 1);
              columType = "CHARACTER VARYING(" + length + ")";

            }

            // Set typeOriginal
            String columnOriginalType = StringUtils.isNotBlank(type.getOriginalTypeName()) ? type.getOriginalTypeName()
              : null;

            // Set defaultValue
            String columnDefaultValue = StringUtils.isNotBlank(columnStructure.getDefaultValue())
              ? columnStructure.getDefaultValue()
              : null;

            // Set nullable
            Boolean columnStructureNillable = columnStructure.isNillable();

            // Set description
            String columnDescription = "Description should be set";
            if (columnStructure.getDescription() != null && !columnStructure.getDescription().trim().isEmpty()) {
              columnDescription = columnStructure.getDescription().trim();
            }

            // Set functionalDescription
            String lobType = lobsTracker.getLOBsType(tableCounter, columnCounter);

            siarddkBinding.addColumnForTable(tableName, columnName, columnID, columType, columnOriginalType,
              columnDefaultValue, columnStructureNillable, columnDescription, lobType);
            columnCounter += 1;

          }

          // Set primary key - mandatory
          PrimaryKey primaryKey = tableStructure.getPrimaryKey();
          if (primaryKey == null) {
            siarddkBinding.addPrimaryKeyForTable(tableName, "MISSING", List.of("MISSING"));
          } else {
            String primaryKeyName = escapeString(primaryKey.getName());
            List<String> columnNames = primaryKey.getColumnNames();
            List<String> escapedColumnNames = new ArrayList<>();
            for (String columnName : columnNames) {
              escapedColumnNames.add(escapeString(columnName));
            }

            siarddkBinding.addPrimaryKeyForTable(tableName, primaryKeyName, escapedColumnNames);
          }

          // Set foreignKeys
          List<ForeignKey> foreignKeys = tableStructure.getForeignKeys();
          if (foreignKeys != null && foreignKeys.size() > 0) {
            for (ForeignKey key : foreignKeys) {
              // Set key name - mandatory
              String foreignKeyName = escapeString(key.getName());

              // Set referenced table - mandatory
              String foreignKeyReferencedTable = escapeString(key.getReferencedTable());

              // Set reference - mandatory
              Map<String, String> escapedReferencedColumns = new LinkedHashMap<>();
              for (Reference ref : key.getReferences()) {
                escapedReferencedColumns.put(escapeString(ref.getColumn()), escapeString(ref.getReferenced()));
              }

              siarddkBinding.addForeignKeyForTable(tableName, foreignKeyName, foreignKeyReferencedTable,
                escapedReferencedColumns);
            }
          }

          // Set rows
          if (tableStructure.getRows() >= 0) {
            siarddkBinding.setRowsForTable(tableName, BigInteger.valueOf(tableStructure.getRows()));
          } else {
            throw new ModuleException()
              .withMessage("Error while exporting table structure: number of table rows not set");
          }

          tableCounter += 1;
        }

        // Set views
        List<ViewStructure> viewStructures = schemaStructure.getViews();

        if (viewStructures != null && viewStructures.size() > 0) {
          for (ViewStructure viewStructure : viewStructures) {
            // Set view name - mandatory
            String viewName = escapeString(viewStructure.getName());

            // Set queryOriginal - mandatory
            String viewQueryOriginal = "unknown";
            if (StringUtils.isNotBlank(viewStructure.getQueryOriginal())) {
              viewQueryOriginal = viewStructure.getQueryOriginal();
            }

            // Set description
            String viewDescription = StringUtils.isNotBlank(viewStructure.getDescription())
              ? viewStructure.getDescription()
              : null;

            siarddkBinding.addView(viewName, viewQueryOriginal, viewDescription);
          }
        }
      }
    }

    return siarddkBinding.getSIARDDK();
  }

  String escapeString(String s) {
    if (s.matches(regex)) {
      return s;
    } else {
      s = new StringBuilder().append("\"").append(s).append("\"").toString();
    }
    return s;
  }
}
