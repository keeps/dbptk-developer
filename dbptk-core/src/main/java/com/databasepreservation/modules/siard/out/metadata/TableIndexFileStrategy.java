package com.databasepreservation.modules.siard.out.metadata;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
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
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.content.LOBsTracker;

import dk.sa.xmlns.diark._1_0.tableindex.ColumnType;
import dk.sa.xmlns.diark._1_0.tableindex.ColumnsType;
import dk.sa.xmlns.diark._1_0.tableindex.ForeignKeyType;
import dk.sa.xmlns.diark._1_0.tableindex.ForeignKeysType;
import dk.sa.xmlns.diark._1_0.tableindex.FunctionalDescriptionType;
import dk.sa.xmlns.diark._1_0.tableindex.PrimaryKeyType;
import dk.sa.xmlns.diark._1_0.tableindex.ReferenceType;
import dk.sa.xmlns.diark._1_0.tableindex.SiardDiark;
import dk.sa.xmlns.diark._1_0.tableindex.TableType;
import dk.sa.xmlns.diark._1_0.tableindex.TablesType;
import dk.sa.xmlns.diark._1_0.tableindex.ViewType;
import dk.sa.xmlns.diark._1_0.tableindex.ViewsType;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class TableIndexFileStrategy implements IndexFileStrategy {

  // LOBsTracker used to get the locations of functionalDescriptions
  private LOBsTracker lobsTracker;

  public TableIndexFileStrategy(LOBsTracker lobsTracker) {
    this.lobsTracker = lobsTracker;
  }

  private static final Logger logger = LoggerFactory.getLogger(TableIndexFileStrategy.class);

  @Override
  public Object generateXML(DatabaseStructure dbStructure) throws ModuleException {

    // Set version - mandatory
    SiardDiark siardDiark = new SiardDiark();
    siardDiark.setVersion("1.0");

    // Set dbName - mandatory
    siardDiark.setDbName(dbStructure.getName());

    // Set databaseProduct
    if (StringUtils.isNotBlank(dbStructure.getProductName())) {
      siardDiark.setDatabaseProduct(dbStructure.getProductName());
    }

    // Set tables - mandatory
    int tableCounter = 1;
    TablesType tablesType = new TablesType();

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
      logger.warn("No primary keys in the following table(s): "
        + tableListBuilder.substring(0, tableListBuilder.length() - 2));
    }

    for (SchemaStructure schemaStructure : schemas) {
      if (schemaStructure.getTables().isEmpty()) {
        logger.info("No tables found in this schema: " + schemaStructure.getName());
        continue;
      } else {
        for (TableStructure tableStructure : schemaStructure.getTables()) {

          // Set table - mandatory

          TableType tableType = new TableType();

          // Set name - mandatory
          tableType.setName(escapeString(tableStructure.getName()));

          // Set folder - mandatory
          tableType.setFolder("table" + Integer.toString(tableCounter));

          // Set description
          if (tableStructure.getDescription() != null && !tableStructure.getDescription().trim().isEmpty()) {
            tableType.setDescription(tableStructure.getDescription().trim());
          } else {
            tableType.setDescription("Description should be entered manually");
          }

          // Set columns - mandatory
          int columnCounter = 1;
          ColumnsType columns = new ColumnsType();
          for (ColumnStructure columnStructure : tableStructure.getColumns()) {

            // Set column - mandatory

            ColumnType column = new ColumnType();
            Type type = columnStructure.getType();

            // Set column name - mandatory
            column.setName(escapeString(columnStructure.getName()));

            // Set columnID - mandatory
            column.setColumnID("c" + Integer.toString(columnCounter));

            // Set type - mandatory
            String sql99DataType = type.getSql99TypeName();
            if (sql99DataType.equals(SIARDDKConstants.BINARY_LARGE_OBJECT)) {
              column.setType("INTEGER");
            } else if (sql99DataType.equals(SIARDDKConstants.CHARACTER_LARGE_OBJECT)) {

              if (lobsTracker.getMaxClobLength(tableCounter, columnCounter) > 0) {
                column.setType(SIARDDKConstants.DEFAULT_CLOB_TYPE + "("
                  + lobsTracker.getMaxClobLength(tableCounter, columnCounter) + ")");
              } else {
                column.setType(SIARDDKConstants.DEFAULT_CLOB_TYPE + "(1)");
              }
            } else {
              column.setType(type.getSql99TypeName());
            }

            // Set typeOriginal
            if (StringUtils.isNotBlank(type.getOriginalTypeName())) {
              column.setTypeOriginal(type.getOriginalTypeName());
            }

            // Set defaultValue
            if (StringUtils.isNotBlank(columnStructure.getDefaultValue())) {
              column.setDefaultValue(columnStructure.getDefaultValue());
            }

            // Set nullable
            column.setNullable(columnStructure.getNillable());

            // Set description
            if (columnStructure.getDescription() != null && !columnStructure.getDescription().trim().isEmpty()) {
              column.setDescription(columnStructure.getDescription().trim());
            } else {
              column.setDescription("Description should be set");
            }

            // Set functionalDescription
            String lobType = lobsTracker.getLOBsType(tableCounter, columnCounter);
            if (lobType != null) {
              if (lobType.equals(SIARDDKConstants.BINARY_LARGE_OBJECT)) {
                FunctionalDescriptionType functionalDescriptionType = FunctionalDescriptionType.DOKUMENTIDENTIFIKATION;
                column.getFunctionalDescription().add(functionalDescriptionType);
              }
            }

            columns.getColumn().add(column);
            columnCounter += 1;

          }
          tableType.setColumns(columns);

          // Set primary key - mandatory
          PrimaryKeyType primaryKeyType = new PrimaryKeyType(); // JAXB
          PrimaryKey primaryKey = tableStructure.getPrimaryKey();
          if (primaryKey == null) {
            primaryKeyType.setName("MISSING");
            primaryKeyType.getColumn().add("MISSING");
          } else {
            primaryKeyType.setName(escapeString(primaryKey.getName()));
            List<String> columnNames = primaryKey.getColumnNames();
            for (String columnName : columnNames) {
              // Set column names for primary key

              primaryKeyType.getColumn().add(escapeString(columnName));
            }
          }
          tableType.setPrimaryKey(primaryKeyType);

          // Set foreignKeys
          ForeignKeysType foreignKeysType = new ForeignKeysType();
          List<ForeignKey> foreignKeys = tableStructure.getForeignKeys();
          if (foreignKeys != null && foreignKeys.size() > 0) {
            for (ForeignKey key : foreignKeys) {
              ForeignKeyType foreignKeyType = new ForeignKeyType();

              // Set key name - mandatory
              foreignKeyType.setName(escapeString(key.getName()));

              // Set referenced table - mandatory
              foreignKeyType.setReferencedTable(escapeString(key.getReferencedTable()));

              // Set reference - mandatory
              for (Reference ref : key.getReferences()) {
                ReferenceType referenceType = new ReferenceType();
                referenceType.setColumn(escapeString(ref.getColumn()));
                referenceType.setReferenced(escapeString(ref.getReferenced()));
                foreignKeyType.getReference().add(referenceType);
              }
              foreignKeysType.getForeignKey().add(foreignKeyType);
            }
            tableType.setForeignKeys(foreignKeysType);
          }

          // Set rows
          if (tableStructure.getRows() >= 0) {
            tableType.setRows(BigInteger.valueOf(tableStructure.getRows()));
          } else {
            throw new ModuleException("Error while exporting table structure: number of table rows not set");
          }

          tablesType.getTable().add(tableType);

          tableCounter += 1;
        }

        // Set views
        List<ViewStructure> viewStructures = schemaStructure.getViews();

        if (viewStructures != null && viewStructures.size() > 0) {
          ViewsType viewsType = new ViewsType();
          for (ViewStructure viewStructure : viewStructures) {

            // Set view - mandatory
            ViewType viewType = new ViewType();

            // Set view name - mandatory
            viewType.setName(escapeString(viewStructure.getName()));

            // Set queryOriginal - mandatory
            if (StringUtils.isNotBlank(viewStructure.getQueryOriginal())) {
              viewType.setQueryOriginal(viewStructure.getQueryOriginal());
            } else {
              viewType.setQueryOriginal("unknown");
            }

            // Set description
            if (StringUtils.isNotBlank(viewStructure.getDescription())) {
              viewType.setDescription(viewStructure.getDescription());
            }

            viewsType.getView().add(viewType);
          }
          siardDiark.setViews(viewsType);
        }
      }
    }
    siardDiark.setTables(tablesType);

    return siardDiark;
  }

  private String escapeString(String s) {
    if (s.contains(" ")) {
      s = new StringBuilder().append("\"").append(s).append("\"").toString();
    }
    return s;
  }
}
