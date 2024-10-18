package com.databasepreservation.modules.siard.out.metadata;

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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public abstract class SIARDDKTableIndexFileStrategy<T, // SiardDiark Type
  TTS, // TablesType
  TT, // TableType
  CT, // ColumnType
  CTS, // ColumnsType
  PKT, // PrimaryKeyType
  FKTS, // ForeignKeysType
  FKT, // ForeignKeyType
  RT, // ReferenceType
  VT, // ViewType
  VTS, // ViewsType
  FDT // FunctionalDescriptionType
> implements IndexFileStrategy {

  // LOBsTracker used to get the locations of functionalDescriptions
  private LOBsTracker lobsTracker;
  private String regex;

  public SIARDDKTableIndexFileStrategy(LOBsTracker lobsTracker) {
    this.lobsTracker = lobsTracker;
    regex = "(\\p{L}(_|\\w)*)|(\".*\")";
  }

  private static final Logger logger = LoggerFactory.getLogger(SIARDDKTableIndexFileStrategy.class);

  @Override
  public Object generateXML(DatabaseStructure dbStructure) throws ModuleException {

    // Set version - mandatory
    T siardDiark = createSiardDiarkInstance();
    setVersion(siardDiark, "1.0");

    // Set dbName - mandatory
    if (dbStructure.getDbOriginalName() != null){
      setDbName(siardDiark, dbStructure.getDbOriginalName());
    } else {
      setDbName(siardDiark, dbStructure.getName());
    }


    // Set databaseProduct
    if (StringUtils.isNotBlank(dbStructure.getProductName())) {
      setDatabaseProduct(siardDiark, dbStructure.getProductName());
    }

    // Set tables - mandatory
    int tableCounter = 1;
    TTS tablesType = createTablesTypeInstance();

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

          // Set table - mandatory

          TT tableType = createTableTypeInstance();

          // Set name - mandatory
          setTableName(tableType, escapeString(tableStructure.getName()));

          // Set folder - mandatory
          setFolder(tableType, "table" + Integer.toString(tableCounter));

          // Set description
          if (tableStructure.getDescription() != null && !tableStructure.getDescription().trim().isEmpty()) {
            setTableDescription(tableType, tableStructure.getDescription().trim());
          } else {
            setTableDescription(tableType, "Description should be entered manually");
          }

          // Set columns - mandatory
          int columnCounter = 1;
          CTS columns = createColumnsTypeInstance();
          for (ColumnStructure columnStructure : tableStructure.getColumns()) {

            // Set column - mandatory

            CT column = createColumnTypeInstance();
            Type type = columnStructure.getType();

            // Set column name - mandatory
            setColumnName(column, escapeString(columnStructure.getName()));

            // Set columnID - mandatory
            setColumnID(column, "c" + Integer.toString(columnCounter));

            // Set type - mandatory
            String sql99DataType = type.getSql99TypeName();
            if (sql99DataType.equals(SIARDDKConstants.BINARY_LARGE_OBJECT)) {
              setType(column, "INTEGER");
            } else if (sql99DataType.equals(SIARDDKConstants.CHARACTER_LARGE_OBJECT)) {

              if (lobsTracker.getMaxClobLength(tableCounter, columnCounter) > 0) {
                setType(column, SIARDDKConstants.DEFAULT_CLOB_TYPE + "("
                  + lobsTracker.getMaxClobLength(tableCounter, columnCounter) + ")");
              } else {
                setType(column, SIARDDKConstants.DEFAULT_CLOB_TYPE + "(1)");
              }
            } else if (sql99DataType.startsWith("BIT VARYING") || sql99DataType.startsWith("BINARY VARYING")) {

              // Convert BIT VARYING/BINARY VARYING TO CHARACTER VARYING

              String length = sql99DataType.split("\\(")[1].trim();
              length = length.substring(0, length.length() - 1);
              setType(column, "CHARACTER VARYING(" + length + ")");

            } else {
              setType(column, type.getSql99TypeName());
            }

            // Set typeOriginal
            if (StringUtils.isNotBlank(type.getOriginalTypeName())) {
              setTypeOriginal(column, type.getOriginalTypeName());
            }

            // Set defaultValue
            if (StringUtils.isNotBlank(columnStructure.getDefaultValue())) {
              setDefaultValue(column, columnStructure.getDefaultValue());
            }

            // Set nullable
            setNullable(column, columnStructure.isNillable());

            // Set description
            if (columnStructure.getDescription() != null && !columnStructure.getDescription().trim().isEmpty()) {
              setColumnDescription(column, columnStructure.getDescription().trim());
            } else {
              setColumnDescription(column, "Description should be set");
            }

            // Set functionalDescription
            String lobType = lobsTracker.getLOBsType(tableCounter, columnCounter);
            if (lobType != null) {
              if (lobType.equals(SIARDDKConstants.BINARY_LARGE_OBJECT)) {
                FDT functionalDescriptionType = createDOKUMENTIDENTIFIKATION();
                getFunctionalDescription(column).add(functionalDescriptionType);
              }
            }

            getColumn(columns).add(column);
            columnCounter += 1;

          }
          setColumns(tableType, columns);

          // Set primary key - mandatory
          PKT primaryKeyType = createPrimaryKeyTypeInstance(); // JAXB
          PrimaryKey primaryKey = tableStructure.getPrimaryKey();
          if (primaryKey == null) {
            setPrimaryKeyName(primaryKeyType, "MISSING");
            getPrimaryKeyTypeColumn(primaryKeyType).add("MISSING");
          } else {
            setPrimaryKeyName(primaryKeyType, escapeString(primaryKey.getName()));
            List<String> columnNames = primaryKey.getColumnNames();
            for (String columnName : columnNames) {
              // Set column names for primary key

              getPrimaryKeyTypeColumn(primaryKeyType).add(escapeString(columnName));
            }
          }
          setPrimaryKey(tableType, primaryKeyType);

          // Set foreignKeys
          FKTS foreignKeysType = createForeignKeysTypeInstance();
          List<ForeignKey> foreignKeys = tableStructure.getForeignKeys();
          if (foreignKeys != null && foreignKeys.size() > 0) {
            for (ForeignKey key : foreignKeys) {
              FKT foreignKeyType = createForeignKeyTypeInstance();

              // Set key name - mandatory
              setForeignKeyName(foreignKeyType, escapeString(key.getName()));

              // Set referenced table - mandatory
              setReferencedTable(foreignKeyType, escapeString(key.getReferencedTable()));

              // Set reference - mandatory
              for (Reference ref : key.getReferences()) {
                RT referenceType = createReferenceTypeInstance();
                setReferencedColumn(referenceType, escapeString(ref.getColumn()));
                setReferenced(referenceType, escapeString(ref.getReferenced()));
                getReference(foreignKeyType).add(referenceType);
              }
              getForeignKey(foreignKeysType).add(foreignKeyType);
            }
            setForeignKeys(tableType, foreignKeysType);
          }

          // Set rows
          if (tableStructure.getRows() >= 0) {
            setRows(tableType, BigInteger.valueOf(tableStructure.getRows()));
          } else {
            throw new ModuleException()
              .withMessage("Error while exporting table structure: number of table rows not set");
          }

          getTable(tablesType).add(tableType);

          tableCounter += 1;
        }

        // Set views
        List<ViewStructure> viewStructures = schemaStructure.getViews();

        if (viewStructures != null && viewStructures.size() > 0) {
          VTS viewsType = createViewsTypeInstance();
          for (ViewStructure viewStructure : viewStructures) {

            // Set view - mandatory
            VT viewType = createViewTypeInstance();

            // Set view name - mandatory
            setViewName(viewType, escapeString(viewStructure.getName()));

            // Set queryOriginal - mandatory
            if (StringUtils.isNotBlank(viewStructure.getQueryOriginal())) {
              setQueryOriginal(viewType, viewStructure.getQueryOriginal());
            } else {
              setQueryOriginal(viewType, "unknown");
            }

            // Set description
            if (StringUtils.isNotBlank(viewStructure.getDescription())) {
              setViewDescription(viewType, viewStructure.getDescription());
            }

            getView(viewsType).add(viewType);
          }
          setViews(siardDiark, viewsType);
        }
      }
    }
    setTables(siardDiark, tablesType);

    return siardDiark;
  }

  String escapeString(String s) {
    if (s.matches(regex)) {
      return s;
    } else {
      s = new StringBuilder().append("\"").append(s).append("\"").toString();
    }
    return s;
  }

  abstract T createSiardDiarkInstance();

  abstract TT createTableTypeInstance();

  abstract TTS createTablesTypeInstance();

  abstract CT createColumnTypeInstance();

  abstract CTS createColumnsTypeInstance();

  abstract PKT createPrimaryKeyTypeInstance();

  abstract FKTS createForeignKeysTypeInstance();

  abstract FKT createForeignKeyTypeInstance();

  abstract RT createReferenceTypeInstance();

  abstract VT createViewTypeInstance();

  abstract VTS createViewsTypeInstance();

  abstract void setDbName(T siardDiark, String dbName);

  abstract void setVersion(T siardDiark, String version);

  abstract void setDatabaseProduct(T siardDiark, String databaseProduct);

  abstract void setColumnName(CT columnType, String name);

  abstract void setColumnID(CT columnType, String id);

  abstract void setType(CT columnType, String type);

  abstract void setTypeOriginal(CT columnType, String typeOriginal);

  abstract void setDefaultValue(CT columnType, String defaultValue);

  abstract void setNullable(CT columnType, boolean nullable);

  abstract List<FDT> getFunctionalDescription(CT columnType);

  abstract void setColumnDescription(CT columnType, String description);

  abstract void setTableName(TT tableType, String name);

  abstract void setFolder(TT tableType, String folder);

  abstract void setTableDescription(TT tableType, String description);

  abstract void setColumns(TT tableType, CTS columns);

  abstract void setForeignKeys(TT tableType, FKTS foreignKeysType);

  abstract void setRows(TT tableType, BigInteger rows);

  abstract void setPrimaryKey(TT tableType, PKT primaryKeyType);

  abstract List<TT> getTable(TTS tablesType);

  abstract List<CT> getColumn(CTS columns);

  abstract FDT createDOKUMENTIDENTIFIKATION();

  abstract void setPrimaryKeyName(PKT primaryKeyType, String name);

  abstract List<String> getPrimaryKeyTypeColumn(PKT primaryKeyType);

  abstract void setForeignKeyName(FKT foreignKeyType, String name);

  abstract List<RT> getReference(FKT foreignKeyType);

  abstract List<FKT> getForeignKey(FKTS foreignKeysType);

  abstract void setReferencedTable(FKT foreignKeyType, String referencedTable);

  abstract void setReferencedColumn(RT referenceType, String referencedColumn);

  abstract void setReferenced(RT referenceType, String referenced);

  abstract void setViewName(VT viewType, String name);

  abstract void setQueryOriginal(VT viewType, String query);

  abstract void setViewDescription(VT viewType, String description);

  abstract List<VT> getView(VTS viewsType);

  abstract void setViews(T siardDiark, VTS viewsType);

  abstract void setTables(T siardDiark, TTS tablesType);

}
