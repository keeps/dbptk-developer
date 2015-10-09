package dk.magenta.siarddk;

import java.math.BigInteger;
import java.util.List;

import org.apache.commons.lang.StringUtils;

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

import dk.sa.xmlns.diark._1_0.tableindex.ColumnType;
import dk.sa.xmlns.diark._1_0.tableindex.ColumnsType;
import dk.sa.xmlns.diark._1_0.tableindex.ForeignKeyType;
import dk.sa.xmlns.diark._1_0.tableindex.ForeignKeysType;
import dk.sa.xmlns.diark._1_0.tableindex.PrimaryKeyType;
import dk.sa.xmlns.diark._1_0.tableindex.ReferenceType;
import dk.sa.xmlns.diark._1_0.tableindex.SiardDiark;
import dk.sa.xmlns.diark._1_0.tableindex.TableType;
import dk.sa.xmlns.diark._1_0.tableindex.TablesType;
import dk.sa.xmlns.diark._1_0.tableindex.ViewType;
import dk.sa.xmlns.diark._1_0.tableindex.ViewsType;

//import dk.magenta.siarddk.tableindex.ColumnType;
//import dk.magenta.siarddk.tableindex.ColumnsType;
//import dk.magenta.siarddk.tableindex.ForeignKeyType;
//import dk.magenta.siarddk.tableindex.ForeignKeysType;
//import dk.magenta.siarddk.tableindex.PrimaryKeyType;
//import dk.magenta.siarddk.tableindex.ReferenceType;
//import dk.magenta.siarddk.tableindex.SiardDiark;
//import dk.magenta.siarddk.tableindex.TableType;
//import dk.magenta.siarddk.tableindex.TablesType;
//import dk.magenta.siarddk.tableindex.ViewType;
//import dk.magenta.siarddk.tableindex.ViewsType;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class TableIndexFileStrategy implements IndexFileStrategy {

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
    if (schemas != null && !schemas.isEmpty()) {
      for (SchemaStructure schemaStructure : schemas) {
        if (schemaStructure.getTables() == null) {
          throw new ModuleException("No tables found in schema!");
        } else {
          for (TableStructure tableStructure : schemaStructure.getTables()) {

            // Set table - mandatory

            TableType tableType = new TableType();

            // Set name - mandatory
            tableType.setName(tableStructure.getName());

            // Set folder - mandatory
            tableType.setFolder("table" + Integer.toString(tableCounter));

            // TO-DO: fix how description should be obtained
            // Set description
            tableType.setDescription("Description should be entered manually");

            // Set columns - mandatory
            int columnCounter = 1;
            ColumnsType columns = new ColumnsType();
            for (ColumnStructure columnStructure : tableStructure.getColumns()) {

              // Set column - mandatory

              ColumnType column = new ColumnType();
              Type type = columnStructure.getType();

              // Set column name - mandatory
              column.setName(columnStructure.getName());

              // Set columnID - mandatory
              column.setColumnID("c" + Integer.toString(columnCounter));

              // Set type - mandatory
              column.setType(type.getSql99TypeName());

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

              // TO-DO: get (how?) and set description
              // Set description
              column.setDescription("Description should be set");

              // TO-DO: get (how?) and set functional description
              // Set functionalDescription

              columns.getColumn().add(column);
              columnCounter += 1;

            }
            tableType.setColumns(columns);

            // Set primary key - mandatory
            PrimaryKeyType primaryKeyType = new PrimaryKeyType(); // JAXB
            PrimaryKey primaryKey = tableStructure.getPrimaryKey();

            primaryKeyType.setName(primaryKey.getName());
            List<String> columnNames = primaryKey.getColumnNames();
            for (String columnName : columnNames) {
              // Set column names for primary key

              primaryKeyType.getColumn().add(columnName);
            }
            tableType.setPrimaryKey(primaryKeyType);

            // Set foreignKeys
            ForeignKeysType foreignKeysType = new ForeignKeysType();
            List<ForeignKey> foreignKeys = tableStructure.getForeignKeys();
            if (foreignKeys != null && foreignKeys.size() > 0) {
              for (ForeignKey key : foreignKeys) {
                ForeignKeyType foreignKeyType = new ForeignKeyType();

                // Set key name - mandatory
                foreignKeyType.setName(key.getName());

                // Set referenced table - mandatory
                foreignKeyType.setReferencedTable(key.getReferencedTable());

                // Set reference - mandatory
                for (Reference ref : key.getReferences()) {
                  ReferenceType referenceType = new ReferenceType();
                  referenceType.setColumn(ref.getColumn());
                  referenceType.setReferenced(ref.getReferenced());
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
              viewType.setName(viewStructure.getName());

              // Set queryOriginal - mandatory
              viewType.setQueryOriginal(viewStructure.getQueryOriginal());

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
    } else {
      throw new ModuleException("No schemas in database structure!");
    }

    return siardDiark;
  }

}
