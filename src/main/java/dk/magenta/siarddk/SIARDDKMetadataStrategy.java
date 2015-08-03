package dk.magenta.siarddk;

import javax.xml.bind.JAXB;

import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.ColumnStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.DatabaseStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SchemaStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.TableStructure;
import dk.magenta.common.MetadataStrategy;
import dk.magenta.siarddk.tableindex.ColumnType;
import dk.magenta.siarddk.tableindex.ColumnsType;
import dk.magenta.siarddk.tableindex.SiardDiark;
import dk.magenta.siarddk.tableindex.TableType;
import dk.magenta.siarddk.tableindex.TablesType;

public class SIARDDKMetadataStrategy implements MetadataStrategy {

	@Override
	public void generateMetaData(DatabaseStructure dbStructure) throws ModuleException{
		
		SiardDiark tableIndex = new SiardDiark();
		tableIndex.setVersion("1.0");

		// Set dbName
		// Question: what is dbName is null?
		String dbName = dbStructure.getName();
		if (dbStructure.getName() != null) {
			if (dbName.length() >= 1 && dbName.length() <= 128) {
				if (dbName.matches("(\\p{L}(_|\\w)*)|(&quot;.*&quot;)")) {  // Should be tested more
					tableIndex.setDbName(dbName);					
				} else {
					throw new ModuleException("tableIndex metadata error: dbName should match SQLIdentifier.");
				}
			} else {
				throw new ModuleException("tableIndex metadata error: dbName length is incorrect.");
			}
		} else {
			throw new ModuleException("tableIndex metadata error: dbName cannot be null.");
		}
		
		// Set databaseProduct
		if (dbStructure.getProductName() != null) {
			tableIndex.setDatabaseProduct(dbStructure.getProductName());	
		}
		
		// Set tables 
		TablesType tables = new TablesType();
		if (dbStructure.getSchemas() == null) {
			throw new ModuleException("No schemas in database structure!");
		}
		for (SchemaStructure schemaStructure : dbStructure.getSchemas()) {
			if (schemaStructure.getTables() == null) {
				throw new ModuleException("No tables found!");
			}
			for (TableStructure tableStructure : schemaStructure.getTables()) {
				TableType table = new TableType();
				table.setName(tableStructure.getName());
				table.setFolder(tableStructure.getFolder());
				table.setDescription(tableStructure.getDescription());
				
				ColumnsType columns = new ColumnsType();
				for (ColumnStructure columnStructure : tableStructure.getColumns()) {
					ColumnType column = new ColumnType();
					column.setName(columnStructure.getName());
				}
				
				tables.getTable().add(table);
			}
		}
		tableIndex.setTables(tables);
		
        // create a Marshaller and marshal to System.out
        JAXB.marshal( tableIndex, System.out );

		
	}

}
