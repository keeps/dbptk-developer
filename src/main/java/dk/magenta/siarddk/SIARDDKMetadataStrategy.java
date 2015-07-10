package dk.magenta.siarddk;

import javax.xml.bind.JAXB;

import pt.gov.dgarq.roda.common.convert.db.model.structure.DatabaseStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SchemaStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.TableStructure;
import dk.magenta.common.MetadataStrategy;
import dk.magenta.siarddk.tableindex.SiardDiark;
import dk.magenta.siarddk.tableindex.TableType;
import dk.magenta.siarddk.tableindex.TablesType;

public class SIARDDKMetadataStrategy implements MetadataStrategy {

	@Override
	public void generateMetaData(DatabaseStructure dbStructure) {
		
		SiardDiark tableIndex = new SiardDiark();
		tableIndex.setVersion("1.0");
		tableIndex.setDbName(dbStructure.getName());
		tableIndex.setDatabaseProduct(dbStructure.getProductName());
		
		// Add metadata for tables
		TablesType tables = new TablesType();
		for (SchemaStructure schemaStructure : dbStructure.getSchemas()) {
			for (TableStructure tableStructure : schemaStructure.getTables()) {
				TableType table = new TableType();
				table.setName(tableStructure.getName());
				table.setFolder(tableStructure.getFolder());
				table.setDescription(tableStructure.getDescription());
				
				tables.getTable().add(table);
			}
		}
		tableIndex.setTables(tables);
		
        // create a Marshaller and marshal to System.out
        JAXB.marshal( tableIndex, System.out );

		
	}

}
