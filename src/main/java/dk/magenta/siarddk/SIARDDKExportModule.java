package dk.magenta.siarddk;

import java.util.Set;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.DatabaseHandler;

import dk.magenta.common.MetadataStrategy;

public class SIARDDKExportModule implements DatabaseHandler {

	private MetadataStrategy metadataStrategy;
	
	public SIARDDKExportModule(MetadataStrategy metadataStrategy) {
		this.metadataStrategy = metadataStrategy;
	}
	
	@Override
	public void initDatabase() throws ModuleException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setIgnoredSchemas(Set<String> ignoredSchemas) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleStructure(DatabaseStructure dbStructure)
			throws ModuleException, UnknownTypeException {
		// System.out.println("This is a test");
		
		// Generate tableIndex.xml
		metadataStrategy.generateMetaData(dbStructure);
	}

	@Override
	public void handleDataOpenTable(String schemaName, String tableId) throws ModuleException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleDataCloseTable(String schemaName, String tableId) throws ModuleException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleDataRow(Row row) throws InvalidDataException,
			ModuleException {
		// TODO Auto-generated method stub

	}

	@Override
	public void finishDatabase() throws ModuleException {
		// TODO Auto-generated method stub

	}

}
