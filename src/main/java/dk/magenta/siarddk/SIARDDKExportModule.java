package dk.magenta.siarddk;

import java.nio.file.Path;
import java.util.Set;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.DatabaseHandler;
import com.databasepreservation.modules.siard.out.write.OutputContainer;

import dk.magenta.common.MetadataStrategy;

public class SIARDDKExportModule implements DatabaseHandler {

	private MetadataStrategy metadataStrategy;
	private OutputContainer mainContainer;
	
	public SIARDDKExportModule(Path siardPackage, MetadataStrategy metadataStrategy) {
		this.metadataStrategy = metadataStrategy;
		mainContainer = new OutputContainer(siardPackage, OutputContainer.OutputContainerType.INSIDE_ARCHIVE);
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
		
		if (dbStructure == null) {
			throw new ModuleException("Database structure must not be null");
		}

		
		// Generate tableIndex.xml
		// metadataStrategy.generateMetaData(dbStructure);
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
