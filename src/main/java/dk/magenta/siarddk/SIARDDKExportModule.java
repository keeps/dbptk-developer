package dk.magenta.siarddk;

import java.util.Set;

import javax.xml.bind.JAXB;
import javax.xml.bind.JAXBElement;

import pt.gov.dgarq.roda.common.convert.db.model.data.Row;
import pt.gov.dgarq.roda.common.convert.db.model.exception.InvalidDataException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.DatabaseStructure;
import pt.gov.dgarq.roda.common.convert.db.modules.DatabaseHandler;
import dk.magenta.common.MetadataStrategy;
import dk.magenta.siarddk.tableindex.ObjectFactory;
import dk.magenta.siarddk.tableindex.SiardDiark;

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
	public void handleDataOpenTable(String tableId) throws ModuleException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleDataCloseTable(String tableId) throws ModuleException {
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
