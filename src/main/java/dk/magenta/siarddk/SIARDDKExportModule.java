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
import dk.magenta.siarddk.tableindex.ObjectFactory;
import dk.magenta.siarddk.tableindex.SiardDiark;

public class SIARDDKExportModule implements DatabaseHandler {

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
		
		
		
		SiardDiark tableIndex = new SiardDiark();
		tableIndex.setVersion("1.0");
		tableIndex.setDbName(dbStructure.getName());
		
        // create an element for marshalling
        // JAXBElement<SiardDiark> tiElement = (new ObjectFactory()).createSiardDiark();

        // create a Marshaller and marshal to System.out
        JAXB.marshal( tableIndex, System.out );

		

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
