package com.databasepreservation.modules.siard.metadata;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.metadata.jaxb.siard1.SiardArchive;

public class MetadataStrategySIARD1 implements MetadataStrategy {

	@Override
	public void output(DatabaseStructure database)
			throws ModuleException {

		JAXBContext context;
		try {
			context = JAXBContext.newInstance("com.databasepreservation.modules.siard.metadata.jaxb.siard1");
		} catch (JAXBException e) {
			throw new ModuleException("Error loading JAXBContext", e);
		}

		SiardArchive root = jaxbSiardArchive(database);

		Marshaller m;
		try {
			m = context.createMarshaller();
	        m.marshal( root, System.out );
		} catch (JAXBException e) {
			throw new ModuleException("Error in JAXB Marshalling", e);
		}
	}

	private SiardArchive jaxbSiardArchive(DatabaseStructure database){
		SiardArchive elem = new SiardArchive();
		elem.setArchivalDate(database.getArchivalDate());


		return elem;
	}

}
