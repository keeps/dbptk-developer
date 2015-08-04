package dk.magenta.common;

import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.DatabaseStructure;

public interface MetadataStrategy {
	
	/**
	 * Generates the metadata XML from a given database structure.
	 * @param dbStructure The database structure to generate the metadata from.
	 * @throws ModuleException
	 */
	public void generateMetaData(DatabaseStructure dbStructure) throws ModuleException;
	
	/**
	 * Validates if the input from the database is correct according the XML schema for 
	 * the corresponding metadata file.
	 * @param type
	 * @return True if input is valid and throws and exception otherwise
	 * @throws ModuleException If the input is not valid.
	 */
	public boolean validateInput(String type, String input) throws ModuleException;
}
