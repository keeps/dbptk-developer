package dk.magenta.siarddk;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;

public interface IndexFileStrategy {
	/**
	 * Generates the jaxbElement to be marshalled by the SIARDMarshaller.
	 * @param dbStructure The DatabaseStructure to extract metadata from.
	 * @return JAXB element to marshal.
	 */
	Object generateXML(DatabaseStructure dbStructure) throws ModuleException;
}
