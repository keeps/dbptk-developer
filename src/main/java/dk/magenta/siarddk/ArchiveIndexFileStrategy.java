/**
 * The archiveIndex.xml file only contains manual data.
 * In this class the archiveIndex file is just given as a 
 * parameter on the command line.
 */
package dk.magenta.siarddk;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;

public class ArchiveIndexFileStrategy implements IndexFileStrategy {

	@Override
	public Object generateXML(DatabaseStructure dbStructure)
			throws ModuleException {

		
		
		return null;
	}

}
