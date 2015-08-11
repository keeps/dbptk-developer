package com.databasepreservation.modules.siard.out.metadata;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.out.write.OutputContainer;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface MetadataExportStrategy {
	void writeMetadataXML(DatabaseStructure databaseStructure, OutputContainer container) throws ModuleException;
	void writeMetadataXSD(DatabaseStructure databaseStructure, OutputContainer container) throws ModuleException;
}
