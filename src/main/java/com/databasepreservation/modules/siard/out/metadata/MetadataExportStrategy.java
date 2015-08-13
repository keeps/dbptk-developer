package com.databasepreservation.modules.siard.out.metadata;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface MetadataExportStrategy {
	void writeMetadataXML(DatabaseStructure databaseStructure, SIARDArchiveContainer container) throws ModuleException;
	void writeMetadataXSD(DatabaseStructure databaseStructure, SIARDArchiveContainer container) throws ModuleException;
}
