package com.databasepreservation.modules.siard.in.metadata;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface MetadataImportStrategy {
	void readMetadata(SIARDArchiveContainer container) throws ModuleException;

	DatabaseStructure getDatabaseStructure() throws ModuleException;
}
