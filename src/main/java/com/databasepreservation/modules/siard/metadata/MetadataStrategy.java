package com.databasepreservation.modules.siard.metadata;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.path.PathStrategy;
import com.databasepreservation.modules.siard.write.OutputContainer;
import com.databasepreservation.modules.siard.write.WriteStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface MetadataStrategy {
	void writeMetadataXML(OutputContainer container) throws ModuleException;
	void writeMetadataXSD(OutputContainer container) throws ModuleException;
}
