package com.databasepreservation.modules.siard.in.metadata;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface MetadataImportStrategy {
        void loadMetadata(ReadStrategy readStrategy, SIARDArchiveContainer container) throws ModuleException;

        DatabaseStructure getDatabaseStructure() throws ModuleException;
}
