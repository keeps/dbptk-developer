package com.databasepreservation.modules.siard.out.metadata;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface MetadataExportStrategy {
        void writeMetadataXML(DatabaseStructure databaseStructure, SIARDArchiveContainer container,
          WriteStrategy writeStrategy) throws ModuleException;

        void writeMetadataXSD(DatabaseStructure databaseStructure, SIARDArchiveContainer container,
          WriteStrategy writeStrategy) throws ModuleException;
}
