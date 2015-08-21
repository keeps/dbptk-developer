package com.databasepreservation.modules.siard.in.metadata;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2MetadataImportStrategy implements MetadataImportStrategy {
        private final MetadataPathStrategy metadataPathStrategy;
        private final ContentPathImportStrategy contentPathStrategy;

        public SIARD2MetadataImportStrategy(MetadataPathStrategy metadataPathStrategy,
          ContentPathImportStrategy contentPathImportStrategy) {
                this.metadataPathStrategy = metadataPathStrategy;
                this.contentPathStrategy = contentPathImportStrategy;
        }

        @Override public void loadMetadata(ReadStrategy readStrategy, SIARDArchiveContainer container)
          throws ModuleException {

        }

        @Override public DatabaseStructure getDatabaseStructure() throws ModuleException {
                return null;
        }
}
