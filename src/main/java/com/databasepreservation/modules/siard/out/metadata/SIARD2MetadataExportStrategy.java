package com.databasepreservation.modules.siard.out.metadata;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2MetadataExportStrategy implements MetadataExportStrategy {
        private final ContentPathExportStrategy contentPathStrategy;
        private final MetadataPathStrategy metadataPathStrategy;

        public SIARD2MetadataExportStrategy(MetadataPathStrategy metadataPathStrategy,
          ContentPathExportStrategy paths) {
                this.contentPathStrategy = paths;
                this.metadataPathStrategy = metadataPathStrategy;
        }

        @Override public void writeMetadataXML(DatabaseStructure databaseStructure, SIARDArchiveContainer container,
          WriteStrategy writeStrategy) throws ModuleException {

        }

        @Override public void writeMetadataXSD(DatabaseStructure databaseStructure, SIARDArchiveContainer container,
          WriteStrategy writeStrategy) throws ModuleException {

        }
}
