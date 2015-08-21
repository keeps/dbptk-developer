package com.databasepreservation.modules.siard.in.input;

import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

import java.nio.file.Path;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ImportModule {
        private final ReadStrategy readStrategy;
        private final SIARDArchiveContainer mainContainer;
        private final MetadataImportStrategy metadataStrategy;
        private final ContentImportStrategy contentStrategy;

        public SIARD2ImportModule(Path siardPackage) {
                readStrategy = null;//new ZipReadStrategy();
                mainContainer = null;//new SIARDArchiveContainer(siardPackage, SIARDArchiveContainer.OutputContainerType.MAIN);

                ContentPathImportStrategy contentPathStrategy = null;//new SIARD2ContentPathImportStrategy();
                contentStrategy = null;//new SIARD2ContentImportStrategy(readStrategy, contentPathStrategy);

                MetadataPathStrategy metadataPathStrategy = null;//new SIARD2MetadataPathStrategy();
                metadataStrategy = null;//new SIARD2MetadataImportStrategy(metadataPathStrategy, contentPathStrategy);
        }

        public DatabaseImportModule getDatabaseImportModule() {
                return new SIARDImportDefault(contentStrategy, mainContainer, readStrategy, metadataStrategy);
        }
}
