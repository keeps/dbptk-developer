package com.databasepreservation.modules.siard.out.output;

import com.databasepreservation.modules.DatabaseExportModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD2MetadataPathStrategy;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;
import com.databasepreservation.modules.siard.out.content.SIARD2ContentExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARD2MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD2ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.modules.siard.out.write.ZipWriteStrategy;

import java.nio.file.Path;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ExportModule {
        private final ContentPathExportStrategy contentPathStrategy;
        private final MetadataPathStrategy metadataPathStrategy;

        private final SIARDArchiveContainer mainContainer;
        private final WriteStrategy writeStrategy;

        private MetadataExportStrategy metadataStrategy;
        private ContentExportStrategy contentStrategy;

        public SIARD2ExportModule(Path siardPackage, boolean compressZip) {
                contentPathStrategy = new SIARD2ContentPathExportStrategy();
                metadataPathStrategy = new SIARD2MetadataPathStrategy();
                if (compressZip) {
                        writeStrategy = new ZipWriteStrategy(ZipWriteStrategy.CompressionMethod.DEFLATE);
                } else {
                        writeStrategy = new ZipWriteStrategy(ZipWriteStrategy.CompressionMethod.STORE);
                }
                mainContainer = new SIARDArchiveContainer(siardPackage, SIARDArchiveContainer.OutputContainerType.MAIN);

                metadataStrategy = new SIARD2MetadataExportStrategy(metadataPathStrategy, contentPathStrategy);
                //TODO: change prettyXML from 'true' to a module argument
                contentStrategy = new SIARD2ContentExportStrategy(contentPathStrategy, writeStrategy, mainContainer,
                  true);
        }

        public DatabaseExportModule getDatabaseHandler() {
                return new SIARDExportDefault(contentStrategy, mainContainer, writeStrategy, metadataStrategy);
        }
}
