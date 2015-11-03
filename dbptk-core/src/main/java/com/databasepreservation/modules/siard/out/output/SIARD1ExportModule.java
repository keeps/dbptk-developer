package com.databasepreservation.modules.siard.out.output;

import java.nio.file.Path;

import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD1MetadataPathStrategy;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;
import com.databasepreservation.modules.siard.out.content.SIARD1ContentExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARD1MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD1ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.modules.siard.out.write.ZipWriteStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD1ExportModule {
  private final ContentPathExportStrategy contentPathStrategy;
  private final MetadataPathStrategy metadataPathStrategy;

  private final SIARDArchiveContainer mainContainer;
  private final WriteStrategy writeStrategy;

  private MetadataExportStrategy metadataStrategy;
  private ContentExportStrategy contentStrategy;

  public SIARD1ExportModule(Path siardPackage, boolean compressZip, boolean prettyXML) {
    contentPathStrategy = new SIARD1ContentPathExportStrategy();
    metadataPathStrategy = new SIARD1MetadataPathStrategy();
    if (compressZip) {
      writeStrategy = new ZipWriteStrategy(ZipWriteStrategy.CompressionMethod.DEFLATE);
    } else {
      writeStrategy = new ZipWriteStrategy(ZipWriteStrategy.CompressionMethod.STORE);
    }
    mainContainer = new SIARDArchiveContainer(siardPackage, SIARDArchiveContainer.OutputContainerType.MAIN);

    metadataStrategy = new SIARD1MetadataExportStrategy(metadataPathStrategy, contentPathStrategy);
    contentStrategy = new SIARD1ContentExportStrategy(contentPathStrategy, writeStrategy, mainContainer, prettyXML);
  }

  public DatabaseExportModule getDatabaseHandler() {
    return new SIARDExportDefault(contentStrategy, mainContainer, writeStrategy, metadataStrategy);
  }
}
