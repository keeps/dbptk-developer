/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.output;

import java.nio.file.Path;
import java.util.HashMap;

import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD2MetadataPathStrategy;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;
import com.databasepreservation.modules.siard.out.content.SIARD2ContentExportStrategy;
import com.databasepreservation.modules.siard.out.content.SIARD2ContentWithExternalLobsExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARD2MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD2ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD2ContentWithExternalLobsPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.FolderWriteStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.modules.siard.out.write.ZipWithExternalLobsWriteStrategy;
import com.databasepreservation.modules.siard.out.write.ZipWriteStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ExportModule {
  private final SIARD2ContentPathExportStrategy contentPathStrategy;
  private final MetadataPathStrategy metadataPathStrategy;

  private final SIARDArchiveContainer mainContainer;
  private final WriteStrategy writeStrategy;

  private final Path tableFilter;

  private MetadataExportStrategy metadataStrategy;
  private ContentExportStrategy contentStrategy;

  private HashMap<String, String> descriptiveMetadata;

  public SIARD2ExportModule(Path siardPackage, boolean compressZip, boolean prettyXML, Path tableFilter,
    HashMap<String, String> descriptiveMetadata) {
    this.descriptiveMetadata = descriptiveMetadata;
    contentPathStrategy = new SIARD2ContentPathExportStrategy();
    metadataPathStrategy = new SIARD2MetadataPathStrategy();
    if (compressZip) {
      writeStrategy = new ZipWriteStrategy(ZipWriteStrategy.CompressionMethod.DEFLATE);
    } else {
      writeStrategy = new ZipWriteStrategy(ZipWriteStrategy.CompressionMethod.STORE);
    }
    mainContainer = new SIARDArchiveContainer(siardPackage, SIARDArchiveContainer.OutputContainerType.MAIN);

    metadataStrategy = new SIARD2MetadataExportStrategy(metadataPathStrategy, contentPathStrategy, false);
    contentStrategy = new SIARD2ContentExportStrategy(contentPathStrategy, writeStrategy, mainContainer, prettyXML);

    this.tableFilter = tableFilter;
  }

  public SIARD2ExportModule(Path siardPackage, boolean compressZip, boolean prettyXML, Path tableFilter,
    int externalLobsPerFolder, long externalLobsFolderSize, HashMap<String, String> descriptiveMetadata) {
    this.descriptiveMetadata = descriptiveMetadata;
    contentPathStrategy = new SIARD2ContentWithExternalLobsPathExportStrategy();
    metadataPathStrategy = new SIARD2MetadataPathStrategy();

    FolderWriteStrategy folderWriteStrategy = new FolderWriteStrategy();
    ZipWriteStrategy zipWriteStrategy;
    if (compressZip) {
      zipWriteStrategy = new ZipWriteStrategy(ZipWriteStrategy.CompressionMethod.DEFLATE);
    } else {
      zipWriteStrategy = new ZipWriteStrategy(ZipWriteStrategy.CompressionMethod.STORE);
    }
    writeStrategy = new ZipWithExternalLobsWriteStrategy(zipWriteStrategy, folderWriteStrategy);

    mainContainer = new SIARDArchiveContainer(siardPackage, SIARDArchiveContainer.OutputContainerType.MAIN);

    metadataStrategy = new SIARD2MetadataExportStrategy(metadataPathStrategy, contentPathStrategy, true);
    contentStrategy = new SIARD2ContentWithExternalLobsExportStrategy(contentPathStrategy, writeStrategy, mainContainer,
      prettyXML, externalLobsPerFolder, externalLobsFolderSize);

    this.tableFilter = tableFilter;
  }

  public DatabaseExportModule getDatabaseHandler() {
    return new SIARDExportDefault(contentStrategy, mainContainer, writeStrategy, metadataStrategy, tableFilter,
      descriptiveMetadata);
  }
}
