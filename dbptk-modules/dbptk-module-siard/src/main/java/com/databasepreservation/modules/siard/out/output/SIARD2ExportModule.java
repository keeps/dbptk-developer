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

import com.databasepreservation.common.compression.CompressionMethod;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD2MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;
import com.databasepreservation.modules.siard.out.content.SIARD20ContentExportStrategy;
import com.databasepreservation.modules.siard.out.content.SIARD20ContentWithExternalLobsExportStrategy;
import com.databasepreservation.modules.siard.out.content.SIARD22ContentExportStrategy;
import com.databasepreservation.modules.siard.out.content.SIARD22ContentWithExternalLobsExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARD20MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARD21MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARD22MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD20ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD20ContentWithExternalLobsPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD22ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD22ContentWithExternalLobsPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.FolderWriteStrategy;
import com.databasepreservation.modules.siard.out.write.ParallelZipWriteStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.modules.siard.out.write.ZipWithExternalLobsWriteStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ExportModule {
  private ContentPathExportStrategy contentPathStrategy;
  private final MetadataPathStrategy metadataPathStrategy;

  private final SIARDArchiveContainer mainContainer;
  private final WriteStrategy writeStrategy;

  private MetadataExportStrategy metadataStrategy;
  private ContentExportStrategy contentStrategy;

  private HashMap<String, String> descriptiveMetadata;

  public SIARD2ExportModule(SIARDConstants.SiardVersion version, Path siardPackage, boolean compressZip,
    boolean prettyXML, HashMap<String, String> descriptiveMetadata, String digestAlgorithm, String fontCase) {
    this.descriptiveMetadata = descriptiveMetadata;
    metadataPathStrategy = new SIARD2MetadataPathStrategy();
    if (compressZip) {
      writeStrategy = new ParallelZipWriteStrategy(CompressionMethod.DEFLATE);
    } else {
      writeStrategy = new ParallelZipWriteStrategy(CompressionMethod.STORE);
    }
    mainContainer = new SIARDArchiveContainer(version, siardPackage, SIARDArchiveContainer.OutputContainerType.MAIN);

    switch (version) {
      case V2_0:
        contentPathStrategy = new SIARD20ContentPathExportStrategy();
        metadataStrategy = new SIARD20MetadataExportStrategy(metadataPathStrategy, contentPathStrategy, false);
        contentStrategy = new SIARD20ContentExportStrategy(contentPathStrategy, writeStrategy, mainContainer, prettyXML,
          digestAlgorithm, fontCase);
        break;
      case V2_1:
        contentPathStrategy = new SIARD20ContentPathExportStrategy();
        metadataStrategy = new SIARD21MetadataExportStrategy(metadataPathStrategy, contentPathStrategy, false);
        contentStrategy = new SIARD20ContentExportStrategy(contentPathStrategy, writeStrategy, mainContainer, prettyXML,
          digestAlgorithm, fontCase);
        break;
      case V2_2:
        contentPathStrategy = new SIARD22ContentPathExportStrategy();
        metadataStrategy = new SIARD22MetadataExportStrategy(metadataPathStrategy, contentPathStrategy, false);
        contentStrategy = new SIARD22ContentExportStrategy(contentPathStrategy, writeStrategy, mainContainer, prettyXML,
          digestAlgorithm, fontCase);
        break;
    }
  }

  public SIARD2ExportModule(SIARDConstants.SiardVersion version, Path siardPackage, boolean compressZip,
    boolean prettyXML, int externalLobsPerFolder, long externalLobsFolderSize, long externalLobsBLOBThresholdLimit,
    long externalLobsCLOBThresholdLimit, HashMap<String, String> descriptiveMetadata, String digestAlgorithm,
    String fontCase) {
    this.descriptiveMetadata = descriptiveMetadata;
    metadataPathStrategy = new SIARD2MetadataPathStrategy();

    FolderWriteStrategy folderWriteStrategy = new FolderWriteStrategy();
    ParallelZipWriteStrategy zipWriteStrategy;
    if (compressZip) {
      zipWriteStrategy = new ParallelZipWriteStrategy(CompressionMethod.DEFLATE);
    } else {
      zipWriteStrategy = new ParallelZipWriteStrategy(CompressionMethod.STORE);
    }
    writeStrategy = new ZipWithExternalLobsWriteStrategy(zipWriteStrategy, folderWriteStrategy, digestAlgorithm);

    mainContainer = new SIARDArchiveContainer(version, siardPackage, SIARDArchiveContainer.OutputContainerType.MAIN);

    switch (version) {
      case V2_0:
        contentPathStrategy = new SIARD20ContentWithExternalLobsPathExportStrategy();
        metadataStrategy = new SIARD20MetadataExportStrategy(metadataPathStrategy, contentPathStrategy, true);
        contentStrategy = new SIARD20ContentWithExternalLobsExportStrategy(contentPathStrategy, writeStrategy,
          mainContainer, prettyXML, externalLobsPerFolder, externalLobsFolderSize, externalLobsBLOBThresholdLimit,
          externalLobsCLOBThresholdLimit, digestAlgorithm, fontCase);
        break;
      case V2_1:
        contentPathStrategy = new SIARD20ContentWithExternalLobsPathExportStrategy();
        metadataStrategy = new SIARD21MetadataExportStrategy(metadataPathStrategy, contentPathStrategy, true);
        contentStrategy = new SIARD20ContentWithExternalLobsExportStrategy(contentPathStrategy, writeStrategy,
          mainContainer, prettyXML, externalLobsPerFolder, externalLobsFolderSize, externalLobsBLOBThresholdLimit,
          externalLobsCLOBThresholdLimit, digestAlgorithm, fontCase);
        break;
      case V2_2:
        contentPathStrategy = new SIARD22ContentWithExternalLobsPathExportStrategy();
        metadataStrategy = new SIARD22MetadataExportStrategy(metadataPathStrategy, contentPathStrategy, true);
        contentStrategy = new SIARD22ContentWithExternalLobsExportStrategy(contentPathStrategy, writeStrategy,
          mainContainer, prettyXML, externalLobsPerFolder, externalLobsFolderSize, externalLobsBLOBThresholdLimit,
          externalLobsCLOBThresholdLimit, digestAlgorithm, fontCase);
        break;
    }
  }

  public DatabaseFilterModule getDatabaseHandler() {
    return new SIARDExportDefault(contentStrategy, mainContainer, writeStrategy, metadataStrategy, descriptiveMetadata);
  }
}
