/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.output;

import java.nio.file.Path;
import java.util.Map;

import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD1MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
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

  private final Path tableFilter;

  private MetadataExportStrategy metadataStrategy;
  private ContentExportStrategy contentStrategy;

  private Map<String, String> descriptiveMetadata;

  public SIARD1ExportModule(Path siardPackage, boolean compressZip, boolean prettyXML, Path tableFilter,
    Map<String, String> descriptiveMetadata) {
    this.descriptiveMetadata = descriptiveMetadata;
    contentPathStrategy = new SIARD1ContentPathExportStrategy();
    metadataPathStrategy = new SIARD1MetadataPathStrategy();
    if (compressZip) {
      writeStrategy = new ZipWriteStrategy(ZipWriteStrategy.CompressionMethod.DEFLATE);
    } else {
      writeStrategy = new ZipWriteStrategy(ZipWriteStrategy.CompressionMethod.STORE);
    }
    mainContainer = new SIARDArchiveContainer(SIARDConstants.SiardVersion.V1_0, siardPackage,
      SIARDArchiveContainer.OutputContainerType.MAIN);

    metadataStrategy = new SIARD1MetadataExportStrategy(metadataPathStrategy, contentPathStrategy);
    contentStrategy = new SIARD1ContentExportStrategy(contentPathStrategy, writeStrategy, mainContainer, prettyXML);

    this.tableFilter = tableFilter;
  }

  public DatabaseExportModule getDatabaseHandler() {
    return new SIARDExportDefault(contentStrategy, mainContainer, writeStrategy, metadataStrategy, tableFilter,
      descriptiveMetadata);
  }
}
