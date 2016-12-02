package com.databasepreservation.modules.siard.in.input;

import java.nio.file.Path;

import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARDDKMetadataPathStrategy;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.content.SIARDDKContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARDDKMetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ResourceFileIndexInputStreamStrategy;
import com.databasepreservation.modules.siard.in.path.SIARDDKPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.FolderReadStrategyMD5Sum;

/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 */
public class SIARDDKImportModule {

  protected final FolderReadStrategyMD5Sum readStrategy;
  protected final SIARDArchiveContainer mainContainer;
  protected final MetadataImportStrategy metadataStrategy;
  protected final ContentImportStrategy contentStrategy;

  public SIARDDKImportModule(Path siardPackage, String paramImportAsSchema) {
    mainContainer = new SIARDArchiveContainer(siardPackage.toAbsolutePath().normalize(),
      SIARDArchiveContainer.OutputContainerType.MAIN);
    readStrategy = new FolderReadStrategyMD5Sum(mainContainer);

    MetadataPathStrategy metadataPathStrategy = new SIARDDKMetadataPathStrategy();
    // Please notice, that the MetadataPathStrategy instance is wrapped into
    // the SIARDDKPathImportStrategy below.

    // NOTE: if we need to use the fileIndex.xsd from a given
    // "arkiverings version" then change
    // the FileIndexInputStreamStrategy to ArchiveFileIndexInputStreamStrategy
    SIARDDKPathImportStrategy pathStrategy = new SIARDDKPathImportStrategy(mainContainer, readStrategy,
      metadataPathStrategy, paramImportAsSchema, new ResourceFileIndexInputStreamStrategy());

    metadataStrategy = new SIARDDKMetadataImportStrategy(pathStrategy, paramImportAsSchema);
    contentStrategy = new SIARDDKContentImportStrategy(readStrategy, pathStrategy, paramImportAsSchema);

  }

  public DatabaseImportModule getDatabaseImportModule() {
    return new SIARDImportDefault(contentStrategy, mainContainer, readStrategy, metadataStrategy);
  }

}
