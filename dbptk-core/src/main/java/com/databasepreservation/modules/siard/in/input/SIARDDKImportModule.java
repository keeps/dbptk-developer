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
import com.databasepreservation.modules.siard.in.path.SIARDDKContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.FolderReadStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 */
public class SIARDDKImportModule {

  protected final ReadStrategy readStrategy;
  protected final SIARDArchiveContainer mainContainer;
  protected final MetadataImportStrategy metadataStrategy;
  protected final ContentImportStrategy contentStrategy;

  public SIARDDKImportModule(Path siardPackage, String paramImportAsSchema) {
    mainContainer = new SIARDArchiveContainer(siardPackage, SIARDArchiveContainer.OutputContainerType.MAIN);
    readStrategy = new FolderReadStrategy(mainContainer);

    MetadataPathStrategy metadataPathStrategy = new SIARDDKMetadataPathStrategy();
    SIARDDKContentPathImportStrategy contentPathStrategy = new SIARDDKContentPathImportStrategy(mainContainer,
      readStrategy,
      metadataPathStrategy, paramImportAsSchema);
    metadataStrategy = new SIARDDKMetadataImportStrategy(metadataPathStrategy, contentPathStrategy,
      paramImportAsSchema);
    contentStrategy = new SIARDDKContentImportStrategy(readStrategy, contentPathStrategy, paramImportAsSchema);

  }

  public DatabaseImportModule getDatabaseImportModule() {
    return new SIARDImportDefault(contentStrategy, mainContainer, readStrategy, metadataStrategy);
  }

}
