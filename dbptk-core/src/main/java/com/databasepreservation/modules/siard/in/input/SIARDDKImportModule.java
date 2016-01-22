package com.databasepreservation.modules.siard.in.input;

import java.util.Map;

import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.modules.siard.SIARDDKModuleFactory;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARDDKMetadataPathStrategy;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.content.SIARDDKContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARDDKMetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARDDKContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipReadStrategy;

/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 */
public class SIARDDKImportModule {

  private final ReadStrategy readStrategy;
  private final SIARDArchiveContainer mainContainer;
  private final MetadataImportStrategy metadataStrategy;
  private final ContentImportStrategy contentStrategy;

  protected final Map<Parameter, String> parameters;

  public SIARDDKImportModule(Map<Parameter, String> parameters) {
    readStrategy = new ZipReadStrategy();
    // TODO: Create new read strategy from SIARDDK
    this.parameters = parameters;
    String siardPackagePattern = this.parameters.get(SIARDDKModuleFactory.PARAM_IMPORT_FOLDER);
    // TODO:
    mainContainer = null; // new SIARDArchiveContainer(siardPackagePattern,
                          // SIARDArchiveContainer.OutputContainerType.MAIN);

    ContentPathImportStrategy contentPathStrategy = new SIARDDKContentPathImportStrategy();
    contentStrategy = new SIARDDKContentImportStrategy(readStrategy, contentPathStrategy);

    MetadataPathStrategy metadataPathStrategy = new SIARDDKMetadataPathStrategy();
    metadataStrategy = new SIARDDKMetadataImportStrategy(metadataPathStrategy, contentPathStrategy,
      this.parameters.get(SIARDDKModuleFactory.PARAM_IMPORT_AS_SCHEMA));

  }

  public DatabaseImportModule getDatabaseImportModule() {
    return new SIARDImportDefault(contentStrategy, mainContainer, readStrategy, metadataStrategy);
  }

}
