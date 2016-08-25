package com.databasepreservation.modules.siard.in.input;

import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARDImportDefault implements DatabaseImportModule {
  private final ReadStrategy readStrategy;
  private final SIARDArchiveContainer mainContainer;
  private final ContentImportStrategy contentStrategy;
  private final MetadataImportStrategy metadataStrategy;
  private ModuleSettings moduleSettings;

  public SIARDImportDefault(ContentImportStrategy contentStrategy, SIARDArchiveContainer mainContainer,
    ReadStrategy readStrategy, MetadataImportStrategy metadataStrategy) {
    this.readStrategy = readStrategy;
    this.mainContainer = mainContainer;
    this.contentStrategy = contentStrategy;
    this.metadataStrategy = metadataStrategy;
  }

  @Override
  public void getDatabase(DatabaseExportModule handler) throws ModuleException, UnknownTypeException,
    InvalidDataException {
    moduleSettings = handler.getModuleSettings();
    readStrategy.setup(mainContainer);
    handler.initDatabase();
    try {
      metadataStrategy.loadMetadata(readStrategy, mainContainer);

      DatabaseStructure dbStructure = metadataStrategy.getDatabaseStructure();

      // handler.setIgnoredSchemas(null);

      handler.handleStructure(dbStructure);

      contentStrategy.importContent(handler, mainContainer, dbStructure, moduleSettings);

      handler.finishDatabase();
    } finally {
      readStrategy.finish(mainContainer);
    }
  }
}
