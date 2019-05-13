/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.input;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.DefaultExceptionNormalizer;
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
  private Reporter reporter;
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDImportDefault.class);

  public SIARDImportDefault(ContentImportStrategy contentStrategy, SIARDArchiveContainer mainContainer,
    ReadStrategy readStrategy, MetadataImportStrategy metadataStrategy) {
    this.readStrategy = readStrategy;
    this.mainContainer = mainContainer;
    this.contentStrategy = contentStrategy;
    this.metadataStrategy = metadataStrategy;
  }

  @Override
  public DatabaseExportModule migrateDatabaseTo(DatabaseExportModule handler) throws ModuleException {
    moduleSettings = handler.getModuleSettings();
    readStrategy.setup(mainContainer);
    LOGGER.info("Importing SIARD version {}", mainContainer.getVersion().getDisplayName());
    handler.initDatabase();
    try {
      metadataStrategy.loadMetadata(readStrategy, mainContainer, moduleSettings);

      DatabaseStructure dbStructure = metadataStrategy.getDatabaseStructure();

      // handler.setIgnoredSchemas(null);

      handler.handleStructure(dbStructure);

      contentStrategy.importContent(handler, mainContainer, dbStructure, moduleSettings);

      handler.finishDatabase();
    } finally {
      readStrategy.finish(mainContainer);
    }
    return null;
  }

  /**
   * Provide a reporter through which potential conversion problems should be
   * reported. This reporter should be provided only once for the export module
   * instance.
   *
   * @param reporter
   *          The initialized reporter instance.
   */
  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
    metadataStrategy.setOnceReporter(reporter);
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    return DefaultExceptionNormalizer.getInstance().normalizeException(exception, contextMessage);
  }
}
