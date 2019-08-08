package com.databasepreservation.modules.siard.validate.metadata;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD2MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD20MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD21MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARD2ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipAndFolderReadStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataValidatorHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataValidatorHelper.class);

  public SIARDArchiveContainer getContainer() {
    return container;
  }

  private SIARDArchiveContainer container;
  private ReadStrategy readStrategy;
  private MetadataPathStrategy metadataPathStrategy;
  private ContentPathImportStrategy contentPathStrategy;
  private DatabaseStructure metadata = null;
  private Path SIARDPackagePath = null;
  private Reporter reporter;

  public MetadataValidatorHelper(Path SIARDPackagePath, Reporter reporter) {
    this.SIARDPackagePath = SIARDPackagePath;
    this.reporter = reporter;
  }

  public DatabaseStructure getMetadata() throws ModuleException {
    metadataPathStrategy = new SIARD2MetadataPathStrategy();
    contentPathStrategy = new SIARD2ContentPathImportStrategy();
    container = new SIARDArchiveContainer(SIARDPackagePath, SIARDArchiveContainer.OutputContainerType.MAIN);
    readStrategy = new ZipAndFolderReadStrategy(container);
    ModuleSettings moduleSettings = new ModuleSettings();

    try {
      readStrategy.setup(container);
    } catch (ModuleException e) {
      LOGGER.debug("Problem setting up container", e);
    }

    MetadataImportStrategy metadataImportStrategy;
    switch (container.getVersion()) {
      case V2_0:
        metadataImportStrategy = new SIARD20MetadataImportStrategy(metadataPathStrategy, contentPathStrategy);
        break;
      case V2_1:
        metadataImportStrategy = new SIARD21MetadataImportStrategy(metadataPathStrategy, contentPathStrategy);
        break;
      case DK:
      case V1_0:
      default:
        throw new ModuleException().withMessage("Metadata editing only supports SIARD 2 version");
    }

    metadataImportStrategy.setOnceReporter(reporter);

    try {
      metadataImportStrategy.loadMetadata(readStrategy, container, moduleSettings);

      metadata = metadataImportStrategy.getDatabaseStructure();
    } catch (NullPointerException e) {
      throw new ModuleException().withMessage("Metadata editing only supports SIARD 2 version").withCause(e);
    } finally {
      readStrategy.finish(container);
    }

    return metadata;
  }
}
