package com.databasepreservation.modules.siard.in.input;

import java.nio.file.Path;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD2MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.content.SIARD2ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD2MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARD2ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipAndFolderReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipReadStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ImportModule {
  private final ReadStrategy readStrategy;
  private final SIARDArchiveContainer mainContainer;
  private final SIARDArchiveContainer lobContainer;
  private final MetadataImportStrategy metadataStrategy;
  private final ContentImportStrategy contentStrategy;

  public SIARD2ImportModule(Path siardPackage) {
    this(siardPackage, false);
  }

  /**
   * Constructor used to initialize required objects to get a database import
   * module for SIARD 2 (all minor versions)
   *
   * @param siardPackagePath
   *          Path to the main SIARD file (file with extension .siard)
   * @param auxiliaryContainersInZipFormat
   *          (optional) In some SIARD2 archives, LOBs are saved outside the main
   *          SIARD archive container. These LOBs may be saved in a ZIP or simply
   *          saved to folders. When reading those LOBs it's important to know if
   *          they are inside a simple folder or a zip container.
   */
  public SIARD2ImportModule(Path siardPackagePath, boolean auxiliaryContainersInZipFormat) {
    Path siardPackageNormalizedPath = siardPackagePath.toAbsolutePath().normalize();
    mainContainer = new SIARDArchiveContainer(siardPackageNormalizedPath,
      SIARDArchiveContainer.OutputContainerType.MAIN);
    lobContainer = new SIARDArchiveContainer(siardPackageNormalizedPath.getParent(),
      SIARDArchiveContainer.OutputContainerType.AUXILIARY);
    if (auxiliaryContainersInZipFormat) {
      readStrategy = new ZipReadStrategy();
    } else {
      readStrategy = new ZipAndFolderReadStrategy(mainContainer);
    }

    // identify version before creating metadata/content import strategy instances
    try {
      readStrategy.setup(mainContainer);
      mainContainer.setVersion(MetadataPathStrategy.VersionIdentifier.getVersion(readStrategy, mainContainer));
    } catch (ModuleException e) {
      // do nothing and let it fail later
    }

    ContentPathImportStrategy contentPathStrategy = new SIARD2ContentPathImportStrategy();
    contentStrategy = new SIARD2ContentImportStrategy(readStrategy, contentPathStrategy, lobContainer);

    MetadataPathStrategy metadataPathStrategy = new SIARD2MetadataPathStrategy();
    metadataStrategy = new SIARD2MetadataImportStrategy(metadataPathStrategy, contentPathStrategy);
  }

  public DatabaseImportModule getDatabaseImportModule() {
    return new SIARDImportDefault(contentStrategy, mainContainer, readStrategy, metadataStrategy);
  }
}
