/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.input;

import java.nio.file.Path;
import java.util.Map;

import com.databasepreservation.modules.siard.in.content.SIARD22ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD22MetadataImportStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.modules.siard.SIARD2ModuleFactory;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD2MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.content.SIARD20ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD20MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD21MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARD2ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipAndFolderReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipReadStrategy;
import com.databasepreservation.utils.MapUtils;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ImportModule {
  private static final String moduleName = "siard-2";
  private final ReadStrategy readStrategy;
  private final SIARDArchiveContainer mainContainer;
  private final SIARDArchiveContainer lobContainer;
  private final MetadataImportStrategy metadataStrategy;
  private final ContentImportStrategy contentStrategy;
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARD2ImportModule.class);

  public SIARD2ImportModule(Path siardPackage) {
    this(siardPackage, false, false);
  }

  public SIARD2ImportModule(Path siardPackage, boolean ignoreLobs) {
    this(siardPackage, false, ignoreLobs);
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
  public SIARD2ImportModule(Path siardPackagePath, boolean auxiliaryContainersInZipFormat, boolean ignoreLobs) {
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
    } catch (ModuleException e) {
      LOGGER.debug("Problem setting up container", e);
    }

    ContentPathImportStrategy contentPathStrategy = new SIARD2ContentPathImportStrategy();

    MetadataPathStrategy metadataPathStrategy = new SIARD2MetadataPathStrategy();
    switch (mainContainer.getVersion()) {
      case V2_0:
        contentStrategy = new SIARD20ContentImportStrategy(readStrategy, contentPathStrategy, lobContainer, ignoreLobs);
        metadataStrategy = new SIARD20MetadataImportStrategy(metadataPathStrategy, contentPathStrategy);
        break;
      case V2_1:
        contentStrategy = new SIARD20ContentImportStrategy(readStrategy, contentPathStrategy, lobContainer, ignoreLobs);
        metadataStrategy = new SIARD21MetadataImportStrategy(metadataPathStrategy, contentPathStrategy);
        break;
      case V2_2:
        contentStrategy = new SIARD22ContentImportStrategy(readStrategy, contentPathStrategy, lobContainer, ignoreLobs);
        metadataStrategy = new SIARD22MetadataImportStrategy(metadataPathStrategy, contentPathStrategy);
        break;

      default:
        contentStrategy = new SIARD20ContentImportStrategy(readStrategy, contentPathStrategy, lobContainer, ignoreLobs);
        metadataStrategy = null;
    }
  }

  public DatabaseImportModule getDatabaseImportModule() {
    final Map<String, String> properties = MapUtils.buildMapFromObjects(SIARD2ModuleFactory.PARAMETER_FILE,
      mainContainer.getPath().normalize().toAbsolutePath().toString());
    return new SIARDImportDefault(moduleName, contentStrategy, mainContainer, readStrategy, metadataStrategy,
      properties);
  }
}
