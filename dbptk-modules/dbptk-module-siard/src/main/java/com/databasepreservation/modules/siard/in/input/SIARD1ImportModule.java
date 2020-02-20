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

import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.modules.siard.SIARD1ModuleFactory;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD1MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.content.SIARD1ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD1MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARD1ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipReadStrategy;
import com.databasepreservation.utils.MapUtils;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD1ImportModule {
  private static final String moduleName = "siard-1";
  private final ReadStrategy readStrategy;
  private final SIARDArchiveContainer mainContainer;
  private final MetadataImportStrategy metadataStrategy;
  private final ContentImportStrategy contentStrategy;

  public SIARD1ImportModule(Path siardPackagePath) {
    Path siardPackageNormalizedPath = siardPackagePath.toAbsolutePath().normalize();
    readStrategy = new ZipReadStrategy();
    mainContainer = new SIARDArchiveContainer(SIARDConstants.SiardVersion.V1_0, siardPackageNormalizedPath,
      SIARDArchiveContainer.OutputContainerType.MAIN);

    ContentPathImportStrategy contentPathStrategy = new SIARD1ContentPathImportStrategy();
    contentStrategy = new SIARD1ContentImportStrategy(readStrategy, contentPathStrategy);

    MetadataPathStrategy metadataPathStrategy = new SIARD1MetadataPathStrategy();
    metadataStrategy = new SIARD1MetadataImportStrategy(metadataPathStrategy, contentPathStrategy);
  }

  public DatabaseImportModule getDatabaseImportModule() {
    final Map<String, String> properties = MapUtils.buildMapFromObjects(SIARD1ModuleFactory.PARAMETER_FILE,
      mainContainer.getPath().normalize().toAbsolutePath().toString());
    return new SIARDImportDefault(moduleName, contentStrategy, mainContainer, readStrategy, metadataStrategy,
      properties);
  }
}
