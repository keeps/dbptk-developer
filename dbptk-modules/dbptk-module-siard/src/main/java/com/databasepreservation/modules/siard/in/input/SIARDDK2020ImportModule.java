/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.input;

import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.modules.siard.SIARDDK2020ModuleFactory;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARDDKMetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.content.SIARDDK2020ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARDDK2020MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ResourceFileIndexInputStreamStrategy;
import com.databasepreservation.modules.siard.in.path.SIARDDK2020PathImportStrategy;
import com.databasepreservation.modules.siard.in.read.FolderReadStrategyMD5Sum;
import com.databasepreservation.utils.MapUtils;

import java.nio.file.Path;
import java.util.Map;

/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 */
public class SIARDDK2020ImportModule {
  private static final String moduleName = "siard-dk-2020";
  protected final FolderReadStrategyMD5Sum readStrategy;
  protected final SIARDArchiveContainer mainContainer;
  protected final MetadataImportStrategy metadataStrategy;
  protected final ContentImportStrategy contentStrategy;

  private final String paramImportAsSchema;

  public SIARDDK2020ImportModule(Path siardPackage, String paramImportAsSchema) {
    this.paramImportAsSchema = paramImportAsSchema;
    mainContainer = new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK, siardPackage.toAbsolutePath().normalize(),
      SIARDArchiveContainer.OutputContainerType.MAIN);
    readStrategy = new FolderReadStrategyMD5Sum(mainContainer);

    MetadataPathStrategy metadataPathStrategy = new SIARDDKMetadataPathStrategy();
    // Please notice, that the MetadataPathStrategy instance is wrapped into
    // the SIARDDKPathImportStrategy below.

    // NOTE: if we need to use the fileIndex.xsd from a given
    // "arkiverings version" then change
    // the FileIndexInputStreamStrategy to ArchiveFileIndexInputStreamStrategy
    SIARDDK2020PathImportStrategy pathStrategy = new SIARDDK2020PathImportStrategy(mainContainer, readStrategy,
      metadataPathStrategy, paramImportAsSchema, new ResourceFileIndexInputStreamStrategy());

    metadataStrategy = new SIARDDK2020MetadataImportStrategy(pathStrategy, paramImportAsSchema);
    contentStrategy = new SIARDDK2020ContentImportStrategy(readStrategy, pathStrategy, paramImportAsSchema);

  }

  public DatabaseImportModule getDatabaseImportModule() {
    final Map<String, String> properties = MapUtils.buildMapFromObjects(SIARDDK2020ModuleFactory.PARAMETER_FOLDER,
      mainContainer.getPath().normalize().toAbsolutePath().toString(), SIARDDK2020ModuleFactory.PARAMETER_AS_SCHEMA,
      paramImportAsSchema);
    return new SIARDImportDefault(moduleName, contentStrategy, mainContainer, readStrategy, metadataStrategy,
      properties);
  }

}
