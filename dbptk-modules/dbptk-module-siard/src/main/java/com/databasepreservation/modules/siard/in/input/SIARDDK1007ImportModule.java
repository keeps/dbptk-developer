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
import com.databasepreservation.modules.siard.SIARDDK1007ModuleFactory;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARDDK1007MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.content.SIARDDK1007ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARDDK1007MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ResourceFileIndexInputStreamStrategy;
import com.databasepreservation.modules.siard.in.path.SIARDDK1007PathImportStrategy;
import com.databasepreservation.modules.siard.in.read.FolderReadStrategyMD5Sum;
import com.databasepreservation.utils.MapUtils;

/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 */
public class SIARDDK1007ImportModule {
  private static final String moduleName = "siard-dk-1007";
  protected final FolderReadStrategyMD5Sum readStrategy;
  protected final SIARDArchiveContainer mainContainer;
  protected final MetadataImportStrategy metadataStrategy;
  protected final ContentImportStrategy contentStrategy;

  private final String paramImportAsSchema;

  public SIARDDK1007ImportModule(Path siardPackage, String paramImportAsSchema) {
    this.paramImportAsSchema = paramImportAsSchema;
    mainContainer = new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK, siardPackage.toAbsolutePath().normalize(),
      SIARDArchiveContainer.OutputContainerType.MAIN);
    readStrategy = new FolderReadStrategyMD5Sum(mainContainer);

    MetadataPathStrategy metadataPathStrategy = new SIARDDK1007MetadataPathStrategy();
    // Please notice, that the MetadataPathStrategy instance is wrapped into
    // the SIARDDKPathImportStrategy below.

    // NOTE: if we need to use the fileIndex.xsd from a given
    // "arkiverings version" then change
    // the FileIndexInputStreamStrategy to ArchiveFileIndexInputStreamStrategy
    SIARDDK1007PathImportStrategy pathStrategy = new SIARDDK1007PathImportStrategy(mainContainer, readStrategy,
      metadataPathStrategy, paramImportAsSchema, new ResourceFileIndexInputStreamStrategy());

    metadataStrategy = new SIARDDK1007MetadataImportStrategy(pathStrategy, paramImportAsSchema);
    contentStrategy = new SIARDDK1007ContentImportStrategy(readStrategy, pathStrategy, paramImportAsSchema);

  }

  public DatabaseImportModule getDatabaseImportModule() {
    final Map<String, String> properties = MapUtils.buildMapFromObjects(SIARDDK1007ModuleFactory.PARAMETER_FOLDER,
      mainContainer.getPath().normalize().toAbsolutePath().toString(), SIARDDK1007ModuleFactory.PARAMETER_AS_SCHEMA,
      paramImportAsSchema);
    return new SIARDImportDefault(moduleName, contentStrategy, mainContainer, readStrategy, metadataStrategy,
      properties);
  }

}
