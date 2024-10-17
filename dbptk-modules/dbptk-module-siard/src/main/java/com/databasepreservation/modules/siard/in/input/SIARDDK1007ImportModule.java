/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.input;

import java.nio.file.Path;

import com.databasepreservation.modules.siard.SIARDDK1007ModuleFactory;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.SIARDDK1007MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.content.SIARDDK1007ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARDDK1007MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ResourceFileIndexInputStreamStrategy;
import com.databasepreservation.modules.siard.in.path.SIARDDK1007PathImportStrategy;
import com.databasepreservation.modules.siard.in.read.FolderReadStrategyMD5Sum;

/**
 * @author António Lindo <talindo@keep.pt>
 *
 */
public class SIARDDK1007ImportModule extends SIARDDKImportModule {
  private static final String moduleName = "siard-dk-1007";

  public SIARDDK1007ImportModule(Path siardPackage, String paramImportAsSchema) {
    super(moduleName, siardPackage, paramImportAsSchema,
      new SIARDDK1007PathImportStrategy(
        new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK, siardPackage.toAbsolutePath().normalize(),
          SIARDArchiveContainer.OutputContainerType.MAIN),
        new FolderReadStrategyMD5Sum(new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK,
          siardPackage.toAbsolutePath().normalize(), SIARDArchiveContainer.OutputContainerType.MAIN)),
        new SIARDDK1007MetadataPathStrategy(), paramImportAsSchema, new ResourceFileIndexInputStreamStrategy()));
  }

  @Override
  protected MetadataImportStrategy createMetadataImportStrategy() {
    return new SIARDDK1007MetadataImportStrategy((SIARDDK1007PathImportStrategy) pathStrategy, paramImportAsSchema);
  }

  @Override
  protected ContentImportStrategy createContentImportStrategy() {
    return new SIARDDK1007ContentImportStrategy(readStrategy, pathStrategy, paramImportAsSchema);
  }

  @Override
  protected String getModuleFactoryParameterFolder() {
    return SIARDDK1007ModuleFactory.PARAMETER_FOLDER;
  }

  @Override
  protected String getModuleFactoryParameterAsSchema() {
    return SIARDDK1007ModuleFactory.PARAMETER_AS_SCHEMA;
  }
}
