/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.input;

import java.nio.file.Path;

import com.databasepreservation.modules.siard.SIARDDK1007ExtModuleFactory;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.SIARDDK1007MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.content.SIARDDK1007ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARDDK1007ExtMetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ResourceFileIndexInputStreamStrategy;
import com.databasepreservation.modules.siard.in.path.SIARDDK1007ExtPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.FolderReadStrategyMD5Sum;

/**
 * @author Alexandre Flores <aflores@keep.pt>
 *
 */
public class SIARDDK1007ExtImportModule extends SIARDDKImportModule {
  private static final String moduleName = "siard-dk-1007-ext";

  public SIARDDK1007ExtImportModule(Path siardPackage, String paramImportAsSchema) {
    super(moduleName, siardPackage, paramImportAsSchema,
      new SIARDDK1007ExtPathImportStrategy(
        new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK, siardPackage.toAbsolutePath().normalize(),
          SIARDArchiveContainer.OutputContainerType.MAIN),
        new FolderReadStrategyMD5Sum(new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK,
          siardPackage.toAbsolutePath().normalize(), SIARDArchiveContainer.OutputContainerType.MAIN)),
        new SIARDDK1007MetadataPathStrategy(), paramImportAsSchema, new ResourceFileIndexInputStreamStrategy()));
  }

  @Override
  protected MetadataImportStrategy createMetadataImportStrategy() {
    return new SIARDDK1007ExtMetadataImportStrategy((SIARDDK1007ExtPathImportStrategy) pathStrategy,
      paramImportAsSchema);
  }

  @Override
  protected ContentImportStrategy createContentImportStrategy() {
    return new SIARDDK1007ContentImportStrategy(readStrategy, pathStrategy, paramImportAsSchema);
  }

  @Override
  protected String getModuleFactoryParameterFolder() {
    return SIARDDK1007ExtModuleFactory.PARAMETER_FOLDER;
  }

  @Override
  protected String getModuleFactoryParameterAsSchema() {
    return SIARDDK1007ExtModuleFactory.PARAMETER_AS_SCHEMA;
  }
}
