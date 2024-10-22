/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.input;

import com.databasepreservation.modules.siard.SIARDDK128ModuleFactory;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.SIARDDK128MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.content.SIARDDK128ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARDDK128MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ResourceFileIndexInputStreamStrategy;
import com.databasepreservation.modules.siard.in.path.SIARDDK128PathImportStrategy;
import com.databasepreservation.modules.siard.in.read.FolderReadStrategyMD5Sum;

import java.nio.file.Path;

/**
 * @author António Lindo <talindo@keep.pt>
 *
 */
public class SIARDDK128ImportModule extends SIARDDKImportModule {
  private static final String moduleName = "siard-dk-128";

  public SIARDDK128ImportModule(Path siardPackage, String paramImportAsSchema) {
    super(moduleName, siardPackage, paramImportAsSchema,
      new SIARDDK128PathImportStrategy(
        new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK, siardPackage.toAbsolutePath().normalize(),
          SIARDArchiveContainer.OutputContainerType.MAIN),
        new FolderReadStrategyMD5Sum(new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK,
          siardPackage.toAbsolutePath().normalize(), SIARDArchiveContainer.OutputContainerType.MAIN)),
        new SIARDDK128MetadataPathStrategy(), paramImportAsSchema, new ResourceFileIndexInputStreamStrategy()));
  }

  @Override
  protected MetadataImportStrategy createMetadataImportStrategy() {
    return new SIARDDK128MetadataImportStrategy((SIARDDK128PathImportStrategy) pathStrategy, paramImportAsSchema);
  }

  @Override
  protected ContentImportStrategy createContentImportStrategy() {
    return new SIARDDK128ContentImportStrategy(readStrategy, pathStrategy, paramImportAsSchema);
  }

  @Override
  protected String getModuleFactoryParameterFolder() {
    return SIARDDK128ModuleFactory.PARAMETER_FOLDER;
  }

  @Override
  protected String getModuleFactoryParameterAsSchema() {
    return SIARDDK128ModuleFactory.PARAMETER_AS_SCHEMA;
  }
}
