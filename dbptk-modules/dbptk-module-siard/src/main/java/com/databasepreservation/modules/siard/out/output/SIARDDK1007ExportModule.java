/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
/**
 * Factory for setting up SIARDDK strategies
 * 
 * @author Andreas Kring <andreas@magenta.dk>
 * 
 */

package com.databasepreservation.modules.siard.out.output;

import java.util.Map;

import com.databasepreservation.modules.siard.common.adapters.SIARDDK1007Adapter;
import com.databasepreservation.modules.siard.common.path.SIARDDK1007MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARDDKMetadataPathStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK1007FileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK1007DocIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDKDocIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDKFileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDKMetadataExportStrategy;

/**
 * @author António Lindo <alindo@keep.pt>
 *
 */
public class SIARDDK1007ExportModule extends SIARDDKExportModule {

  public SIARDDK1007ExportModule(Map<String, String> exportModuleArgs) {
    super(exportModuleArgs);
  }

  @Override
  SIARDDKFileIndexFileStrategy createSIARDDKFileIndexFileStrategyInstance() {
    return new SIARDDK1007FileIndexFileStrategy();
  }

  @Override
  SIARDDKDocIndexFileStrategy createSIARDDKDocIndexFileStrategyInstance() {
    return new SIARDDK1007DocIndexFileStrategy();
  }

  @Override
  SIARDDKMetadataPathStrategy createSIARDDKMetadataPathStrategyInstance() {
    return new SIARDDK1007MetadataPathStrategy();
  }

  @Override
  SIARDDKMetadataExportStrategy createSIARDDKMetadataExportStrategyInstance() {
    return new SIARDDKMetadataExportStrategy(this, new SIARDDK1007Adapter());
  }

  @Override
  SIARDDKDatabaseExportModule createSIARDDKDatabaseExportModule() {
    return new SIARDDK1007DatabaseExportModule(this);
  }
}
