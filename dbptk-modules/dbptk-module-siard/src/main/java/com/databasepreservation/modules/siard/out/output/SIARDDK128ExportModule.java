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

import com.databasepreservation.modules.siard.common.adapters.SIARDDK128Adapter;
import com.databasepreservation.modules.siard.common.path.SIARDDK128MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARDDKMetadataPathStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK128DocIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK128FileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDKDocIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDKFileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDKMetadataExportStrategy;
import java.util.Map;

/**
 * @author António Lindo <alindo@keep.pt>
 *
 */
public class SIARDDK128ExportModule extends SIARDDKExportModule {

  public SIARDDK128ExportModule(Map<String, String> exportModuleArgs) {
    super(exportModuleArgs);
  }

  @Override
  SIARDDKFileIndexFileStrategy createSIARDDKFileIndexFileStrategyInstance() {
    return new SIARDDK128FileIndexFileStrategy();
  }

  @Override
  SIARDDKDocIndexFileStrategy createSIARDDKDocIndexFileStrategyInstance() {
    return new SIARDDK128DocIndexFileStrategy();
  }

  @Override
  SIARDDKMetadataPathStrategy createSIARDDKMetadataPathStrategyInstance() {
    return new SIARDDK128MetadataPathStrategy();
  }

  @Override
  SIARDDKMetadataExportStrategy createSIARDDKMetadataExportStrategyInstance() {
    return new SIARDDKMetadataExportStrategy(this, new SIARDDK128Adapter());
  }

  @Override
  SIARDDKDatabaseExportModule createSIARDDKDatabaseExportModule() {
    return new SIARDDK128DatabaseExportModule(this);
  }
}

