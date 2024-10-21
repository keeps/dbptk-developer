/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.output;

import com.databasepreservation.modules.siard.constants.SIARDDKConstants;

/**
 * @author António Lindo <alindo@keep.pt>
 *
 */
public class SIARDDK128DatabaseExportModule extends SIARDDKDatabaseExportModule {

  public SIARDDK128DatabaseExportModule(SIARDDKExportModule siarddkExportModule) {
    super(siarddkExportModule);
  }

  @Override
  String getJAXBContext() {
    return SIARDDKConstants.JAXB_CONTEXT_FILEINDEX_128;
  }
}
