/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.modules.siard.out.content.LOBsTracker;
import com.databasepreservation.modules.siard.out.output.SIARDDK1007ExportModule;

/**
 * @author António Lindo <alindo@keep.pt>
 *
 */
public class SIARDDK1007MetadataExportStrategy extends SIARDDKMetadataExportStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDDK1007MetadataExportStrategy.class);

  public SIARDDK1007MetadataExportStrategy(SIARDDK1007ExportModule siarddk1007ExportModule) {
    super(siarddk1007ExportModule);
  }

  @Override
  IndexFileStrategy createSIARDDKTableIndexFileStrategy(LOBsTracker lobsTracker) {
    return new SIARDDK1007TableIndexFileStrategy(lobsTracker);
  }
}