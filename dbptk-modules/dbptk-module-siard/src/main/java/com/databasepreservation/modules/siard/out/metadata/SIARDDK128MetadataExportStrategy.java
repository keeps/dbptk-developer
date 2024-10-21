/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.metadata;

import com.databasepreservation.modules.siard.out.content.LOBsTracker;
import com.databasepreservation.modules.siard.out.output.SIARDDK128ExportModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author António Lindo <alindo@keep.pt>
 *
 */
public class SIARDDK128MetadataExportStrategy extends SIARDDKMetadataExportStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDDK128MetadataExportStrategy.class);

  public SIARDDK128MetadataExportStrategy(SIARDDK128ExportModule siarddk128ExportModule) {
    super(siarddk128ExportModule);
  }

  @Override
  IndexFileStrategy createSIARDDKTableIndexFileStrategy(LOBsTracker lobsTracker) {
    return new SIARDDK128TableIndexFileStrategy(lobsTracker);
  }
}