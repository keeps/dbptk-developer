/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.common.path;

import java.security.InvalidParameterException;

import com.databasepreservation.modules.siard.constants.SIARDDKConstants;

/**
 * @author António Lindo <alindo@keep.pt>
 */
public class SIARDDK1007MetadataPathStrategy extends SIARDDKMetadataPathStrategy {

  @Override
  public String getXsdResourcePath(String filename) throws InvalidParameterException {

    if (checkFilename(filename)) {
      return new StringBuilder().append(SIARDDKConstants.RESOURCE_FILE_SEPARATOR)
        .append(SIARDDKConstants.SCHEMA_RESOURCE_DIR).append(SIARDDKConstants.RESOURCE_FILE_SEPARATOR)
        .append(SIARDDKConstants.SIARDDK_1007).append(SIARDDKConstants.RESOURCE_FILE_SEPARATOR).append(filename)
        .append(SIARDDKConstants.FILE_EXTENSION_SEPARATOR).append(SIARDDKConstants.XSD_EXTENSION).toString();
    } else {
      throw new InvalidParameterException("Invalid filename for metadata file");
    }
  }

}
