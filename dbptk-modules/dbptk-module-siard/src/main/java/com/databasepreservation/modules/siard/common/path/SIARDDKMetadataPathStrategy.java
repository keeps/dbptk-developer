package com.databasepreservation.modules.siard.common.path;

import com.databasepreservation.modules.siard.constants.SIARDDKConstants;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public abstract class SIARDDKMetadataPathStrategy implements MetadataPathStrategy {
  // Names for directories
  private static final String INDICES_DIR = "Indices";
  private static final String SCHEMA_DIR = "Schemas" + SIARDDKConstants.FILE_SEPARATOR + "standard";

  @Override
  public String getXmlFilePath(String filename) throws InvalidParameterException {

    if (checkFilename(filename)) {

      return new StringBuilder().append(INDICES_DIR).append(SIARDDKConstants.FILE_SEPARATOR).append(filename)
        .append(SIARDDKConstants.FILE_EXTENSION_SEPARATOR).append(SIARDDKConstants.XML_EXTENSION).toString();

    } else {
      throw new InvalidParameterException("Invalid filename for metadata file.");
    }
  }

  @Override
  public String getXsdFilePath(String filename) {

    if (checkFilename(filename)) {
      return new StringBuilder().append(SCHEMA_DIR).append(SIARDDKConstants.FILE_SEPARATOR).append(filename)
        .append(SIARDDKConstants.FILE_EXTENSION_SEPARATOR).append(SIARDDKConstants.XSD_EXTENSION).toString();
    } else {
      throw new InvalidParameterException("Invalid filename for metadata file.");
    }
  }

  @Override
  public abstract String getXsdResourcePath(String filename) throws InvalidParameterException;

  public boolean checkFilename(String filename) {

    // Valid filenames
    String[] validFileNames = {SIARDDKConstants.TABLE_INDEX, SIARDDKConstants.ARCHIVE_INDEX, SIARDDKConstants.DOC_INDEX,
      SIARDDKConstants.CONTEXT_DOCUMENTATION_INDEX, SIARDDKConstants.FILE_INDEX,
      SIARDDKConstants.DOCUMENT_IDENTIFICATION, SIARDDKConstants.XML_SCHEMA, "fileIndex_original", "docIndex_original"};

    ArrayList<String> validFilenames = new ArrayList<String>(Arrays.asList(validFileNames));
    if (validFilenames.contains(filename)) {
      return true;
    }
    return false;
  }
}
