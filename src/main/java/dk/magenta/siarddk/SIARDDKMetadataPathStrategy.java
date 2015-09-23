package dk.magenta.siarddk;

import java.io.File;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;

import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;

public class SIARDDKMetadataPathStrategy implements MetadataPathStrategy {

  // Extensions for files
  private static final String XML_EXTENSION = "xml";
  private static final String XSD_EXTENSION = "xsd";

  // control characters
  private static final String FILE_SEPARATOR = File.separator; // is "/" on Unix
                                                               // and "\\" on
                                                               // Windows
  private static final String FILE_EXTENSION_SEPARATOR = ".";

  // Names for directories
  private static final String INDICES_DIR = "Indices";
  private static final String SCHEMA_DIR = "Schemas" + FILE_SEPARATOR + "standard";

  // TO-DO: take all of this into constants
  // Valid filenames
  private static final String[] VALID_FILENAMES = {"tableIndex", "archiveIndex", "docIndex",
    "contextDocumentationIndex", "fileIndex"};

  @Override
  public String getXmlFilePath(String filename) throws InvalidParameterException {

    if (checkFilename(filename)) {

      return new StringBuilder().append(INDICES_DIR).append(FILE_SEPARATOR).append(filename)
        .append(FILE_EXTENSION_SEPARATOR).append(XML_EXTENSION).toString();

    } else {
      throw new InvalidParameterException("Invalid filename for metadata file.");
    }
  }

  @Override
  public String getXsdFilePath(String filename) {

    if (checkFilename(filename)) {
      return new StringBuilder().append(SCHEMA_DIR).append(FILE_SEPARATOR).append(filename)
        .append(FILE_EXTENSION_SEPARATOR).append(XSD_EXTENSION).toString();
    } else {
      throw new InvalidParameterException("Invalid filename for metadata file.");
    }
  }

  private boolean checkFilename(String filename) {
    ArrayList<String> validFilenames = new ArrayList<String>(Arrays.asList(VALID_FILENAMES));
    if (validFilenames.contains(filename)) {
      return true;
    }
    return false;
  }

}
