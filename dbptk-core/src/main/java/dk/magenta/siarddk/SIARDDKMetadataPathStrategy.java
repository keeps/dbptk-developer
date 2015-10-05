package dk.magenta.siarddk;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;

import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDKMetadataPathStrategy implements MetadataPathStrategy {

  // Names for directories
  private static final String INDICES_DIR = "Indices";
  private static final String SCHEMA_DIR = "Schemas" + Constants.FILE_SEPARATOR + "standard";

  @Override
  public String getXmlFilePath(String filename) throws InvalidParameterException {

    if (checkFilename(filename)) {

      return new StringBuilder().append(INDICES_DIR).append(Constants.FILE_SEPARATOR).append(filename)
        .append(Constants.FILE_EXTENSION_SEPARATOR).append(Constants.XML_EXTENSION).toString();

    } else {
      throw new InvalidParameterException("Invalid filename for metadata file.");
    }
  }

  @Override
  public String getXsdFilePath(String filename) {

    if (checkFilename(filename)) {
      return new StringBuilder().append(SCHEMA_DIR).append(Constants.FILE_SEPARATOR).append(filename)
        .append(Constants.FILE_EXTENSION_SEPARATOR).append(Constants.XSD_EXTENSION).toString();
    } else {
      throw new InvalidParameterException("Invalid filename for metadata file.");
    }
  }

  private boolean checkFilename(String filename) {

    // Valid filenames
    String[] validFileNames = {Constants.TABLE_INDEX, Constants.ARCHIVE_INDEX, Constants.DOC_INDEX,
      Constants.CONTEXT_DOCUMENTATION_INDEX, Constants.FILE_INDEX};

    ArrayList<String> validFilenames = new ArrayList<String>(Arrays.asList(validFileNames));
    if (validFilenames.contains(filename)) {
      return true;
    }
    return false;
  }

}
