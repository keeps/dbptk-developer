package com.databasepreservation.modules.siard.validate.FormatStructure;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.reporters.ValidationReporter.Status;
import com.databasepreservation.modules.siard.validate.ValidatorModule;

/**
 * This validator checks the Structure of the SIARD archive file (4.1 in eCH-0165 SIARD Format Specification)
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDStructureValidator extends ValidatorModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDStructureValidator.class);

  private static final String MODULE_NAME = "Structure of the SIARD archive file";
  private static final String P_42 = "4.2";
  private static final String P_421 = "P_4.2-1";
  private static final String P_422 = "P_4.2-2";
  private static final String P_423 = "P_4.2-3";
  private static final String P_424 = "P_4.2-4";
  private static final String P_425 = "P_4.2-5";
  private static final String P_426 = "P_4.2-6";

  public static SIARDStructureValidator newInstance() {
    return new SIARDStructureValidator();
  }

  private SIARDStructureValidator() {
  }

  public boolean validate() {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(P_42, MODULE_NAME);

    if (validateSIARDStructure()) {
      getValidationReporter().validationStatus(P_421, Status.OK);
    } else {
      validationFailed(P_421, MODULE_NAME);
      return false;
    }

    if (validateContentFolderStructure()) {
      getValidationReporter().validationStatus(P_422, Status.OK);
    } else {
      validationFailed(P_422, MODULE_NAME);
      return false;
    }

    if (validateTableFolderStructure()) {
      getValidationReporter().validationStatus(P_423, Status.OK);
    } else {
      validationFailed(P_423, MODULE_NAME);
      return false;
    }

    if (validateRecognitionOfSIARDFormat()) {
      getValidationReporter().validationStatus(P_424, Status.OK);
    } else {
      validationFailed(P_424, MODULE_NAME);
      return false;
    }

    if (validateHeaderFolderStructure()) {
      getValidationReporter().validationStatus(P_425, Status.OK);
    } else {
      validationFailed(P_425, MODULE_NAME);
      return false;
    }

    if (validateFilesAndFoldersNames()) {
      getValidationReporter().validationStatus(P_426, Status.OK);
    } else {
      validationFailed(P_426, MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorFinished(MODULE_NAME, Status.PASSED);

    return true;
  }

  /**
   * P_4.2-1
   * The table data are located in the content/ folder and the metadata in the
   * header/ folder. No further folders or files are permitted.
   *
   * @return true if valid otherwise false
   */
  private boolean validateSIARDStructure() {
    for (String file : getZipFileNames()) {
      if (!file.startsWith("header/") && !file.startsWith("content/")) {
        return false;
      }
    }

    return true;
  }

  /**
   * P_4.2-2
   * The content/ folder contains one or more schema folders in which the individual
   * table folders are located. No other folders or files are permitted.
   *
   * @return true if valid otherwise false
   */
  private boolean validateContentFolderStructure() {
    for (String fileName : getZipFileNames()) {
      if (fileName.startsWith("content")) {
        Path path = Paths.get(fileName);
        if (path.getNameCount() >= 3) {
          final Path schema = path.getName(1);
          final Path table = path.getName(2);
          if (!schema.toString().matches("schema[0-9]+"))
            return false;
          if (!table.toString().matches("table[0-9]+"))
            return false;
        }
      }
    }

    return true;
  }

  /**
   * P_4.2-3
   * The individual table folders contain an XML file and an XSD file, the names of
   * which (folder designation and both file names) must be identical. With the
   * exception of BLOB and CLOB folders together with their content (BIN, TXT, or
   * XML files, or a file extension associated with the MIME type of the lob files in
   * case this is known, e.g. JPG), no other folders or files are permitted.
   *
   * @return true if valid otherwise false
   */
  private boolean validateTableFolderStructure() {
    for (String fileName : getZipFileNames()) {
      if (fileName.startsWith("content")) {
        Path path = Paths.get(fileName);
        if (path.getNameCount() >= 4) {
          String tableName = path.subpath(2, 3).toString();
          Path tableContent = path.subpath(3, path.getNameCount());
          if (tableContent.getNameCount() == 1) { // ex: table1.xml, ignore lob folder
            if (!tableContent.toString().contains("lob")) {
              if (!tableContent.toString().startsWith(tableName)) {
                return false;
              }
              if (path.getFileName().toString().endsWith(".xml") == path.getFileName().toString().endsWith(".xsd")) {
                return false;
              }
            }
          } else if (tableContent.getNameCount() == 2) { // lob5/record1.bin
            if (!tableContent.getParent().toString().matches("lob[0-9]+")) {
              return false; // parent folder must be lob
            }

            if (!tableContent.getFileName().toString().matches("record[0-9]+.*")) {
              return false; // lob file must be named record
            }

            final boolean bin = path.getFileName().toString().endsWith(".bin");
            final boolean text = path.getFileName().toString().endsWith(".txt");
            final boolean xml = path.getFileName().toString().endsWith(".xml");

            if (!(bin ^ text ^ xml))
              return false;
          } else {
            return false;
          }
        }
      }
    }

    return true;
  }

  /**
   * P_4.2-4
   * In order to facilitate the recognition of the SIARD Format (e.g. by PRONOM) an
   * empty folder /header/siardversion/2.1/ identifying the version of the
   * SIARD Format must exist
   *
   * @return true if valid otherwise false
   */
  private boolean validateRecognitionOfSIARDFormat() {
    List<Path> versions = new ArrayList<>();

    for (String fileName : getZipFileNames()) {
      Path path = Paths.get(fileName);
      if (path.startsWith("header/siardversion")) {
        if (path.getNameCount() > 2) {
          versions.add(path);
        }
      }
    }

    if (versions.size() != 1) {
      return false;
    }

    final int v2_1 = versions.get(0).compareTo(Paths.get("header/siardversion/2.1"));
    final int v2_0 = versions.get(0).compareTo(Paths.get("header/siardversion/2.0"));

    return v2_1 == 0 || v2_0 == 0;
  }

  /**
   * P_4.2-5
   * The metadata.xml and metadata.xsd files must be present in the header/
   * folder. Additional files, such as style sheets, are permitted.
   *
   * @return true if valid otherwise false
   */
  private boolean validateHeaderFolderStructure() {
    List<String> headers = new ArrayList<>();

    for (String fileName : getZipFileNames()) {
      if (fileName.startsWith("header")) headers.add(fileName);
    }

    if (headers.isEmpty()) return false;

    return headers.contains("header/metadata.xml") && headers.contains("header/metadata.xsd");

    /*headers.remove("header/metadata.xml");
    headers.remove("header/metadata.xsd");

    for (String header : headers) {
      if (!header.endsWith("2.1/") && !header.endsWith("2.0/")) {
        if (!header.endsWith(".xsd")) return false;
      }
    }*/
  }

  /**
   * P_4.2-6
   * All file and folder names referring to elements inside the SIARD (ZIP64) file
   * must be structured as follows:
   * The name must begin with a letter [a-z or A-Z] and must then contain only the
   * following characters:
   * a-z
   * A-Z
   * 0-9
   * _
   * . (may only be used to separate the name from the extension)
   *
   * @return true if valid otherwise false
   */
  private boolean validateFilesAndFoldersNames() {
    for (String fileName : getZipFileNames()) {
      String[] foldersAndFiles = fileName.split("/");
      for (String s : foldersAndFiles) {
        if (s.length() > 20) {
          getValidationReporter().warning(P_426, "File and folder names should not exceed 20 characters", s);
        }
      }

      fileName = fileName.replace("/", "");
      final int lastIndexOf = fileName.lastIndexOf(".");
      final String substring;
      if (lastIndexOf != -1) {
        substring = fileName.substring(0, lastIndexOf);
      } else {
        substring = fileName;
      }

      if (substring.contains(".")) return false;
      if (!substring.matches("[A-Za-z0-9_]+")) return false;
    }

    return true;
  }

  /*
   * Auxiliary Methods
   */
}
