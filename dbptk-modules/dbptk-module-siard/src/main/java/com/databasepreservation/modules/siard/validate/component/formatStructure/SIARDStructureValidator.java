package com.databasepreservation.modules.siard.validate.component.formatStructure;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.databasepreservation.model.exception.ModuleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.Constants;
import com.databasepreservation.model.reporters.ValidationReporter.Status;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;

/**
 * This validator checks the Structure of the SIARD archive file (4.1 in eCH-0165 SIARD Format Specification)
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDStructureValidator extends ValidatorComponentImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDStructureValidator.class);

  private final String MODULE_NAME;
  private static final String P_42 = "4.2";
  private static final String P_421 = "P_4.2-1";
  private static final String P_422 = "P_4.2-2";
  private static final String P_423 = "P_4.2-3";
  private static final String P_424 = "P_4.2-4";
  private static final String P_425 = "P_4.2-5";
  private static final String P_426 = "P_4.2-6";

  private List<String> P_421_ERRORS = new ArrayList<>();
  private List<String> P_422_ERRORS = new ArrayList<>();
  private List<String> P_423_ERRORS = new ArrayList<>();
  private List<String> P_426_ERRORS = new ArrayList<>();

  public SIARDStructureValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, P_42);

    if (preValidationRequirements()){
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(P_42, MODULE_NAME);

    if (validateSIARDStructure()) {
      observer.notifyValidationStep(MODULE_NAME, P_421, Status.OK);
      getValidationReporter().validationStatus(P_421, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_421, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_421, MODULE_NAME,
        "No further folders or files are permitted besides the header/ and content/ folders", P_421_ERRORS);
      closeZipFile();
      return false;
    }

    if (validateContentFolderStructure()) {
      observer.notifyValidationStep(MODULE_NAME, P_422, Status.OK);
      getValidationReporter().validationStatus(P_422, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_422, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_422, MODULE_NAME, "No other folders or files are permitted inside the content/ folder.",
        P_422_ERRORS);
      closeZipFile();
      return false;
    }

    if (validateTableFolderStructure()) {
      observer.notifyValidationStep(MODULE_NAME, P_423, Status.OK);
      getValidationReporter().validationStatus(P_423, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_423, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_423, MODULE_NAME, "", P_423_ERRORS);
      closeZipFile();
      return false;
    }

    if (validateRecognitionOfSIARDFormat()) {
      observer.notifyValidationStep(MODULE_NAME, P_424, Status.OK);
      getValidationReporter().validationStatus(P_424, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_424, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_424, MODULE_NAME, "Missing the folder to facilitate the recognition of the SIARD format");
      closeZipFile();
      return false;
    }

    if (validateHeaderFolderStructure()) {
      observer.notifyValidationStep(MODULE_NAME, P_425, Status.OK);
      getValidationReporter().validationStatus(P_425, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_425, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_425, MODULE_NAME, "The metadata.xml and metadata.xsd files must be present in the header/ folder");
      closeZipFile();
      return false;
    }

    if (validateFilesAndFoldersNames()) {
      observer.notifyValidationStep(MODULE_NAME, P_426, Status.OK);
      getValidationReporter().validationStatus(P_426, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_426, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_426, MODULE_NAME, "", P_426_ERRORS);
      closeZipFile();
      return false;
    }

    observer.notifyFinishValidationModule(MODULE_NAME, Status.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, Status.PASSED);
    closeZipFile();

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
    if (preValidationRequirements()){
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    for (String file : getZipFileNames()) {
      if (!file.startsWith("header/") && !file.startsWith("content/")) {
        P_421_ERRORS.add(file);
      }
    }

    return P_421_ERRORS.isEmpty();
  }

  /**
   * P_4.2-2
   * The content/ folder contains one or more schema folders in which the individual
   * table folders are located. No other folders or files are permitted.
   *
   * @return true if valid otherwise false
   */
  private boolean validateContentFolderStructure() {
    if (preValidationRequirements()){
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    for (String fileName : getZipFileNames()) {
      if (fileName.startsWith(Constants.SIARD_CONTENT_FOLDER)) {
        Path path = Paths.get(fileName);
        if (path.getNameCount() == 2) {
          if (!path.getName(1).toString().matches("schema[0-9]+")) {
            P_422_ERRORS.add(path.toString());
          }
        }
        if (path.getNameCount() >= 3) {
          final Path schema = path.getName(1);
          final Path table = path.getName(2);
          if (!schema.toString().matches("schema[0-9]+"))
            P_422_ERRORS.add(path.toString());
          if (!table.toString().matches("table[0-9]+"))
            P_422_ERRORS.add(path.toString());
        }
      }
    }

    return P_422_ERRORS.isEmpty();
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
    if (preValidationRequirements()){
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    for (String fileName : getZipFileNames()) {
      if (fileName.startsWith(Constants.SIARD_CONTENT_FOLDER)) {
        Path path = Paths.get(fileName);
        if (path.getNameCount() >= 4) {
          String tableName = path.subpath(2, 3).toString();
          Path tableContent = path.subpath(3, path.getNameCount());
          if (tableContent.getNameCount() == 1) { // ex: table1.xml, ignore lob folder
            if (!tableContent.toString().contains("lob")) {
              if (!tableContent.toString().startsWith(tableName)) {
                P_423_ERRORS.add(path.toString());
              }
              if (path.getFileName().toString().endsWith(Constants.XML_EXTENSION) == path.getFileName().toString()
                .endsWith(Constants.XSD_EXTENSION)) {
                P_423_ERRORS.add(path.toString());
              }
            }
          } else if (tableContent.getNameCount() == 2) { // lob5/record1.bin
            if (!tableContent.getParent().toString().matches("lob[0-9]+")) {
              P_423_ERRORS.add(path.toString()); // parent folder must be lob
            }

            if (!tableContent.getFileName().toString().matches("record[0-9]+.*")) {
              P_423_ERRORS.add(path.toString()); // lob file must be named record
            }

            final boolean bin = path.getFileName().toString().endsWith(Constants.BIN_EXTENSION);
            final boolean text = path.getFileName().toString().endsWith(Constants.TXT_EXTENSION);
            final boolean xml = path.getFileName().toString().endsWith(Constants.XML_EXTENSION);

            if (!(bin ^ text ^ xml)) P_423_ERRORS.add(path.toString());
          } else {
            P_423_ERRORS.add(path.toString());
          }
        }
      }
    }

    return P_423_ERRORS.isEmpty();
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
    if (preValidationRequirements()){
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    List<Path> versions = new ArrayList<>();

    for (String fileName : getZipFileNames()) {
      Path path = Paths.get(fileName);
      if (path.startsWith(validatorPathStrategy.getSIARDVersionPath())) {
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
    if (preValidationRequirements()){
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    List<String> headers = new ArrayList<>();

    for (String fileName : getZipFileNames()) {
      if (fileName.startsWith(Constants.SIARD_HEADER_FOLDER))
        headers.add(fileName);
    }

    if (headers.isEmpty()) return false;

    if (!headers.contains(validatorPathStrategy.getMetadataXMLPath())) return false;
    if (!headers.contains(validatorPathStrategy.getMetadataXSDPath())) return false;

    headers.remove(validatorPathStrategy.getMetadataXMLPath());
    headers.remove(validatorPathStrategy.getMetadataXSDPath());
    headers.remove(Constants.SIARD_HEADER_FOLDER + Constants.RESOURCE_FILE_SEPARATOR);
    headers.remove(validatorPathStrategy.getSIARDVersionPath());

    for (String header : headers) {
      if (!header.endsWith("2.1/") && !header.endsWith("2.0/")) {
        if (!header.endsWith(".xsd")) return false;
      }
    }

    return true;
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
    if (preValidationRequirements()){
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    for (String fileName : getZipFileNames()) {
      String[] foldersAndFiles = fileName.split("/");
      for (String s : foldersAndFiles) {
        if (s.length() > 20) {
          getValidationReporter().warning(P_426, "File and folder names should not exceed 20 characters", s);
        }
      }

      final String replaced = fileName.replace("/", "");
      final int lastIndexOf = replaced.lastIndexOf(".");
      final String substring;
      if (lastIndexOf != -1) {
        substring = replaced.substring(0, lastIndexOf);
      } else {
        substring = replaced;
      }

      if (substring.contains(".")) P_426_ERRORS.add(fileName);
      if (!substring.matches("[A-Za-z0-9_]+")) P_426_ERRORS.add(fileName);
    }

    return P_426_ERRORS.isEmpty();
  }
}
