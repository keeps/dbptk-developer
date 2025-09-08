/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.siard_22.component.formatStructure;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.modules.siard.validate.generic.component.ValidatorComponentImpl;

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

  @Override
  public void clean() {
    P_421_ERRORS.clear();
    P_422_ERRORS.clear();
    P_423_ERRORS.clear();
    P_426_ERRORS.clear();
    zipFileManagerStrategy.closeZipFile();
  }

  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, P_42);

    getValidationReporter().moduleValidatorHeader(P_42, MODULE_NAME);

    if (validateSIARDStructure()) {
      observer.notifyValidationStep(MODULE_NAME, P_421, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_421, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_421, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_421, ValidationReporterStatus.ERROR,
        "No further folders or files are permitted besides the header/ and content/ folders", P_421_ERRORS,
        MODULE_NAME);
      return false;
    }

    if (validateContentFolderStructure()) {
      observer.notifyValidationStep(MODULE_NAME, P_422, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_422, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_422, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_422, ValidationReporterStatus.ERROR,
        "No other folders or files are permitted inside the content/ folder.",
        P_422_ERRORS, MODULE_NAME);
      return false;
    }

    if (validateTableFolderStructure()) {
      observer.notifyValidationStep(MODULE_NAME, P_423, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_423, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_423, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_423, ValidationReporterStatus.ERROR,
        "The individual table folders contain an XML file and an XSD file, the names of\n"
          +
          "which (folder designation and both file names) must be identical.", P_423_ERRORS, MODULE_NAME);
      return false;
    }

    if (validateRecognitionOfSIARDFormat()) {
      observer.notifyValidationStep(MODULE_NAME, P_424, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_424, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_424, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_424, MODULE_NAME, "Missing the folder to facilitate the recognition of the SIARD format");
      return false;
    }

    if (validateHeaderFolderStructure()) {
      observer.notifyValidationStep(MODULE_NAME, P_425, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_425, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_425, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_425, MODULE_NAME, "The metadata.xml and metadata.xsd files must be present in the header/ folder");
      return false;
    }

    if (validateFilesAndFoldersNames()) {
      observer.notifyValidationStep(MODULE_NAME, P_426, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_426, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_426, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_426, ValidationReporterStatus.ERROR,
        "All file and folder names referring to elements inside the SIARD (ZIP64) file\n" +
          "must be well structured", P_426_ERRORS, MODULE_NAME);
      return false;
    }

    observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporterStatus.PASSED);

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
    final List<String> zipArchiveEntriesPath = zipFileManagerStrategy.getZipArchiveEntriesPath(path);
    if (zipArchiveEntriesPath == null) {
      return false;
    }
    for (String file : zipArchiveEntriesPath) {
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

    final List<String> zipArchiveEntriesPaths = zipFileManagerStrategy.getZipArchiveEntriesPath(path);
    if (zipArchiveEntriesPaths == null) {
      return false;
    }
    for (String fileName : zipArchiveEntriesPaths) {
      if (fileName.startsWith(Constants.SIARD_CONTENT_FOLDER)) {
        Path path = Paths.get(fileName);
        if (path.getNameCount() == 2 && !path.getName(1).toString().matches("schema[0-9]+")) {
            P_422_ERRORS.add(path.toString());
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
    final List<String> zipArchiveEntriesPaths = zipFileManagerStrategy.getZipArchiveEntriesPath(path);
    if (zipArchiveEntriesPaths == null) {
      return false;
    }
    for (String fileName : zipArchiveEntriesPaths) {
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
   * empty folder /header/siardversion/2.2/ identifying the version of the
   * SIARD Format must exist
   *
   * @return true if valid otherwise false
   */
  private boolean validateRecognitionOfSIARDFormat() {
    List<Path> versions = new ArrayList<>();
    final List<String> zipArchiveEntriesPaths = zipFileManagerStrategy.getZipArchiveEntriesPath(path);
    if (zipArchiveEntriesPaths == null) {
      return false;
    }
    for (String fileName : zipArchiveEntriesPaths) {
      Path path = Paths.get(fileName);
      if (path.startsWith(validatorPathStrategy.getSIARDVersionPath()) && path.getNameCount() > 2) {
          versions.add(path);
        }
      }

    if (versions.size() != 1) {
      return false;
    }

    final int v2_2 = versions.get(0).compareTo(Paths.get("header/siardversion/2.2"));

    return v2_2 == 0;
  }

  /**
   * P_4.2-5
   * The metadata.xml and metadata.xsd files must be present in the header/
   * folder. Additional files, such as style sheets, are permitted.
   *
   * @return true if valid otherwise false
   */
  private boolean validateHeaderFolderStructure() {
    final List<String> zipArchiveEntriesPaths = zipFileManagerStrategy.getZipArchiveEntriesPath(path);
    List<String> headers = new ArrayList<>();
    if (zipArchiveEntriesPaths == null) {
      return false;
    }
    for (String fileName : zipArchiveEntriesPaths) {
      if (fileName.startsWith(Constants.SIARD_HEADER_FOLDER))
        headers.add(fileName);
    }

    if (headers.isEmpty())
      return false;

    if (!headers.contains(validatorPathStrategy.getMetadataXMLPath()))
      return false;
    if (!headers.contains(validatorPathStrategy.getMetadataXSDPath()))
      return false;

    headers.remove(validatorPathStrategy.getMetadataXMLPath());
    headers.remove(validatorPathStrategy.getMetadataXSDPath());
    headers.remove(Constants.SIARD_HEADER_FOLDER + Constants.RESOURCE_FILE_SEPARATOR);
    headers.remove(validatorPathStrategy.getSIARDVersionPath());

    for (String header : headers) {
      if (!header.endsWith("2.2/") && !header.endsWith(".xsd")) {
        return false;
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
    final List<String> zipArchiveEntriesPaths = zipFileManagerStrategy.getZipArchiveEntriesPath(path);
    if (zipArchiveEntriesPaths == null) {
      return false;
    }
    for (String fileName : zipArchiveEntriesPaths) {
      String[] foldersAndFiles = fileName.split("/");
      for (String s : foldersAndFiles) {
        if (s.length() > 20) {
          getValidationReporter().warning(P_426, "File and folder names should not exceed 20 characters", s);
        }
      }

      final String replaced = fileName.replace("/", "");
      final int lastIndexOf = replaced.lastIndexOf('.');
      final String substring;
      if (lastIndexOf != -1) {
        substring = replaced.substring(0, lastIndexOf);
      } else {
        substring = replaced;
      }

      if (substring.contains("."))
        P_426_ERRORS.add(fileName);
      if (!substring.matches("[A-Za-z0-9_]+"))
        P_426_ERRORS.add(fileName);
    }

    return P_426_ERRORS.isEmpty();
  }
}
