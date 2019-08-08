package com.databasepreservation.modules.siard.validate.FormatStructure;

import com.databasepreservation.model.modules.validate.ValidatorModule;
import com.databasepreservation.model.reporters.ValidationReporter.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

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

  private static List<String> zipFileNames = null;

  public static SIARDStructureValidator newInstance() {
    return new SIARDStructureValidator();
  }

  private SIARDStructureValidator() {
  }

  public boolean validate() {
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

    getValidationReporter().moduleValidatorFinished(MODULE_NAME, Status.OK);

    return true;
  }

  private boolean validateSIARDStructure() {
    if (getSIARDPackagePath() == null) {
      return false;
    }

    if (zipFileNames == null) {
      try {
        retrieveFilesInsideZip();
      } catch (IOException e) {
        return false;
      }
    }

    for (String file : zipFileNames) {
      if (!file.startsWith("header/") && !file.startsWith("content/")) {
        return false;
      }
    }

    return true;
  }

  private boolean validateContentFolderStructure() {
    if (getSIARDPackagePath() == null) {
      return false;
    }

    if (zipFileNames == null) {
      try {
        retrieveFilesInsideZip();
      } catch (IOException e) {
        return false;
      }
    }

    for (String fileName : zipFileNames) {
      if (fileName.startsWith("content")) {
        Path path = Paths.get(fileName);
        if (!path.subpath(0, 3).toString().matches("content/schema[0-9]+/table[0-9]+")) {
          return false;
        }
      }
    }

    return true;
  }

  private boolean validateTableFolderStructure() {
    if (getSIARDPackagePath() == null) {
      return false;
    }

    if (zipFileNames == null) {
      try {
        retrieveFilesInsideZip();
      } catch (IOException e) {
        return false;
      }
    }

    for (String fileName : zipFileNames) {
      if (fileName.startsWith("content")) {
        Path path = Paths.get(fileName);
        String tableName = path.subpath(2, 3).toString();
        Path tableContent = path.subpath(3, path.getNameCount());
        if (tableContent.getNameCount() == 1) { // ex: table1.xml
          if (!tableContent.toString().startsWith(tableName)) return false;
          if (path.getFileName().toString().endsWith(".xml") == path.getFileName().toString().endsWith(".xsd"))
            return false;
        } else if (tableContent.getNameCount() == 2) { // lob5/record1.bin
          if (!tableContent.getParent().toString().matches("lob[0-9]+")) return false; // parent folder must be lob

          if (!tableContent.getFileName().toString().matches("record[0-9]+.*"))
            return false; // lob file must be named record

          final boolean bin = path.getFileName().toString().endsWith(".bin");
          final boolean text = path.getFileName().toString().endsWith(".txt");
          final boolean xml = path.getFileName().toString().endsWith(".xml");

          if (!(bin ^ text ^ xml)) return false;
        } else {
          return false;
        }
      }
    }

    return true;
  }

  private boolean validateRecognitionOfSIARDFormat() {
    if (getSIARDPackagePath() == null) {
      return false;
    }

    if (zipFileNames == null) {
      try {
        retrieveFilesInsideZip();
      } catch (IOException e) {
        return false;
      }
    }

    int counter = 0;

    for (String fileName : zipFileNames) {
      Path path = Paths.get(fileName);
      if (path.toString().startsWith("header/siardversion")) {
        if (counter == 0) {
          counter++;
          if (path.getFileName().toString().equals("2.1") == path.getFileName().toString().equals("2.0")) {
            return false;
          }
        } else return false;
      }
    }

    return true;
  }

  private boolean validateHeaderFolderStructure() {
    if (getSIARDPackagePath() == null) {
      return false;
    }

    if (zipFileNames == null) {
      try {
        retrieveFilesInsideZip();
      } catch (IOException e) {
        return false;
      }
    }

    List<String> headers = new ArrayList<>();

    for (String fileName : zipFileNames) {
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

  private boolean validateFilesAndFoldersNames() {
    if (getSIARDPackagePath() == null) {
      return false;
    }

    if (zipFileNames == null) {
      try {
        retrieveFilesInsideZip();
      } catch (IOException e) {
        return false;
      }
    }

    for (String fileName : zipFileNames) {
      String[] foldersAndFiles = fileName.split("/");
      for (String s : foldersAndFiles) {
        if (s.length() > 20) {
          getValidationReporter().warning(P_426, "File and folder names should not exceed 20 characters", s);
        }
      }

      fileName = fileName.replace("/", "");
      final int lastIndexOf = fileName.lastIndexOf(".");
      final String substring = fileName.substring(0, lastIndexOf);

      if (substring.contains(".")) return false;
      if (!substring.matches("[A-Za-z0-9_]+")) return false;
    }

    return true;
  }

  private void retrieveFilesInsideZip() throws IOException {
    zipFileNames = new ArrayList<>();
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      Enumeration<? extends ZipEntry> zipEntries = zipFile.entries();
      while (zipEntries.hasMoreElements()) {
        zipFileNames.add((zipEntries.nextElement()).getName());
      }
    }
  }
}
