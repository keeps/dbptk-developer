/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component.formatStructure;

import static java.util.zip.ZipEntry.DEFLATED;
import static java.util.zip.ZipEntry.STORED;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.Constants;
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;

/**
 * This validator checks the Construction of the SIARD archive file (4.1 in eCH-0165 SIARD Format Specification)
 * ZIP validations accordingly the APPNOTES from PKWARE Manufacture (https://pkware.cachefly.net/webdocs/APPNOTE/APPNOTE-6.3.6.TXT - Consulted at: 8/8/2019)
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class ZipConstructionValidator extends ValidatorComponentImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZipConstructionValidator.class);

  private final String MODULE_NAME;
  private static final String G_41 = "4.1";
  private static final String G_411 = "G_4.1-1";
  private static final String G_412 = "G_4.1-2";
  private static final String G_413 = "G_4.1-3";
  private static final String G_414 = "G_4.1-4";
  private static final String G_415 = "G_4.1-5";
  private static final byte[] ZIP_MAGIC_NUMBER = {'P', 'K', 0x3, 0x4};

  private List<String> compressionZipEntriesWithErrors = new ArrayList<>();

  public ZipConstructionValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    compressionZipEntriesWithErrors.clear();
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() {
    observer.notifyStartValidationModule(MODULE_NAME, G_41);
    getValidationReporter().moduleValidatorHeader(G_41, MODULE_NAME);

    if (isZipFile()) {
      observer.notifyValidationStep(MODULE_NAME, G_411, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(G_411, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, G_411, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(G_411, MODULE_NAME,
        "Zip archive is not in accordance with the specification published by the company PkWare");
      return false;
    }

    if (deflateOrStore()) {
      observer.notifyValidationStep(MODULE_NAME, G_412, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(G_412, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, G_412, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(G_412, MODULE_NAME, "", "Invalid entries", compressionZipEntriesWithErrors);
      return false;
    }

    if (passwordProtected()) {
      observer.notifyValidationStep(MODULE_NAME, G_413, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(G_413, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, G_413, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(G_413, MODULE_NAME, "The SIARD file is password-protected or encrypted.");
      return false;
    }

    // G_414 SELF VALIDATE
    observer.notifyValidationStep(MODULE_NAME, G_414, ValidationReporterStatus.OK);
    getValidationReporter().validationStatus(G_414, ValidationReporterStatus.OK);

    if (fileExtension()) {
      observer.notifyValidationStep(MODULE_NAME, G_415, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(G_415, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, G_415, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(G_415, MODULE_NAME,  "The ZIP archive must have the file extension .siard");
      return false;
    }

    observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporterStatus.PASSED);
    return true;
  }

  /**
   * G_4.1-1
   * 
   * The SIARD file is stored as a single, ZIP archive in accordance with the
   * specification published by the company PkWare
   *
   * @return true if valid otherwise false
   */
  private boolean isZipFile() {
    if (path == null) {
      LOGGER.debug("The path to the SIARD file must exists on {}", MODULE_NAME);
      getValidationReporter().validationStatus(G_411, ValidationReporterStatus.ERROR,
              "Failed to validate " + MODULE_NAME, "Please check the log file for more information");
      return false;
    }

    boolean isZip = true;

    byte[] buffer = new byte[ZIP_MAGIC_NUMBER.length];
    try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
      raf.readFully(buffer);
      for (int i = 0; i < ZIP_MAGIC_NUMBER.length; i++) {
        if (buffer[i] != ZIP_MAGIC_NUMBER[i]) {
          isZip = false;
          break;
        }
      }
    } catch (IOException e) {
      LOGGER.debug("Failed to validate {}", G_411, e);
      getValidationReporter().validationStatus(G_411, ValidationReporterStatus.ERROR,
              "Failed to validate due to an exception on " + MODULE_NAME, "Please check the log file for more information");
      isZip = false;
    }

    return isZip;
  }

  /**
   * G_4.1-2
   * 
   * SIARD files must either be uncompressed or else compressed using the
   * “deflate” algorithm as described in RFC 1951
   *
   * @return true if valid otherwise false
   */
  private boolean deflateOrStore() {
    if (path == null) {
      LOGGER.debug("The path to the SIARD file must exists on {}", MODULE_NAME);
      return false;
    }

    try {
      final Enumeration<ZipArchiveEntry> entries = zipFileManagerStrategy.getZipArchiveEntries(path);
      while (entries.hasMoreElements()) {
        final ZipArchiveEntry zipArchiveEntry = entries.nextElement();
        final int method = zipArchiveEntry.getMethod();
        if (method != DEFLATED && method != STORED) {
          compressionZipEntriesWithErrors.add(zipArchiveEntry.getName());
        }
      }
    } catch (IOException e) {
      LOGGER.debug("Failed to validate {} due to an error", G_412, e);
      getValidationReporter().validationStatus(G_412, ValidationReporterStatus.ERROR,
              "Failed to validate due to an exception on " + MODULE_NAME, "Please check the log file for more information");
      return false;
    }

    return compressionZipEntriesWithErrors.isEmpty();
  }

  /**
   * G_4.1-3
   * 
   * The SIARD file is not password-protected or encrypted.
   *
   * @return true if valid otherwise false
   */
  private boolean passwordProtected() {
    if (path == null) {
      LOGGER.debug("The path to the SIARD file must exists on {}", MODULE_NAME);
      return false;
    }

    try {
      final Enumeration<ZipArchiveEntry> entries = zipFileManagerStrategy.getZipArchiveEntries(path);
      while (entries.hasMoreElements()) {
        final ZipArchiveEntry zipArchiveEntry = entries.nextElement();
        final boolean usesEncryption = zipArchiveEntry.getGeneralPurposeBit().usesEncryption();
        if (usesEncryption) {
          return false;
        }
      }
    } catch (IOException e) {
      LOGGER.debug("Failed to validate {} due to an error", G_413, e);
      getValidationReporter().validationStatus(G_413, ValidationReporterStatus.ERROR,
              "Failed to validate due to an exception on " + MODULE_NAME, "Please check the log file for more information");
      return false;
    }

    return true;
  }

  /**
   * G_4.1-5
   * 
   * The ZIP archive has the file extension “.siard”.
   *
   * @return true if valid otherwise false
   */
  private boolean fileExtension() {
    if (path == null) {
      LOGGER.debug("The path to the SIARD file must exists on {}", MODULE_NAME);
      return false;
    }

    return path.getFileName().toString().endsWith(Constants.SIARD_EXTENSION);
  }
}
