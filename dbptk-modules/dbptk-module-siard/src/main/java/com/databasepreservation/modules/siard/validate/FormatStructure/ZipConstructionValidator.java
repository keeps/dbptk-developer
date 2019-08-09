package com.databasepreservation.modules.siard.validate.FormatStructure;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Enumeration;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.modules.validate.ValidatorModule;
import com.databasepreservation.model.reporters.ValidationReporter;

/**
 * This validator checks the Construction of the SIARD archive file (4.1 in eCH-0165 SIARD Format Specification)
 * ZIP validations accordingly the APPNOTES from PKWARE Manufacture (https://pkware.cachefly.net/webdocs/APPNOTE/APPNOTE-6.3.6.TXT - Consulted at: 8/8/2019)
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class ZipConstructionValidator extends ValidatorModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZipConstructionValidator.class);

  private static final String MODULE_NAME = "Construction of the SIARD archive file";
  private static final String G_41 = "4.1";
  private static final String G_411 = "G_4.1-1";
  private static final String G_412 = "G_4.1-2";
  private static final String G_413 = "G_4.1-3";
  private static final String G_414 = "G_4.1-4";
  private static final String G_415 = "G_4.1-5";
  private static final byte[] ZIP_MAGIC_NUMBER = {'P', 'K', 0x3, 0x4};
  private static final String SIARD_EXTENSION = ".siard";

  public static ZipConstructionValidator newInstance() {
    return new ZipConstructionValidator();
  }

  private ZipConstructionValidator() {
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(G_41, MODULE_NAME);
    if (isZipFile()) {
      getValidationReporter().validationStatus(G_411, ValidationReporter.Status.OK);
    } else {
      validationFailed(G_411, MODULE_NAME);
      return false;
    }

    if (deflateOrStore()) {
        getValidationReporter().validationStatus(G_412, ValidationReporter.Status.OK);
    } else {
      validationFailed(G_412, MODULE_NAME);
      return false;
    }

    if (passwordProtected()) {
      getValidationReporter().validationStatus(G_413, ValidationReporter.Status.OK);
    } else {
      validationFailed(G_413, MODULE_NAME);
      return false;
    }

    // G_414 SELF VALIDATE
    getValidationReporter().validationStatus(G_414, ValidationReporter.Status.OK);

    if (fileExtension()) {
      getValidationReporter().validationStatus(G_415, ValidationReporter.Status.OK);
    } else {
      validationFailed(G_415, MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.OK);
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
    if (getSIARDPackagePath() == null) {
      return false;
    }

    boolean isZip = true;

    byte[] buffer = new byte[ZIP_MAGIC_NUMBER.length];
    try {
      RandomAccessFile raf = new RandomAccessFile(getSIARDPackagePath().toFile(), "r");
      raf.readFully(buffer);
      for (int i = 0; i < ZIP_MAGIC_NUMBER.length; i++) {
        if (buffer[i] != ZIP_MAGIC_NUMBER[i]) {
          isZip = false;
          break;
        }
      }
      raf.close();
    } catch (IOException e) {
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
    if (getSIARDPackagePath() == null) {
      return false;
    }

    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      final Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();
      while (entries.hasMoreElements()) {
        final int method = entries.nextElement().getMethod();
        if (method != ZipArchiveEntry.DEFLATED && method != ZipArchiveEntry.STORED) {
          return false;
        }
      }
    } catch (IOException e) {
      return false;
    }

    return true;
  }

  /**
   * G_4.1-3
   * 
   * The SIARD file is not password-protected or encrypted.
   *
   * @return true if valid otherwise false
   */
  private boolean passwordProtected() {
    if (getSIARDPackagePath() == null) {
      return false;
    }

    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      final Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();
      while (entries.hasMoreElements()) {
        ZipArchiveEntry entry = entries.nextElement();
        if (entry.getGeneralPurposeBit().usesEncryption()) {
          return false;
        }
      }
    } catch (IOException e) {
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
    if (getSIARDPackagePath() == null) {
      return false;
    }

    return getSIARDPackagePath().getFileName().toString().endsWith(SIARD_EXTENSION);
  }
}
