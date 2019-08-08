package com.databasepreservation.modules.siard.validate.FormatStructure;

import com.databasepreservation.model.modules.validate.ValidatorModule;
import com.databasepreservation.model.reporters.ValidationReporter;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Enumeration;
import java.util.zip.ZipEntry;

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
  private static final byte[] STORE = {0x0, 0x0};
  private static final byte[] DEFLATE = {0x8, 0x0};
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
      if (checkZipEntries()) {
        getValidationReporter().validationStatus(G_412, ValidationReporter.Status.OK);
      } else {
        validationFailed(G_412, MODULE_NAME);
        return false;
      }
    } else {
      validationFailed(G_412, MODULE_NAME);
      return false;
    }

    if (!passwordProtected()) {
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

  private boolean deflateOrStore() {
    if (getSIARDPackagePath() == null) {
      return false;
    }

    boolean deflateOrStore;

    byte[] buffer = new byte[10];
    try {
      RandomAccessFile raf = new RandomAccessFile(getSIARDPackagePath().toFile(), "r");
      raf.read(buffer);
      deflateOrStore = store(buffer) ^ deflate(buffer);
    } catch (IOException e) {
      deflateOrStore = false;
    }

    return deflateOrStore;
  }

  private boolean checkZipEntries() {
    if (getSIARDPackagePath() == null) {
      return false;
    }

    try (java.util.zip.ZipFile zipFile = new java.util.zip.ZipFile(getSIARDPackagePath().toFile())) {
      Enumeration zipEntries = zipFile.entries();
      while (zipEntries.hasMoreElements()) {
        int method = ((ZipEntry) zipEntries.nextElement()).getMethod();
        if (method != ZipEntry.DEFLATED && method != ZipEntry.STORED) {
          return false;
        }
      }
    } catch (IOException e) {
      return false;
    }

    return true;
  }

  private boolean passwordProtected() {
    if (getSIARDPackagePath() == null) {
      return false;
    }

    try {
      return new ZipFile(getSIARDPackagePath().toFile()).isEncrypted();
    } catch (ZipException e) {
      return false;
    }
  }

  private boolean fileExtension() {
    if (getSIARDPackagePath() == null) {
      return false;
    }

    return getSIARDPackagePath().getFileName().toString().endsWith(SIARD_EXTENSION);
  }

  private boolean store(final byte[] buffer) {
    for (int i = 8; i < 10; i++) {
      if (buffer[i] != STORE[i - 8]) {
        return false;
      }
    }

    return true;
  }

  private boolean deflate(final byte[] buffer) {
    for (int i = 8; i < 10; i++) {
      if (buffer[i] != DEFLATE[i - 8]) {
        return false;
      }
    }

    return true;
  }
}
