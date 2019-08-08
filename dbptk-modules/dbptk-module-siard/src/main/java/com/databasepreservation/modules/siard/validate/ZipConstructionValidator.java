package com.databasepreservation.modules.siard.validate;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.reporters.ValidationReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class ZipConstructionValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZipConstructionValidator.class);

  private static final String G_411 = "G_4.1-1";
  private static final String G_412 = "G_4.1-2";
  private static final String G_413 = "G_4.1-3";
  private static final String G_414 = "G_4.1-4";
  private static final String G_415 = "G_4.1-5";
  private static final byte[] ZIP_MAGIC_NUMBER = {'P', 'K', 0x3, 0x4};
  private static final byte[] STORE = {0x0, 0x0};
  private static final byte[] DEFLATE = {0x8, 0x0};
  private Path SIARDPackagePath = null;
  private Reporter reporter;
  private ValidationReporter validationReporter;

  public static ZipConstructionValidator newInstance() {
    return new ZipConstructionValidator();
  }

  private ZipConstructionValidator() {
  }

  public void setSIARDPackagePath(Path SIARDPackagePath) {
    this.SIARDPackagePath = SIARDPackagePath;
  }

  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  public void setValidationReporter(ValidationReporter validationReporter) {
    this.validationReporter = validationReporter;
  }

  public boolean validate() {
    boolean result = isZipFile();

    if (result) {
      validationReporter.validationStatus(G_411, ValidationReporter.Status.OK);
      result = deflateOrStore();
    }

    if (result) result = checkZipEntries();

    return result;
  }

  private boolean isZipFile() {
    if (SIARDPackagePath == null) {
      return false;
    }

    boolean isZip = true;

    byte[] buffer = new byte[ZIP_MAGIC_NUMBER.length];
    try {
      RandomAccessFile raf = new RandomAccessFile(SIARDPackagePath.toFile(), "r");
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
    if (SIARDPackagePath == null) {
      return false;
    }

    boolean isZip;

    byte[] buffer = new byte[10];
    try {
      RandomAccessFile raf = new RandomAccessFile(SIARDPackagePath.toFile(), "r");
      raf.read(buffer);
      isZip = store(buffer) ^ deflate(buffer);
    } catch (IOException e) {
      isZip = false;
    }

    return isZip;
  }

  private boolean checkZipEntries() {
    if (SIARDPackagePath == null) {
      return false;
    }

    ZipInputStream zipInputStream;
    ZipEntry zipEntry;
    int compressed;
    try {
      zipInputStream = new ZipInputStream(new BufferedInputStream(new FileInputStream(SIARDPackagePath.toFile())));
      while ((zipEntry = zipInputStream.getNextEntry()) != null) {
        reporter.customMessage(getClass().getName(), "Validating compress method for entry: " + zipEntry.getName());
        compressed = zipEntry.getMethod();
        if (compressed != 8 && compressed != 0) {
          return false;
        }
      }

      zipInputStream.closeEntry();
      zipInputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return true;
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
