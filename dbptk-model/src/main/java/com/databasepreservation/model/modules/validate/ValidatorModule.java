package com.databasepreservation.model.modules.validate;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public abstract class ValidatorModule {
  private Path SIARDPackagePath = null;
  private Reporter reporter;
  private ValidationReporter validationReporter;
  private ZipFile zipFile = null;
  private List<String> zipFileNames = null;

  protected Path getSIARDPackagePath() {
    return SIARDPackagePath;
  }

  public void setSIARDPackagePath(Path SIARDPackagePath) {
    this.SIARDPackagePath = SIARDPackagePath;
  }

  public Reporter getReporter() {
    return reporter;
  }

  public void setReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  protected ValidationReporter getValidationReporter() {
    return validationReporter;
  }

  public void setValidationReporter(ValidationReporter validationReporter) {
    this.validationReporter = validationReporter;
  }

  protected void validationFailed(String ID, String moduleName) {
    validationReporter.validationStatus(ID, ValidationReporter.Status.ERROR);
    validationReporter.moduleValidatorFinished(moduleName, ValidationReporter.Status.ERROR);
  }

  public ZipFile getZipFile() {
    return zipFile;
  }

  public List<String> getZipFileNames() {
    return zipFileNames;
  }

  private void importZipFile() throws IOException {
    if (zipFile == null)
      zipFile = new ZipFile(getSIARDPackagePath().toFile());
  }

  protected InputStream getZipInputStream(final String fileName) {
    try {
      if (this.zipFile == null) {
        importZipFile();
      }
      final ZipArchiveEntry entry = this.zipFile.getEntry(fileName);
      return this.zipFile.getInputStream(entry);
    } catch (IOException e) {
      return null;
    }
  }

  protected void closeZipFile() throws ModuleException {
    if (zipFile != null) {
      try {
        zipFile.close();
      } catch (IOException e) {
        throw new ModuleException().withCause(e.getCause()).withMessage("Error trying to close the SIARD file");
      }
      zipFile = null;
    }
  }

  private void retrieveFilesInsideZip() throws IOException {
    this.zipFileNames = new ArrayList<>();
    if (this.zipFile == null) {
      importZipFile();
    }
    final Enumeration<ZipArchiveEntry> entries = this.zipFile.getEntries();
    while (entries.hasMoreElements()) {
      this.zipFileNames.add(entries.nextElement().getName());
    }
  }

  protected boolean preValidationRequirements() {
    if (this.SIARDPackagePath == null) {
      return true;
    }

    if (this.zipFile == null) {
      try {
        importZipFile();
      } catch (IOException e) {
        return true;
      }
    }

    if (this.zipFileNames == null) {
      try {
        retrieveFilesInsideZip();
      } catch (IOException e) {
        return true;
      }
    }

    return false;
  }

  public abstract boolean validate() throws ModuleException;
}
