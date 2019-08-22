package com.databasepreservation.modules.siard.validate;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.modules.siard.validate.common.path.ValidatorPathStrategy;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public abstract class ValidatorModule {
  private static Path SIARDPackagePath = null;
  private Reporter reporter;
  private ValidationReporter validationReporter;
  private ZipFile zipFile = null;
  private List<String> zipFileNames = null;
  protected static ValidatorPathStrategy validatorPathStrategy = null;

  public abstract boolean validate() throws ModuleException;

  protected Path getSIARDPackagePath() {
    return SIARDPackagePath;
  }

  public void setSIARDPackagePath(Path SIARDPackagePath) {
    if (ValidatorModule.SIARDPackagePath == null) {
      ValidatorModule.SIARDPackagePath = SIARDPackagePath;
    }
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

  public void setValidatorPathStrategy(ValidatorPathStrategy validatorPathStrategy) {
    this.validatorPathStrategy = validatorPathStrategy;
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
    if (SIARDPackagePath == null) {
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

  /**
   * Lazy loading, needs the validatorPathStrategy otherwise it will throw a
   * nullPointerException
   * 
   * @throws ModuleException
   */
  public void setup() throws ModuleException {
    if (!validatorPathStrategy.isInitialized()) {
      registerSchemaAndTables();
    }
  }

  private void registerSchemaAndTables() throws ModuleException {
    try {
      NodeList result = (NodeList) XMLUtils.getXPathResult(
        getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), "/ns:siardArchive/ns:schemas/ns:schema",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < result.getLength(); i++) {
        Element element = (Element) result.item(i);
        String schemaName = element.getElementsByTagName("name").item(0).getTextContent();
        String schemaFolder = element.getElementsByTagName("folder").item(0).getTextContent();
        validatorPathStrategy.registerSchema(schemaName, schemaFolder);
        NodeList tables = (NodeList) XMLUtils.getXPathResult(
          getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
          "/ns:siardArchive/ns:schemas/ns:schema[ns:name/text()='" + schemaName + "']/ns:tables/ns:table",
          XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

        for (int j = 0; j < tables.getLength(); j++) {
          Element table = (Element) tables.item(j);
          final String tableName = table.getElementsByTagName("name").item(0).getTextContent();
          final String tableFolder = table.getElementsByTagName("folder").item(0).getTextContent();
          validatorPathStrategy.registerTable(schemaName, tableName, tableFolder);
        }
      }
    } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException e) {
      throw new ModuleException().withMessage("Error registering the schemas and tables").withCause(e);
    }
  }
}
