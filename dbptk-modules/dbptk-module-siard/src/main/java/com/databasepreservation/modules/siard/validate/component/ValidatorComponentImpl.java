/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component;

import static com.databasepreservation.model.reporters.ValidationReporter.Indent;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.common.ValidationObserver;
import com.databasepreservation.common.ValidatorPathStrategy;
import com.databasepreservation.common.ZipFileManagerStrategy;
import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.validate.components.ValidatorComponent;
import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public abstract class ValidatorComponentImpl implements ValidatorComponent {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValidatorComponentImpl.class);

  protected Path path = null;
  private Reporter reporter = null;
  protected ValidationObserver observer = null;
  private ValidationReporter validationReporter = null;
  protected ZipFileManagerStrategy zipFileManagerStrategy = null;
  protected ValidatorPathStrategy validatorPathStrategy = null;
  protected List<String> allowedUDTs = null;

  protected Path getSIARDPackagePath() {
    return path;
  }

  @Override
  public void setSIARDPath(Path path) {
    if (this.path == null) {
      this.path = path;
    }
  }

  protected Reporter getReporter() {
    return reporter;
  }

  @Override
  public void setReporter(Reporter reporter) {
    if (this.reporter == null) {
      this.reporter = reporter;
    }
  }

  protected ValidationReporter getValidationReporter() {
    return validationReporter;
  }

  @Override
  public void setValidationReporter(ValidationReporter validationReporter) {
    if (this.validationReporter == null) {
      this.validationReporter = validationReporter;
    }
  }

  @Override
  public void setValidatorPathStrategy(ValidatorPathStrategy validatorPathStrategy) {
    if (this.validatorPathStrategy == null) {
      this.validatorPathStrategy = validatorPathStrategy;
    }
  }

  @Override
  public void setAllowedUTD(List<String> allowedUDTs) {
    if (this.allowedUDTs == null) {
      this.allowedUDTs = allowedUDTs;
    }
  }

  @Override
  public void setZipFileManager(ZipFileManagerStrategy manager) {
    if (this.zipFileManagerStrategy == null) {
      this.zipFileManagerStrategy = manager;
    }
  }

  @Override
  public void clean() {
    // Do nothing override
  }

  protected void validationFailed(String ID, ValidationReporterStatus status, String message, List<String> errors,
    String moduleName) {
    for (String error : errors) {
      validationReporter.validationStatus(ID, status, message, error);
    }
    validationReporter.moduleValidatorFinished(moduleName, ValidationReporterStatus.FAILED);
  }

  protected void validationFailed(String ID, String moduleName, String details) {
    validationReporter.validationStatus(ID, ValidationReporterStatus.ERROR, details);
    validationReporter.moduleValidatorFinished(moduleName, ValidationReporterStatus.FAILED);
  }

  protected void validationFailed(String ID, String moduleName, String details, List<String> errors) {
    validationReporter.validationStatus(ID, ValidationReporterStatus.ERROR, details);
    for (String error : errors) {
      validationReporter.validationStatus(ValidationReporterStatus.ERROR, error, Indent.TAB_2);
    }
    validationReporter.moduleValidatorFinished(moduleName, ValidationReporterStatus.FAILED);
  }

  protected void validationFailed(String ID, String moduleName, String details, String errorMessage,
    List<String> errors) {
    validationReporter.validationStatus(ID, ValidationReporterStatus.ERROR, details);
    for (String error : errors) {
      validationReporter.validationStatus(errorMessage, ValidationReporterStatus.ERROR, error, Indent.TAB_2);
    }
    validationReporter.moduleValidatorFinished(moduleName, ValidationReporterStatus.FAILED);
  }

  protected void validationFailed(String ID, String moduleName) {
    validationReporter.validationStatus(ID, ValidationReporterStatus.ERROR);
    validationReporter.moduleValidatorFinished(moduleName, ValidationReporterStatus.FAILED);
  }

  /**
   * Lazy loading, needs the validatorPathStrategy otherwise it will throw a
   * nullPointerException
   *
   */
  public void setup() throws ModuleException {
    if (!validatorPathStrategy.isReady()) {
      registerSchemaAndTables();
    }
  }

  @Override
  public void setObserver(ValidationObserver observer) {
    if (this.observer == null) {
      this.observer = observer;
    }
  }

  private boolean registerSchemaAndTables() throws ModuleException {
    InputStream zipInputStream = null;
    try {
      zipInputStream = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath());

      NodeList result = (NodeList) XMLUtils.getXPathResult(zipInputStream, "/ns:siardArchive/ns:schemas/ns:schema",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < result.getLength(); i++) {
        Element element = (Element) result.item(i);
        String schemaName = element.getElementsByTagName("name").item(0).getTextContent();
        String schemaFolder = element.getElementsByTagName("folder").item(0).getTextContent();
        validatorPathStrategy.registerSchema(schemaName, schemaFolder);
        InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath());
        NodeList tables = (NodeList) XMLUtils.getXPathResult(is,
          "/ns:siardArchive/ns:schemas/ns:schema[ns:name/text()='" + schemaName + "']/ns:tables/ns:table",
          XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

        for (int j = 0; j < tables.getLength(); j++) {
          Element table = (Element) tables.item(j);
          final String tableName = table.getElementsByTagName("name").item(0).getTextContent();
          final String tableFolder = table.getElementsByTagName("folder").item(0).getTextContent();
          validatorPathStrategy.registerTable(schemaName, tableName, tableFolder);
        }

        is.close();
      }
    } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException e) {
      throw new ModuleException().withMessage("Error registering the schemas and tables").withCause(e);
    } finally {
      try {
        if (zipInputStream != null) {
          zipInputStream.close();
        }
      } catch (IOException e) {
        LOGGER.debug("Could not close the stream after an error occurred", e);
      }
    }

    return true;
  }
}
