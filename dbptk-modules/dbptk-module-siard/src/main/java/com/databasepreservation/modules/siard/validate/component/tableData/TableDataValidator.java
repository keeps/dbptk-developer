/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component.tableData;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;
import com.databasepreservation.utils.ListUtils;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class TableDataValidator extends ValidatorComponentImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableDataValidator.class);

  private final String MODULE_NAME;
  private XMLInputFactory factory;
  private static final String P_64 = "T_6.4";
  private static final String P_641 = "T_6.4-1";
  private static final String P_642 = "T_6.4-2";
  private static final String P_643 = "T_6.4-3";
  private static final String P_644 = "T_6.4-4";
  private static final String P_645 = "T_6.4-5";

  private boolean P_645_HAS_ERRORS = false;
  private List<String> P_645_ERRORS_ATTRIBUTES;

  private static final String TABLE_REGEX = "^table$";
  private static final String ROW_REGEX = "^row$";
  private static final String COLUMN_REGEX = "^c[1-9]([0-9]+)?$";
  private static final String ARRAY_REGEX = "^a[1-9]([0-9]+)?$";
  private static final String STRUCTURED_REGEX = "^u[1-9]([0-9]+)?$";

  private static Pattern patternXSDFile;

  public TableDataValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    factory = XMLInputFactory.newInstance();
    compileRegexPattern();
  }

  @Override
  public void clean() {
    factory = null;
    if (P_645_ERRORS_ATTRIBUTES != null) {
      P_645_ERRORS_ATTRIBUTES.clear();
    }
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, P_64);
    getValidationReporter().moduleValidatorHeader(P_64, MODULE_NAME);

    if (validateStoredExtensionFile()) {
      observer.notifyValidationStep(MODULE_NAME, P_641, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_641, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_641, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporterStatus.FAILED);
      return false;
    }

    if (validateRowElements()) {
      observer.notifyValidationStep(MODULE_NAME, P_642, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_642, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_642, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporterStatus.FAILED);
      return false;
    }

    //getValidationReporter().validationStatus(P_643, ValidationReporterStatus.OK);

    observer.notifyValidationStep(MODULE_NAME, P_644, ValidationReporterStatus.SKIPPED);
    getValidationReporter().skipValidation(P_644, "Optional");

    if (validateLOBAttributes()) {
      observer.notifyValidationStep(MODULE_NAME, P_645, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_645, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_645, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporterStatus.FAILED);
      return false;
    }

    observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporterStatus.PASSED);

    return true;
  }

  /**
   * T_6.4-1
   *
   * The table data for each table must be stored in an XML file.
   *
   * @return true if valid otherwise false
   */
  private boolean validateStoredExtensionFile() {
    boolean valid = true;
    List<String> SIARDXMLPaths = new ArrayList<>();

    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      NodeList schemaFolders = (NodeList) XMLUtils.getXPathResult(is,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:folder/text()", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < schemaFolders.getLength(); i++) {
        String schemaFolderName = schemaFolders.item(i).getNodeValue();
        String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table/ns:folder/text()";
        xpathExpression = xpathExpression.replace("$1", schemaFolderName);
        try (InputStream tableFoldersInputStream = zipFileManagerStrategy.getZipInputStream(path,
          validatorPathStrategy.getMetadataXMLPath())) {
          NodeList tableFolders = (NodeList) XMLUtils.getXPathResult(tableFoldersInputStream, xpathExpression,
            XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

          for (int j = 0; j < tableFolders.getLength(); j++) {
            final String path = validatorPathStrategy.getXMLTablePathFromFolder(schemaFolderName,
              tableFolders.item(j).getTextContent());
            SIARDXMLPaths.add(path);
          }
        }
      }
    } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
      LOGGER.debug("Failed to validate {}({})", MODULE_NAME, P_641, e);
      getValidationReporter().validationStatus(P_641, ValidationReporterStatus.ERROR,
              "Failed to validate due to an exception on " + MODULE_NAME, "Please check the log file for more information");
      return false;
    }

    final List<String> zipArchiveEntriesPath = zipFileManagerStrategy.getZipArchiveEntriesPath(path);

    for (String path : SIARDXMLPaths) {
      if (!zipArchiveEntriesPath.contains(path)) {
        getValidationReporter().validationStatus(P_641, ValidationReporterStatus.ERROR,
          "The table data for each table must be stored in an XML file.", "Missing XML file " + path);
        valid = false;
      }
    }

    return valid;
  }

  /**
   * T_6.4-2
   *
   * The table file consists of row elements containing the data of a line
   * subdivided into the various columns (c1, c2 ...).
   * 
   * @return true if valid otherwise false
   */
  private boolean validateRowElements() {
    boolean valid = true;
    observer.notifyMessage(MODULE_NAME, P_642, "Validating row elements", ValidationReporterStatus.START);

    final List<String> zipArchiveEntriesPath = zipFileManagerStrategy.getZipArchiveEntriesPath(path);

    for (String zipFileName : zipArchiveEntriesPath) {
      String regexPattern = "^(content/schema[0-9]+/table[0-9]+/table[0-9]+)\\.xml$";
      if (zipFileName.matches(regexPattern)) {
        observer.notifyElementValidating(P_642, zipFileName);

        try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, zipFileName)) {
          XMLStreamReader streamReader = factory.createXMLStreamReader(is);
          while (streamReader.hasNext()) {
            streamReader.next();
            if (streamReader.getEventType() == XMLStreamReader.START_ELEMENT) {
              final String tagName = streamReader.getLocalName();
              if (!(tagName.matches(TABLE_REGEX) || tagName.matches(ROW_REGEX) || tagName.matches(COLUMN_REGEX)
                || tagName.matches(ARRAY_REGEX) || tagName.matches(STRUCTURED_REGEX))) {
                getValidationReporter().validationStatus(P_642, ValidationReporterStatus.ERROR,
                  "The table file consists of row elements containing the data of a line subdivided into the various columns.",
                  "Line " + streamReader.getLocation().getLineNumber() + " in " + zipFileName);
                valid = false;
              }
            }
          }
        } catch (XMLStreamException | IOException e) {
          LOGGER.debug("Failed to validate {}({})", MODULE_NAME, P_642, e);
          getValidationReporter().validationStatus(P_642, ValidationReporterStatus.ERROR,
                  "Failed to validate due to an exception on " + MODULE_NAME, "Please check the log file for more information");
          return false;
        }
      }
    }

    observer.notifyMessage(MODULE_NAME, P_642, "Validating row elements", ValidationReporterStatus.FINISH);
    return valid;
  }

  /**
   * T_6.4-5
   *
   * The decision, when to store large object data in separate files rather than
   * inlining them is at the discretion of the software producing the SIARD
   * archive. To avoid creating empty folders, folders are only created when they
   * are necessary, i.e. contain data. If a large object is stored in a separate
   * file, its cell element must have attributes file, length and digest. Here
   * file is a “file:” URI relative to the lobFolder element of the column or
   * attribute metadata. The length contains the length in bytes (for BLOBs) or
   * characters (for CLOBs or XMLs). The digest records a message digest over the
   * LOB file, making it possible to check integrity of the SIARD archive, even
   * when some LOBs are stored externally.
   * 
   * @return true is valid otherwise false
   */
  private boolean validateLOBAttributes() {
    final List<String> zipArchiveEntriesPath = zipFileManagerStrategy.getZipArchiveEntriesPath(path);

    for (String zipFileName : zipArchiveEntriesPath) {
      final Matcher matcher = patternXSDFile.matcher(zipFileName);

      String schema;
      String table;
      while (matcher.find()) {
        schema = matcher.group(1);
        table = matcher.group(2);

        try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, zipFileName)) {
          NodeList nodeNames = (NodeList) XMLUtils.getXPathResult(is,
            "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element[@type='clobType' or @type='blobType']/@name",
            XPathConstants.NODESET, Constants.NAMESPACE_FOR_TABLE);
          for (int i = 0; i < nodeNames.getLength(); i++) {
            try {
              validateOutsideLOB(schema, table, nodeNames.item(i).getNodeValue());
            } catch (XMLStreamException | IOException e) {
              LOGGER.debug("Failed to validate {}({})", MODULE_NAME, P_645, e);
              getValidationReporter().validationStatus(P_645, ValidationReporterStatus.ERROR,
                      "Failed to validate due to an exception on " + MODULE_NAME, "Please check the log file for more information");
              return false;
            }
          }

        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          LOGGER.debug("Failed to validate {}({})", MODULE_NAME, P_645, e);
          getValidationReporter().validationStatus(P_645, ValidationReporterStatus.ERROR,
                  "Failed to validate due to an exception on " + MODULE_NAME, "Please check the log file for more information");
          return false;
        }
      }
    }

    return !P_645_HAS_ERRORS;
  }

  private void validateOutsideLOB(final String schemaFolder, final String tableFolder, final String columnIndex)
          throws XMLStreamException, IOException {
    final String zipPath = validatorPathStrategy.getXMLTablePathFromFolder(schemaFolder, tableFolder);
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, zipPath)) {

      XMLStreamReader streamReader = factory.createXMLStreamReader(is);
      StringBuilder text = new StringBuilder();
      ArrayList<String> attributes = new ArrayList<>();
      while (streamReader.hasNext()) {
        streamReader.next();

        if (streamReader.getEventType() == XMLStreamReader.START_ELEMENT) {
          if (columnIndex.equals(streamReader.getLocalName())) {
            final int attributeCount = streamReader.getAttributeCount();
            for (int i = 0; i < attributeCount; i++) {
              attributes.add(streamReader.getAttributeLocalName(i));
            }
          }
        }

        if (streamReader.getEventType() == XMLStreamReader.CHARACTERS) {
          text.append(streamReader.getText().trim());
        }

        if (streamReader.getEventType() == XMLStreamReader.END_ELEMENT) {
          if (columnIndex.equals(streamReader.getLocalName())) {
            if (!validateRequiredLOBAttributes(attributes, text.toString())) {
              getValidationReporter().validationStatus(P_645, ValidationReporterStatus.ERROR,
                "If a large object is stored in a separate file, its cell element must have attributes file, length and digest",
                ListUtils.convertListToStringWithSeparator(P_645_ERRORS_ATTRIBUTES, ", ") + " on " + columnIndex
                  + " at " + path);
              P_645_HAS_ERRORS = true;
            }
          }
          text = new StringBuilder();
          attributes = new ArrayList<>();
        }
      }
    }
  }

  private boolean validateRequiredLOBAttributes(final ArrayList<String> attributes, final String text) {
    P_645_ERRORS_ATTRIBUTES = new ArrayList<>();

    if (attributes.contains("file")) {
      boolean matchesFile;
      boolean matchesLength;
      boolean matchesDigest;
      matchesFile = attributes.contains("file");
      matchesLength = attributes.contains("length");
      matchesDigest = attributes.contains("digest");

      if (StringUtils.isBlank(text)) {
        if (!matchesFile) {
          P_645_ERRORS_ATTRIBUTES.add("Missing file attribute");
        }

        if (!matchesLength) {
          P_645_ERRORS_ATTRIBUTES.add("Missing length attribute");
        }

        if (!matchesDigest) {
          P_645_ERRORS_ATTRIBUTES.add("Missing digest attribute");
        }
        return P_645_ERRORS_ATTRIBUTES.isEmpty();
      } else {
        P_645_ERRORS_ATTRIBUTES.add("Found an outside lob however the content is filled");
        return false;
      }
    } else {
      if (attributes.isEmpty()) {
        return true;
      } else {
        P_645_ERRORS_ATTRIBUTES.add("Found an inline lob however with attributes: " + attributes.toString());
        return false;
      }
    }
  }

  private void compileRegexPattern() {
    patternXSDFile = Pattern.compile("^content/(schema[0-9]+)/(table[0-9]+)/table[0-9]+\\.xsd$");
  }
}
