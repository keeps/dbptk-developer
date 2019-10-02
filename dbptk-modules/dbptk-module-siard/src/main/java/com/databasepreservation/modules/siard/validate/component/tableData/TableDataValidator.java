/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component.tableData;

import java.io.IOException;
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

  private List<String> P_641_ERRORS = new ArrayList<>();
  private List<String> P_642_ERRORS = new ArrayList<>();
  private List<String> P_645_ERRORS = new ArrayList<>();
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
    P_641_ERRORS = null;
    P_642_ERRORS = null;
    factory = null;
  }

  @Override
  public boolean validate() throws ModuleException {

    observer.notifyStartValidationModule(MODULE_NAME, P_64);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(P_64, MODULE_NAME);

    if (validateStoredExtensionFile()) {
      observer.notifyValidationStep(MODULE_NAME, P_641, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_641, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_641, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_641, ValidationReporterStatus.ERROR,
        "The table data for each table must be stored in an XML file.", P_641_ERRORS, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateRowElements()) {
      observer.notifyValidationStep(MODULE_NAME, P_642, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_642, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_642, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_642, ValidationReporterStatus.ERROR,
        "The table file consists of row elements containing the data of a line subdivided into the various columns.",
        P_642_ERRORS, MODULE_NAME);
      closeZipFile();
      return false;
    }

    getValidationReporter().validationStatus(P_643, ValidationReporterStatus.OK);

    observer.notifyValidationStep(MODULE_NAME, P_644, ValidationReporterStatus.SKIPPED);
    getValidationReporter().skipValidation(P_644, "Optional");

    if (validateLOBAttributes()) {
      observer.notifyValidationStep(MODULE_NAME, P_645, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_645, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_645, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_645, ValidationReporterStatus.ERROR,
        "If a large object is stored in a separate file, its cell element must have attributes file, length and digest",
        P_645_ERRORS, MODULE_NAME);
      closeZipFile();
      return false;
    }

    observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporterStatus.PASSED);
    closeZipFile();

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
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    List<String> SIARDXMLPaths = new ArrayList<>();

    try {
      NodeList schemaFolders = (NodeList) XMLUtils.getXPathResult(
        getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:folder/text()", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < schemaFolders.getLength(); i++) {
        String schemaFolderName = schemaFolders.item(i).getNodeValue();
        String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table/ns:folder/text()";
        xpathExpression = xpathExpression.replace("$1", schemaFolderName);
        NodeList tableFolders = (NodeList) XMLUtils.getXPathResult(
          getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), xpathExpression, XPathConstants.NODESET,
          Constants.NAMESPACE_FOR_METADATA);

        for (int j = 0; j < tableFolders.getLength(); j++) {
          final String path = validatorPathStrategy.getXMLTablePathFromFolder(schemaFolderName,
            tableFolders.item(j).getTextContent());
          SIARDXMLPaths.add(path);
        }
      }
    } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
      LOGGER.debug("Failed to validate {}({})", MODULE_NAME, P_641, e);
      return false;
    }

    for (String path : SIARDXMLPaths) {
      if (!getZipFileNames().contains(path)) {
        P_641_ERRORS.add("Missing XML file " + path);
      }
    }

    return P_641_ERRORS.isEmpty();
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
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    observer.notifyMessage(MODULE_NAME, P_642, "Validating row elements", ValidationReporterStatus.START);


    for (String zipFileName : getZipFileNames()) {
      String regexPattern = "^(content/schema[0-9]+/table[0-9]+/table[0-9]+)\\.xml$";
      if (zipFileName.matches(regexPattern)) {
        observer.notifyElementValidating(P_642, zipFileName);

        try {
          XMLStreamReader streamReader = factory.createXMLStreamReader(getZipInputStream(zipFileName));
          while (streamReader.hasNext()) {
            streamReader.next();
            if (streamReader.getEventType() == XMLStreamReader.START_ELEMENT) {
              final String tagName = streamReader.getLocalName();
              if (!(tagName.matches(TABLE_REGEX) || tagName.matches(ROW_REGEX) || tagName.matches(COLUMN_REGEX)
                || tagName.matches(ARRAY_REGEX) || tagName.matches(STRUCTURED_REGEX))) {
                P_642_ERRORS.add("Line " + streamReader.getLocation().getLineNumber() + "in " + zipFileName);
              }
            }
          }
        } catch (XMLStreamException e) {
          LOGGER.debug("Failed to validate {}({})", MODULE_NAME, P_642, e);
          return false;
        }
      }
    }

    observer.notifyMessage(MODULE_NAME, P_642, "Validating row elements", ValidationReporterStatus.FINISH);
    return P_642_ERRORS.isEmpty();
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
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    for (String zipFileName : getZipFileNames()) {
      final Matcher matcher = patternXSDFile.matcher(zipFileName);

      String schema = "", table = "";
      while (matcher.find()) {
        schema = matcher.group(1);
        table = matcher.group(2);

        try {
          NodeList nodeNames = (NodeList) XMLUtils.getXPathResult(getZipInputStream(zipFileName),
            "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element[@type='clobType' or @type='blobType']/@name",
            XPathConstants.NODESET, Constants.NAMESPACE_FOR_TABLE);
          for (int i = 0; i < nodeNames.getLength(); i++) {
            try {
              validateOutsideLOB(schema, table, nodeNames.item(i).getNodeValue());
            } catch (XMLStreamException e) {
              LOGGER.debug("Failed to validate {}({})", MODULE_NAME, P_645, e);
              return false;
            }
          }

        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          LOGGER.debug("Failed to validate {}({})", MODULE_NAME, P_645, e);
          return false;
        }
      }
    }

    return P_645_ERRORS.isEmpty();
  }

  private void validateOutsideLOB(final String schemaFolder, final String tableFolder, final String columnIndex)
    throws XMLStreamException {
    final String path = validatorPathStrategy.getXMLTablePathFromFolder(schemaFolder, tableFolder);

    XMLStreamReader streamReader = factory.createXMLStreamReader(getZipInputStream(path));
    boolean columnIndexFound = false;
    boolean validate = false;
    String tagName = "";
    ArrayList<Integer> eventTypes = new ArrayList<>();
    ArrayList<String> attributes = new ArrayList<>();
    while (streamReader.hasNext()) {
      streamReader.next();

      eventTypes.add(streamReader.getEventType());
      if (streamReader.getEventType() == XMLStreamReader.START_ELEMENT) {
        tagName = streamReader.getLocalName();
        if (tagName.equals(columnIndex)) {
          columnIndexFound = true;
          final int attributeCount = streamReader.getAttributeCount();
          for (int i = 0; i < attributeCount; i++) {
            attributes.add(streamReader.getAttributeLocalName(i));
          }
        }
      }

      if (streamReader.getEventType() == XMLStreamReader.END_ELEMENT) {
        if (eventTypes.size() == 2) {
          validate = true;
        }
        eventTypes.clear();
      }

      if (columnIndexFound && validate) {
        if (!validateRequiredLOBAttributes(attributes)) {
          P_645_ERRORS.add(ListUtils.convertListToStringWithSeparator(P_645_ERRORS_ATTRIBUTES, ", ") + " at " + path);
        }
        validate = false;
        columnIndexFound = false;
        attributes.clear();
      }
    }
  }

  private boolean validateRequiredLOBAttributes(ArrayList<String> attributes) {
    P_645_ERRORS_ATTRIBUTES = new ArrayList<>();

    if (attributes.isEmpty()) {
      P_645_ERRORS_ATTRIBUTES.add("Expecting at least 3 attributes (file, length and digest) found none ");
      return false;
    }

    if (attributes.size() < 3) {
      P_645_ERRORS_ATTRIBUTES.add("Expecting at least 3 attributes (file, length and digest) found only: "
        + ListUtils.convertListToStringWithSeparator(attributes, ", "));
      return false;
    }

    boolean matchesFile, matchesLength, matchesDigest;
    matchesFile = attributes.contains("file");
    matchesLength = attributes.contains("length");
    matchesDigest = attributes.contains("digest");

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
  }

  private void compileRegexPattern() {
    patternXSDFile = Pattern.compile("^content/(schema[0-9]+)/(table[0-9]+)/table[0-9]+\\.xsd$");
  }
}
