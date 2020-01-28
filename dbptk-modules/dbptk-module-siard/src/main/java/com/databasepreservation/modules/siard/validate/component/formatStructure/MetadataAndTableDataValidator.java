/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component.formatStructure;

import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.model.validator.SIARDContent;
import com.databasepreservation.modules.siard.validate.common.model.AdvancedOrStructuredColumn;
import com.databasepreservation.modules.siard.validate.common.model.Attribute;
import com.databasepreservation.modules.siard.validate.common.model.Field;
import com.databasepreservation.modules.siard.validate.common.model.Type;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class MetadataAndTableDataValidator extends ValidatorComponentImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataAndTableDataValidator.class);

  private final String MODULE_NAME;
  private XMLInputFactory factory;
  private static final String P_43 = "4.3";
  private static final String P_431 = "P_4.3-1";
  private static final String P_432 = "P_4.3-2";
  private static final String P_433 = "P_4.3-3";
  private static final String P_434 = "P_4.3-4";
  private static final String P_435 = "P_4.3-5";
  private static final String P_436 = "P_4.3-6";
  private static final String P_437 = "P_4.3-7";
  private static final String P_438 = "P_4.3-8";
  private static final String P_439 = "P_4.3-9";
  private static final String P_4310 = "P_4.3-10";
  private static final String A_P_4310 = "A_P_4.3-10";

  private List<String> P_431_ERRORS = new ArrayList<>();
  private List<String> P_432_ERRORS = new ArrayList<>();
  private List<String> P_433_ERRORS = new ArrayList<>();
  private List<String> P_434_ERRORS = new ArrayList<>();
  private List<String> P_435_ERRORS = new ArrayList<>();
  private List<String> P_436_ERRORS = new ArrayList<>();
  private List<String> P_439_ERRORS = new ArrayList<>();
  private List<String> P_4310_ERRORS = new ArrayList<>();
  private List<String> A_P_4310_ERRORS = new ArrayList<>();

  private static HashMap<String, List<String>> SQL2008TypeMatchXMLType;
  private HashMap<SIARDContent, HashMap<String, String>> sql2008Type = new HashMap<>();
  private HashMap<SIARDContent, HashMap<String, String>> arrayType = new HashMap<>();
  private HashMap<SIARDContent, HashMap<String, AdvancedOrStructuredColumn>> advancedOrStructuredDataType = new HashMap<>();
  private TreeMap<SIARDContent, HashMap<String, String>> numberOfNullable = new TreeMap<>();
  private TreeMap<SIARDContent, Integer> numberOfRows = new TreeMap<>();
  private List<String> skipped = new ArrayList<>();
  private List<String> warnings = new ArrayList<>();
  private HashMap<String, Type> types = new HashMap<>();

  public MetadataAndTableDataValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    populateSQL2008Types();
    factory = XMLInputFactory.newInstance();
  }

  @Override
  public void clean() {
    P_431_ERRORS.clear();
    P_432_ERRORS.clear();
    P_433_ERRORS.clear();
    P_434_ERRORS.clear();
    P_435_ERRORS.clear();
    P_436_ERRORS.clear();
    P_4310_ERRORS.clear();
    A_P_4310_ERRORS.clear();
    sql2008Type.clear();
    arrayType.clear();
    advancedOrStructuredDataType.clear();
    numberOfNullable.clear();
    numberOfRows.clear();
    skipped.clear();
    warnings.clear();
    types.clear();
    factory = null;
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, P_43);

    try {
      obtainSQL2008TypeForEachColumn();
      getTypesFromMetadataXML();
    } catch (ParserConfigurationException | SAXException | XPathExpressionException | IOException e) {
      LOGGER.debug("Failed to fetch data for validation component {}", MODULE_NAME, e);
      getValidationReporter().validationStatus(P_43, ValidationReporterStatus.ERROR,
        "Failed to fetch data for validation component " + MODULE_NAME,
        "Please check the log file for more information");
      return false;
    }

    getValidationReporter().moduleValidatorHeader(P_43, MODULE_NAME);

    if (validateMetadataStructure()) {
      observer.notifyValidationStep(MODULE_NAME, P_431, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_431, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_431, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_431, ValidationReporterStatus.ERROR,
        "The metadata.xml must be identical to that in the content/", P_431_ERRORS, MODULE_NAME);
      return false;
    }

    if (validateColumnCount()) {
      observer.notifyValidationStep(MODULE_NAME, P_432, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_432, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_432, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_432, ValidationReporterStatus.ERROR,
        "The number of columns in a table specified in metadata.xml must be identical to that in the corresponding table[number].xsd file",
        P_432_ERRORS, MODULE_NAME);
      return false;
    }

    if (validateDataTypeInformation()) {
      observer.notifyValidationStep(MODULE_NAME, P_433, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_433, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_433, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_433, ValidationReporterStatus.ERROR,
        "The data type information on the column definitions in metadata.xml must be identical to that in the corresponding table[number].xsd file",
        P_433_ERRORS, MODULE_NAME);
      return false;
    }

    boolean value = validateDistinctXMLDataTypeConversion();
    for (String warning : warnings) {
      getValidationReporter().warning(P_434, "Not allowed user-defined data type", warning);
    }

    if (value) {
      if (!warnings.isEmpty()) {
        for (String warning : warnings) {
          getValidationReporter().warning(P_436, warning, "Data type not allowed");
        }
      }
      skip(P_434);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_434, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_434, ValidationReporterStatus.ERROR,
        "The named DISTINCT data types are converted to the XML data type in the table[number].xsd schema files which would be used for representing their base types.",
        P_434_ERRORS, MODULE_NAME);
      return false;
    }

    if (validateArrayXMLDataTypeConversion()) {
      skip(P_435);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_435, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_435, ValidationReporterStatus.ERROR,
        "Arrays are converted in the table[number].xsd schema files into a sequence of structured XML elements which are converted to the XML data type corresponding to the base type of the array.",
        P_435_ERRORS, MODULE_NAME);
      return false;
    }

    if (validateUDTXMLDataTypeConversion()) {
      if (!warnings.isEmpty()) {
        for (String warning : warnings) {
          getValidationReporter().warning(P_436, warning, "Data type not allowed");
        }
      }
      skip(P_436);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_436, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_436, ValidationReporterStatus.ERROR,
        "The named user-defined data type (UDT) is converted in the table[number].xsd schema files into a sequence of structured XML elements which are converted to the XML data type corresponding to the type of each attribute.",
        P_436_ERRORS, MODULE_NAME);
      return false;
    }

    boolean result = validateNillableInformation();
    for (String warning : warnings) {
      getValidationReporter().warning(P_437,
        "The nullable information on the column definitions in the metadata.xml must\n"
          + "be identical to that in the corresponding table[number].xsd file.",
        warning);
    }
    if (result) {
      observer.notifyValidationStep(MODULE_NAME, P_437, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_437, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_437, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_437, MODULE_NAME);
      return false;
    }

    if (validateColumnSequence()) {
      observer.notifyValidationStep(MODULE_NAME, P_438, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_438, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_438, ValidationReporterStatus.OK);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_438, MODULE_NAME);
      return false;
    }

    if (validateFieldSequenceInTableXSD()) {
      observer.notifyValidationStep(MODULE_NAME, P_439, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_439, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_439, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_439, ValidationReporterStatus.ERROR,
        "The field sequence in the table definition of the metadata.xml must be identical to the field sequence in the corresponding table[number].xsd.",
        P_439_ERRORS, MODULE_NAME);
      return false;
    }

    if (validateNumberOfLinesInATable()) {
      observer.notifyValidationStep(MODULE_NAME, P_4310, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_4310, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_4310, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_4310, ValidationReporterStatus.ERROR,
        "The number of lines in a table in metadata.xml must fit into the area specified in the corresponding table[number].xsd. The number of lines in a table in metadata.xml must be identical to the number of lines in the corresponding table[number].xml.",
        P_4310_ERRORS, MODULE_NAME);
      return false;
    }

    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_P_4310, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_P_4310, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      if (validateNumberOfRows()) {
        observer.notifyValidationStep(MODULE_NAME, A_P_4310, ValidationReporterStatus.OK);
        getValidationReporter().validationStatus(A_P_4310, ValidationReporterStatus.OK);
      } else {
        observer.notifyValidationStep(MODULE_NAME, A_P_4310, ValidationReporterStatus.ERROR);
        observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
        validationFailed(A_P_4310, ValidationReporterStatus.ERROR, "", P_4310_ERRORS, MODULE_NAME);
        return false;
      }
    }

    observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporterStatus.PASSED);
    zipFileManagerStrategy.closeZipFile();
    return true;
  }

  /**
   * P_4.3-1
   *
   * The structure prescribed in the metadata.xml must be identical to that in the
   * content/ folder.
   *
   * @return true if valid otherwise false
   */
  private boolean validateMetadataStructure() {
    InputStream is = null;
    try {
      is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath());
      final NodeList nodeList = (NodeList) XMLUtils.getXPathResult(is, "/ns:siardArchive/ns:schemas/ns:schema",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
      is.close();
      for (int i = 0; i < nodeList.getLength(); i++) {
        Element schema = (Element) nodeList.item(i);
        String schemaFolderName = schema.getElementsByTagName("folder").item(0).getTextContent();
        NodeList tableNodes = schema.getElementsByTagName("table");
        for (int j = 0; j < tableNodes.getLength(); j++) {
          Element table = (Element) tableNodes.item(j);
          String tableFolderName = table.getElementsByTagName("folder").item(0).getTextContent();
          final String pathToValidate = createPath("content", schemaFolderName, tableFolderName);
          if (!checkRelativePathExists(pathToValidate)) {
            P_431_ERRORS.add(pathToValidate);
          }
        }
      }
    } catch (IOException | XPathExpressionException | SAXException | ParserConfigurationException e) {
      LOGGER.debug("Failed to validate {}", P_431, e);
      getValidationReporter().validationStatus(P_431, ValidationReporterStatus.ERROR,
        "Failed to validate due to an exception on " + MODULE_NAME, "Please check the log file for more information");
      return false;
    } finally {
      if (is != null) {
        try {
          is.close();
        } catch (IOException e) {
          LOGGER.debug("Could not close the stream after an error occurred", e);
        }
      }
    }

    return P_431_ERRORS.isEmpty();
  }

  /**
   * P_4.3-2
   *
   * The number of columns in a table specified in metadata.xml must be identical
   * to that in the corresponding table[number].xsd file.
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumnCount() {
    HashMap<SIARDContent, Integer> columnCount = new HashMap<>();
    List<SIARDContent> entries = new ArrayList<>();
    InputStream is = null;
    InputStream anotherIs = null;
    try {
      is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath());
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(is, "/ns:siardArchive/ns:schemas/ns:schema",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
      is.close();

      for (int i = 0; i < nodes.getLength(); i++) {
        Element schema = (Element) nodes.item(i);
        String schemaFolderName = schema.getElementsByTagName("folder").item(0).getTextContent();
        NodeList tableNodes = schema.getElementsByTagName("table");
        for (int j = 0; j < tableNodes.getLength(); j++) {
          Element table = (Element) tableNodes.item(j);
          String tableFolderName = table.getElementsByTagName("folder").item(0).getTextContent();
          NodeList columnsNodes = table.getElementsByTagName("columns");
          Element columns = (Element) columnsNodes.item(0);
          NodeList columnNodes = columns.getElementsByTagName("column");
          SIARDContent content = new SIARDContent(schemaFolderName, tableFolderName);
          entries.add(content);
          columnCount.put(content, columnNodes.getLength());
        }
      }

      for (SIARDContent content : entries) {
        final String XSDPath = validatorPathStrategy.getXSDTablePathFromFolder(content.getSchema(), content.getTable());
        anotherIs = zipFileManagerStrategy.getZipInputStream(path, XSDPath);
        final String evaluate = (String) XMLUtils.getXPathResult(anotherIs,
          "count(/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element)", XPathConstants.STRING, null);
        int value = Integer.parseInt(evaluate);

        if (!columnCount.get(content).equals(value)) {
          P_432_ERRORS.add(XSDPath);
        }

        anotherIs.close();
      }
    } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
      LOGGER.debug("Failed to validate {}", P_432, e);
      getValidationReporter().validationStatus(P_432, ValidationReporterStatus.ERROR,
        "Failed to validate due to an exception on " + MODULE_NAME, "Please check the log file for more information");
      return false;
    } finally {
      if (is != null) {
        try {
          is.close();
        } catch (IOException e) {
          LOGGER.debug("Could not close the stream after an error occurred", e);
        }
      }
      if (anotherIs != null) {
        try {
          anotherIs.close();
        } catch (IOException e) {
          LOGGER.debug("Could not close the stream after an error occurred", e);
        }
      }
    }

    return P_432_ERRORS.isEmpty();
  }

  /**
   * P_4.3-3
   *
   * The data type information on the column definitions in metadata.xml must be
   * identical to that in the corresponding table[number].xsd file.
   *
   * @return true if valid otherwise false
   */
  private boolean validateDataTypeInformation() {
    for (Map.Entry<SIARDContent, HashMap<String, String>> entry : sql2008Type.entrySet()) {
      try {
        final List<String> errors = compareSQL2008DataTypeWithXMLType(entry.getKey(), entry.getValue());
        P_433_ERRORS.addAll(errors);
      } catch (SAXException | ParserConfigurationException | IOException | XPathExpressionException e) {
        LOGGER.debug("Failed to validate {}", P_433, e);
        getValidationReporter().validationStatus(P_433, ValidationReporterStatus.ERROR,
          "Failed to validate due to an exception on " + MODULE_NAME, "Please check the log file for more information");
        return false;
      }
    }

    return P_433_ERRORS.isEmpty();
  }

  /**
   * P_4.3-4
   *
   * The named DISTINCT data types are converted to the XML data type in the
   * table[number].xsd schema files which would be used for representing their
   * base types.
   *
   * @return true if valid otherwise false
   */
  private boolean validateDistinctXMLDataTypeConversion() {
    warnings = new ArrayList<>();

    for (Type type : types.values()) {
      if (type.getCategory().equalsIgnoreCase("distinct")) {
        if (!allowedUDTs.contains(type.getName())) {
          warnings.add(type.getName());
        }
      }
    }

    try {
      HashMap<SIARDContent, HashMap<String, String>> distinctTypes = new HashMap<>();
      // Obtain table[number] where the Type is being used
      for (Map.Entry<SIARDContent, HashMap<String, AdvancedOrStructuredColumn>> entry : advancedOrStructuredDataType
        .entrySet()) {
        for (Map.Entry<String, AdvancedOrStructuredColumn> columnDataType : entry.getValue().entrySet()) {
          if (types.get(columnDataType.getValue().getType()) != null) {
            if (types.get(columnDataType.getValue().getType()).getCategory().equalsIgnoreCase("distinct")) {
              HashMap<String, String> internal = new HashMap<>();
              internal.put(columnDataType.getKey(), types.get(columnDataType.getValue().getType()).getBase());
              distinctTypes.put(entry.getKey(), internal);
            }
          }
        }
      }

      if (distinctTypes.isEmpty()) {
        skipped = new ArrayList<>();
        skipped.add("No distinct type found");
      } else {
        // Compare to the base type with the XML type from table[number].xsd
        for (Map.Entry<SIARDContent, HashMap<String, String>> entry : distinctTypes.entrySet()) {
          final List<String> errors = compareSQL2008DataTypeWithXMLType(entry.getKey(), entry.getValue());
          P_434_ERRORS.addAll(errors);
        }
      }

    } catch (SAXException | ParserConfigurationException | IOException | XPathExpressionException e) {
      LOGGER.debug("Failed to validate {}", P_434, e);
      getValidationReporter().validationStatus(P_434, ValidationReporterStatus.ERROR,
        "Failed to validate due to an exception on " + MODULE_NAME, "Please check the log file for more information");
      return false;
    }

    return P_434_ERRORS.isEmpty();
  }

  /**
   * P_4.3-5
   *
   * The ARRAY container type is converted in the table[number].xsd schema files
   * into a sequence of structured XML elements <a1>, <a2>, … which are converted
   * to the XML data type corresponding to the base type of the array.
   * 
   */
  private boolean validateArrayXMLDataTypeConversion() {
    warnings.clear();
    skipped.clear();
    if (arrayType.isEmpty()) {
      skipped.add("No Array type found");
      return true;
    }

    for (Map.Entry<SIARDContent, HashMap<String, String>> entry : arrayType.entrySet()) {
      final String XSDPath = validatorPathStrategy.getXSDTablePathFromFolder(entry.getKey().getSchema(),
        entry.getKey().getTable());
      for (Map.Entry<String, String> column : entry.getValue().entrySet()) {
        try (InputStream zipInputStream = zipFileManagerStrategy.getZipInputStream(path, XSDPath)) {
          final NodeList nodeList = (NodeList) XMLUtils.getXPathResult(zipInputStream,
            "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element[@name='" + column.getKey()
              + "']/xs:complexType/xs:sequence/xs:element",
            XPathConstants.NODESET, null);

          for (int i = 0; i < nodeList.getLength(); i++) {
            Element element = (Element) nodeList.item(i);
            final String xmlType = element.getAttributes().getNamedItem("type").getNodeValue();
            if (!validateSQL2008TypeWithXMLType(entry.getValue().get(column.getKey()), xmlType)) {
              P_435_ERRORS
                .add(obtainErrorMessage(entry.getValue().get(column.getKey()), xmlType, column.getKey(), XSDPath));
            }
          }
        } catch (IOException | XPathExpressionException | SAXException | ParserConfigurationException e) {
          LOGGER.debug("Failed to validate {}", P_435, e);
          getValidationReporter().validationStatus(P_435, ValidationReporterStatus.ERROR,
            "Failed to validate due to an exception on " + MODULE_NAME,
            "Please check the log file for more information");
          return false;
        }
      }
    }

    return P_435_ERRORS.isEmpty();
  }

  /**
   * P_4.3-6
   *
   * The named user-defined data type (UDT) is converted in the table[number].xsd
   * schema files into a sequence of structured XML elements <u1>, <u2>, … which
   * are converted to the XML data type corresponding to the type of each
   * attribute.
   *
   */
  private boolean validateUDTXMLDataTypeConversion() {
    warnings.clear();
    skipped.clear();

    if (advancedOrStructuredDataType.isEmpty()) {
      skipped.add("No UDT type found");
      return true;
    }

    for (Type type : types.values()) {
      if (type.getCategory().equalsIgnoreCase("udt")) {
        if (!allowedUDTs.contains(type.getName())) {
          warnings.add(type.getName());
        }
      }
    }

    // Obtain table[number] where the Type is being used
    for (Map.Entry<SIARDContent, HashMap<String, AdvancedOrStructuredColumn>> entry : advancedOrStructuredDataType
      .entrySet()) {
      for (Map.Entry<String, AdvancedOrStructuredColumn> advancedOrStructuredColumn : entry.getValue().entrySet()) {
        final AdvancedOrStructuredColumn value = advancedOrStructuredColumn.getValue();
        if (types.get(value.getType()) != null) {
          if (types.get(value.getType()).getCategory().equalsIgnoreCase("udt")) {
            final List<Attribute> attributes = types.get(value.getType()).getAttributes();
            HashMap<String, String> map = new HashMap<>();
            int index = 1;
            for (Attribute attribute : attributes) {
              if (StringUtils.isNotBlank(attribute.getType())) {
                map.put("u" + index, attribute.getType());
              } else {
                String type = attribute.getTypeSchema() + "." + attribute.getTypeName();
                String rootIndex = "u" + index;
                final List<ImmutablePair<String, String>> pairs = getParentDataType(type, rootIndex);
                for (ImmutablePair<String, String> pair : pairs) {
                  map.put(pair.getLeft(), pair.getRight());
                }
              }
              index++;
            }
            try {
              P_436_ERRORS
                .addAll(compareSQL2008DataTypeWithXMLType(entry.getKey(), advancedOrStructuredColumn.getKey(), map));
            } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException e) {
              LOGGER.debug("Failed to validate {}", P_436, e);
              getValidationReporter().validationStatus(P_436, ValidationReporterStatus.ERROR,
                "Failed to validate due to an exception on " + MODULE_NAME,
                "Please check the log file for more information");
              return false;
            }
          }
        }
      }
    }

    return P_436_ERRORS.isEmpty();
  }

  /**
   * P_4.3-7
   *
   * The nullable information on the column definitions in the metadata.xml must
   * be identical to that in the corresponding table[number].xsd file.
   *
   * If <nullable>false</nullable> fields contain empty information then a warning
   * will be added. WARNING should be one line that outputs the count of null
   * fields
   *
   * @return true if valid otherwise false
   */
  private boolean validateNillableInformation() {
    observer.notifyComponent(P_437, ValidationReporterStatus.START);

    warnings.clear();
    int counter = 0;

    for (Map.Entry<SIARDContent, HashMap<String, String>> entry : numberOfNullable.entrySet()) {
      String XMLPath = validatorPathStrategy.getXMLTablePathFromFolder(entry.getKey().getSchema(),
        entry.getKey().getTable());
      observer.notifyElementValidating(P_437, XMLPath);

      try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, XMLPath)) {
        XMLStreamReader streamReader = factory.createXMLStreamReader(is);
        HashMap<String, Integer> countColumnsMap = new HashMap<>();
        while (streamReader.hasNext()) {
          streamReader.next();
          if (streamReader.getEventType() == START_ELEMENT) {
            final String localName = streamReader.getLocalName();
            if (!localName.equals("row") && !localName.equals("table")) {
              updateCounter(countColumnsMap, localName);
            }
          }
        }

        streamReader.close();

        for (Map.Entry<String, String> values : entry.getValue().entrySet()) {
          if (values.getValue().equals("false")) {
            final int number = numberOfRows.get(entry.getKey());
            final int nodeCounter;
            if (countColumnsMap.get(values.getKey()) != null) {
              nodeCounter = countColumnsMap.get(values.getKey());
            } else {
              nodeCounter = 0;
            }

            if (number != nodeCounter) {
              counter++;
            }
          }
        }

      } catch (IOException | XMLStreamException e) {
        getValidationReporter().validationStatus(P_437, ValidationReporterStatus.ERROR,
          "Failed to validate due to an exception on " + MODULE_NAME, "Please check the log file for more information");
        LOGGER.debug("Failed to validate {}", P_437, e);
        return false;
      }
    }

    if (counter != 0)
      warnings.add(String.valueOf(counter));

    return true;
  }

  /**
   * P_4.3-8
   *
   * The column sequence in the metadata.xml must be identical to that in the
   * corresponding table[number].xsd.
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumnSequence() {
    return true; // validateColumnCount() && validateDataTypeInformation();
  }

  /**
   * P_4.3-9
   * 
   * The field sequence in the table definition of the metadata.xml must be
   * identical to the field sequence in the corresponding table[number].xsd.
   * 
   * @return true if valid otherwise false
   */
  private boolean validateFieldSequenceInTableXSD() {
    for (Map.Entry<SIARDContent, HashMap<String, AdvancedOrStructuredColumn>> entry : advancedOrStructuredDataType
      .entrySet()) {
      for (Map.Entry<String, AdvancedOrStructuredColumn> advancedOrStructuredColumnEntry : entry.getValue()
        .entrySet()) {
        String XSDPath = validatorPathStrategy.getXSDTablePathFromFolder(entry.getKey().getSchema(),
          entry.getKey().getTable());
        String xpathExpression = "count(/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element[@name='$1']/xs:complexType/xs:sequence/xs:element)";
        xpathExpression = xpathExpression.replace("$1", advancedOrStructuredColumnEntry.getKey());

        try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, XSDPath)) {
          String result = (String) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.STRING,
            Constants.NAMESPACE_FOR_TABLE);
          int count = Integer.parseInt(result);
          if (advancedOrStructuredColumnEntry.getValue().getFields().size() != count) {
            P_439_ERRORS
              .add("Different number of fields in " + advancedOrStructuredColumnEntry.getKey() + " at " + XSDPath);
          }
        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          LOGGER.debug("Failed to validate {}", P_439, e);
          getValidationReporter().validationStatus(P_439, ValidationReporterStatus.ERROR,
            "Failed to validate due to an exception on " + MODULE_NAME,
            "Please check the log file for more information");
          return false;
        }
      }
    }

    return P_439_ERRORS.isEmpty();
  }

  /**
   * P_4.3-10
   *
   * The number of lines in a table in metadata.xml must fit into the area
   * specified in the corresponding table[number].xsd. The number of lines in a
   * table in metadata.xml must be identical to the number of lines in the
   * corresponding table[number].xml.
   *
   * @return true if valid otherwise false
   */
  private boolean validateNumberOfLinesInATable() {
    observer.notifyComponent(P_4310, ValidationReporterStatus.START);
    for (Map.Entry<SIARDContent, Integer> entry : numberOfRows.entrySet()) {
      String XSDPath = validatorPathStrategy.getXSDTablePathFromFolder(entry.getKey().getSchema(),
        entry.getKey().getTable());

      String XMLPath = validatorPathStrategy.getXMLTablePathFromFolder(entry.getKey().getSchema(),
        entry.getKey().getTable());

      observer.notifyElementValidating(P_4310, XMLPath);

      int rows = entry.getValue();
      try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, XMLPath)) {
        XMLStreamReader streamReader = factory.createXMLStreamReader(is);
        int numberOfRowsInXMLFile = 0;
        while (streamReader.hasNext()) {
          streamReader.next();
          if (streamReader.getEventType() == START_ELEMENT) {
            if (streamReader.getLocalName().equals("row")) {
              numberOfRowsInXMLFile++;
            }
          }
        }

        streamReader.close();

        try (InputStream inputStream = zipFileManagerStrategy.getZipInputStream(path, XSDPath)) {
          Node result = (Node) XMLUtils.getXPathResult(inputStream,
            "/xs:schema/xs:element[@name='table']/xs:complexType/xs:sequence/xs:element[@type='recordType']",
            XPathConstants.NODE, Constants.NAMESPACE_FOR_TABLE);
          String minOccursString = result.getAttributes().getNamedItem("minOccurs").getNodeValue();
          String maxOccursString = result.getAttributes().getNamedItem("maxOccurs").getNodeValue();

          /*
           * The number of lines in a table in metadata.xml must fit into the area
           * specified in the corresponding table[number].xsd.
           */
          int minOccurs = Integer.parseInt(minOccursString);
          if (minOccurs < 0)
            P_4310_ERRORS.add("The minOccurs attribute is negative at " + XSDPath);

          if (rows < minOccurs)
            P_4310_ERRORS
              .add("The number of rows at " + XMLPath + " is less than defined by minOccurs attribute at " + XSDPath);

          if (!maxOccursString.equals("unbounded")) {
            int maxOccurs = Integer.parseInt(maxOccursString);
            if (rows > maxOccurs)
              P_4310_ERRORS
                .add("The number of rows at " + XMLPath + " is more than defined by maxOccurs attribute at " + XSDPath);
          }

          /*
           * The number of lines in a table in metadata.xml must be identical to the
           * number of lines in the corresponding table[number].xml.
           */
          if (numberOfRowsInXMLFile != rows)
            P_4310_ERRORS.add("The number of rows at " + XMLPath + " is not identical to "
              + validatorPathStrategy.getMetadataXMLPath());
        }
      } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException
        | XMLStreamException e) {
        LOGGER.debug("Failed to validate {}", P_4310, e);
        getValidationReporter().validationStatus(P_4310, ValidationReporterStatus.ERROR,
          "Failed to validate due to an exception on " + MODULE_NAME, "Please check the log file for more information");
        return false;
      }
    }

    return P_4310_ERRORS.isEmpty();
  }

  /*
   * Additional checks
   */

  /**
   * A_P_4.3-10
   *
   * Examining whether the number of rows in the header correspond to the actual
   * number of rows in Content
   *
   * @return true if valid otherwise false
   */
  private boolean validateNumberOfRows() {
    observer.notifyComponent(A_P_4310, ValidationReporterStatus.START);

    // Count number of row element in table[number].xml
    for (Map.Entry<SIARDContent, Integer> entry : numberOfRows.entrySet()) {
      final String XMLPath = validatorPathStrategy.getXMLTablePathFromFolder(entry.getKey().getSchema(),
        entry.getKey().getTable());

      observer.notifyElementValidating(A_P_4310, XMLPath);

      try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, XMLPath)) {
        XMLStreamReader streamReader = factory.createXMLStreamReader(is);
        int count = 0;
        while (streamReader.hasNext()) {
          streamReader.next();

          if (streamReader.getEventType() == START_ELEMENT) {
            if (streamReader.getLocalName().equals("row")) {
              count++;
            }
          }
        }

        streamReader.close();

        if (entry.getValue() != count) {
          String message = "Found $1 rows in $2 but expected were $3 rows";
          message = message.replace("$1", String.valueOf(count));
          message = message.replace("$2", XMLPath);
          message = message.replace("$3", String.valueOf(entry.getValue()));
          A_P_4310_ERRORS.add(message);
        }

      } catch (XMLStreamException | IOException e) {
        getValidationReporter().validationStatus(A_P_4310, ValidationReporterStatus.ERROR,
          "Failed to validate due to an exception on " + MODULE_NAME, "Please check the log file for more information");
        LOGGER.debug("Failed to validate {}", "number of rows", e);
        return false;
      }
    }

    return A_P_4310_ERRORS.isEmpty();
  }

  private void outputDifferentBlobsTypes()
    throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
    NodeList result;
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      result = (NodeList) XMLUtils.getXPathResult(is,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:columns/ns:column/ns:mimeType/text()",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < result.getLength(); i++) {
        final String nodeValue = result.item(i).getNodeValue();
        getValidationReporter().notice(nodeValue, "MimeType found");
      }
    }
  }

  /*
   * Auxiliary Methods
   */
  private void updateCounter(HashMap<String, Integer> countColumnsMap, String columnIndex) {
    if (countColumnsMap.get(columnIndex) != null) {
      Integer integer = countColumnsMap.get(columnIndex);
      countColumnsMap.put(columnIndex, ++integer);
    } else {
      countColumnsMap.put(columnIndex, 1);
    }
  }

  private String validateFieldSequence(List<Field> fields, List<Attribute> attributes) {

    if (attributes.size() != fields.size()) {
      return "Excepted " + attributes.size() + " fields but found " + fields.size() + " in $1 at $2";
    }

    for (int i = 0; i < attributes.size(); i++) {
      Attribute attribute = attributes.get(i);
      Field field = fields.get(i);

      if (!attribute.getName().equals(field.getName())) {
        // add error
        return "Mismatch name between attribute and fields in $1 at $2";
      }

      if (attribute.haveSuperType()) {
        String key = attribute.getTypeSchema() + "." + attribute.getTypeName();
        return validateFieldSequence(field.getFields(), types.get(key).getAttributes());
      }
    }

    return "";
  }

  private void getTypesFromMetadataXML()
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath());
    NodeList nodes = (NodeList) XMLUtils.getXPathResult(is, "/ns:siardArchive/ns:schemas/ns:schema",
      XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

    // Obtain Types
    for (int i = 0; i < nodes.getLength(); i++) {
      Element schema = (Element) nodes.item(i);
      String schemaName = schema.getElementsByTagName("name").item(0).getTextContent();
      InputStream inputStream = zipFileManagerStrategy.getZipInputStream(path,
        validatorPathStrategy.getMetadataXMLPath());
      NodeList types = (NodeList) XMLUtils.getXPathResult(inputStream,
        "/ns:siardArchive/ns:schemas/ns:schema[ns:name/text() = '" + schemaName + "']/ns:types/ns:type",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
      for (int j = 0; j < types.getLength(); j++) {
        Element type = (Element) types.item(j);
        String name = type.getElementsByTagName("name").item(0).getTextContent();
        String category = type.getElementsByTagName("category").item(0).getTextContent();
        String underSchema = type.getElementsByTagName("underSchema").item(0) != null
          ? type.getElementsByTagName("underSchema").item(0).getTextContent()
          : "";
        String underType = type.getElementsByTagName("underType").item(0) != null
          ? type.getElementsByTagName("underType").item(0).getTextContent()
          : "";
        String instantiable = type.getElementsByTagName("instantiable").item(0).getTextContent();
        String _final = type.getElementsByTagName("final").item(0).getTextContent();
        String base = "";
        if (category.equalsIgnoreCase("distinct")) {
          if (type.getElementsByTagName("base").item(0) != null)
            base = type.getElementsByTagName("base").item(0).getTextContent();
        }
        List<Attribute> attributes = getAttributesFromType(type, category);
        String description = type.getElementsByTagName("description").item(0) != null
          ? type.getElementsByTagName("description").item(0).getTextContent()
          : "";

        String key = schemaName + "." + name;

        this.types.put(key, new Type(schemaName, name, category, underSchema, underType, instantiable, _final, base,
          attributes, description));
      }
      inputStream.close();
    }
    is.close();
  }

  private List<Attribute> getAttributesFromType(Element element, String category) {
    if (category.equalsIgnoreCase("distinct"))
      return Collections.emptyList();

    List<Attribute> attributes = new ArrayList<>();
    NodeList attributeNodes = element.getElementsByTagName("attribute");
    for (int i = 0; i < attributeNodes.getLength(); i++) {
      Element attributeNode = (Element) attributeNodes.item(i);
      String name = attributeNode.getElementsByTagName("name").item(0).getTextContent();
      String type = attributeNode.getElementsByTagName("type").item(0) != null
        ? attributeNode.getElementsByTagName("type").item(0).getTextContent()
        : "";
      String typeOriginal = attributeNode.getElementsByTagName("typeOriginal").item(0) != null
        ? attributeNode.getElementsByTagName("typeOriginal").item(0).getTextContent()
        : "";
      String nullable = attributeNode.getElementsByTagName("nullable").item(0) != null
        ? attributeNode.getElementsByTagName("nullable").item(0).getTextContent()
        : "";
      String typeSchema = attributeNode.getElementsByTagName("typeSchema").item(0) != null
        ? attributeNode.getElementsByTagName("typeSchema").item(0).getTextContent()
        : "";
      String typeName = attributeNode.getElementsByTagName("typeName").item(0) != null
        ? attributeNode.getElementsByTagName("typeName").item(0).getTextContent()
        : "";
      String defaultValue = attributeNode.getElementsByTagName("defaultValue").item(0) != null
        ? attributeNode.getElementsByTagName("defaultValue").item(0).getTextContent()
        : "";
      String description = attributeNode.getElementsByTagName("description").item(0) != null
        ? attributeNode.getElementsByTagName("description").item(0).getTextContent()
        : "";
      String cardinality = attributeNode.getElementsByTagName("cardinality").item(0) != null
        ? attributeNode.getElementsByTagName("cardinality").item(0).getTextContent()
        : "";

      attributes.add(new Attribute(name, type, typeOriginal, nullable, typeSchema, typeName, defaultValue, description,
        cardinality));
    }

    return attributes;
  }

  private List<ImmutablePair<String, String>> getParentDataType(String parentType, String rootIndex) {
    final Type type = types.get(parentType);
    ImmutablePair<String, String> pair;
    List<ImmutablePair<String, String>> pairs = new ArrayList<>();
    int index = 1;
    for (Attribute attribute : type.getAttributes()) {
      String key = rootIndex.concat(".").concat("u" + index);
      if (StringUtils.isNotBlank(attribute.getType())) {
        pair = new ImmutablePair<>(key, attribute.getType());
        pairs.add(pair);
      } else {
        String typeKey = attribute.getTypeSchema() + "." + attribute.getTypeName();
        final List<ImmutablePair<String, String>> result = getParentDataType(typeKey, key);
        pairs.addAll(result);
      }
      index++;
    }

    return pairs;
  }

  private void obtainSQL2008TypeForEachColumn()
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath());
    NodeList resultNodes = (NodeList) XMLUtils.getXPathResult(is, "/ns:siardArchive/ns:schemas/ns:schema",
      XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
    is.close();
    for (int i = 0; i < resultNodes.getLength(); i++) {
      Element schema = (Element) resultNodes.item(i);
      String schemaName = schema.getElementsByTagName("name").item(0).getTextContent();
      String schemaFolderName = schema.getElementsByTagName("folder").item(0).getTextContent();
      NodeList tableNodes = schema.getElementsByTagName("table");
      for (int j = 0; j < tableNodes.getLength(); j++) {
        Element table = (Element) tableNodes.item(j);
        String tableName = table.getElementsByTagName("name").item(0).getTextContent();
        String tableFolderName = table.getElementsByTagName("folder").item(0).getTextContent();
        String rows = table.getElementsByTagName("rows").item(0).getTextContent();
        NodeList columnsNodes = table.getElementsByTagName("columns");
        Element columns = (Element) columnsNodes.item(0);
        NodeList columnNodes = columns.getElementsByTagName("column");

        SIARDContent content = new SIARDContent(schemaFolderName, tableFolderName);

        HashMap<String, String> columnsSQL2008Map = new HashMap<>();
        HashMap<String, String> arrayMap = new HashMap<>();
        HashMap<String, AdvancedOrStructuredColumn> advanceOrStructuredColumnMap = new HashMap<>();
        HashMap<String, String> nullableMap = new HashMap<>();

        for (int k = 0; k < columnNodes.getLength(); k++) {
          String key = calculateKey(k);
          Element column = (Element) columnNodes.item(k);
          final String columnName = column.getElementsByTagName("name").item(0).getTextContent();
          String type;

          nullableMap.put(key, obtainNullable(column));

          if (column.getElementsByTagName("type").item(0) != null) {
            type = column.getElementsByTagName("type").item(0).getTextContent();
            if (column.getElementsByTagName("cardinality").item(0) == null) { // check if its an array
              columnsSQL2008Map.put(key, type);
              sql2008Type.put(content, columnsSQL2008Map);
            } else {
              arrayMap.put(key, type);
              arrayType.put(content, arrayMap);
            }
          } else {
            advanceOrStructuredColumnMap.put(key,
              obtainAdvancedOrStructuredDataType(column, schemaName, tableName, columnName));
            advancedOrStructuredDataType.put(content, advanceOrStructuredColumnMap);
          }
        }
        numberOfNullable.put(content, nullableMap);
        numberOfRows.put(content, Integer.parseInt(rows));
      }
    }
  }

  private String calculateKey(int number) {
    StringBuilder sb = new StringBuilder();
    number += 1;
    sb.append("c").append(number);

    return sb.toString();
  }

  private String obtainNullable(Element element) {
    if (element.getElementsByTagName("nullable").item(0) != null) {
      return element.getElementsByTagName("nullable").item(0).getTextContent();
    } else {
      return "true";
    }
  }

  private AdvancedOrStructuredColumn obtainAdvancedOrStructuredDataType(Element column, String schemaName,
    String tableName, String columnName)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    ArrayList<Field> fields = new ArrayList<>();

    String typeSchema = column.getElementsByTagName("typeSchema").item(0) != null
      ? column.getElementsByTagName("typeSchema").item(0).getTextContent()
      : schemaName;
    String typeName = column.getElementsByTagName("typeName").item(0).getTextContent();

    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:name/text()='$1']/ns:tables/ns:table[ns:name/text()='$2']/ns:columns/ns:column[ns:name/text()='$3']/ns:fields/ns:field";
    xpathExpression = xpathExpression.replace("$1", schemaName);
    xpathExpression = xpathExpression.replace("$2", tableName);
    xpathExpression = xpathExpression.replace("$3", columnName);

    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {

      final NodeList resultNodes = (NodeList) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int l = 0; l < resultNodes.getLength(); l++) {
        Element element = (Element) resultNodes.item(l);
        fields.add(getField(element, xpathExpression));
      }

      return new AdvancedOrStructuredColumn(columnName, typeSchema, typeName, fields);
    }
  }

  private Field getField(Element item, String xpathExpression)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    Field field = new Field();
    final String name = item.getElementsByTagName("name").item(0).getTextContent();
    field.setName(name);
    if (item.getElementsByTagName("fields").getLength() == 0)
      return field;

    String concat = xpathExpression.concat("[ns:name/text()='$1']/ns:fields/ns:field");
    concat = concat.replace("$1", name);

    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      final NodeList fields = (NodeList) XMLUtils.getXPathResult(is, concat, XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);
      for (int l = 0; l < fields.getLength(); l++) {
        Element element = (Element) fields.item(l);
        field.addFieldToList(getField(element, concat));
      }

      return field;
    }
  }

  private List<String> compareSQL2008DataTypeWithXMLType(SIARDContent content, String column,
    HashMap<String, String> map)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    List<String> errors = new ArrayList<>();
    String xsdPath = validatorPathStrategy.getXSDTablePathFromFolder(content.getSchema(), content.getTable());
    String xpathExpression;
    for (Map.Entry<String, String> entry : map.entrySet()) {
      final String key = entry.getKey();
      String[] split = key.split("\\.");
      xpathExpression = "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element[@name='$1']";
      xpathExpression = xpathExpression.replace("$1", column);
      if (split.length > 1) {
        for (String s : split) {
          String append = "/xs:complexType/xs:sequence/xs:element[@name='$1']";
          append = append.replace("$1", s);
          xpathExpression = xpathExpression.concat(append);
        }
      } else {
        String append = "/xs:complexType/xs:sequence/xs:element[@name='$1']";
        append = append.replace("$1", key);
        xpathExpression = xpathExpression.concat(append);
      }
      xpathExpression = xpathExpression.concat("/@type");

      try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, xsdPath)) {
        String xmlType = (String) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.STRING,
          Constants.NAMESPACE_FOR_TABLE);

        if (!validateSQL2008TypeWithXMLType(entry.getValue(), xmlType)) {
          errors.add(obtainErrorMessage(entry.getValue(), xmlType, key, xsdPath));
        }
      }
    }

    return errors;
  }

  private List<String> compareSQL2008DataTypeWithXMLType(SIARDContent content, HashMap<String, String> map)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    List<String> errors = new ArrayList<>();
    String xsdPath = validatorPathStrategy.getXSDTablePathFromFolder(content.getSchema(), content.getTable());
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, xsdPath)) {

      final NodeList nodeList = (NodeList) XMLUtils.getXPathResult(is,
        "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element", XPathConstants.NODESET, null);

      for (int i = 0; i < nodeList.getLength(); i++) {
        String xmltype;
        final Node item = nodeList.item(i);
        if (item.getAttributes().getNamedItem("type") != null) {
          xmltype = item.getAttributes().getNamedItem("type").getNodeValue();
        } else {
          xmltype = null;
        }

        String columnName = item.getAttributes().getNamedItem("name").getNodeValue();
        if (map.get(columnName) != null) {
          if (!validateSQL2008TypeWithXMLType(map.get(columnName), xmltype)) {
            errors.add(obtainErrorMessage(map.get(columnName), xmltype, columnName, xsdPath));
          }
        }
      }

      return errors;
    }
  }

  private String obtainErrorMessage(String SQL2008Type, String XMLType, String column, String file) {
    StringBuilder message = new StringBuilder();

    for (Map.Entry<String, List<String>> entry : SQL2008TypeMatchXMLType.entrySet()) {
      if (SQL2008Type.matches(entry.getKey())) {
        StringBuilder expectedTypes = new StringBuilder();
        for (String s : entry.getValue()) {
          if (entry.getValue().indexOf(s) == entry.getValue().size() - 1) {
            expectedTypes.append(s);
          } else {
            expectedTypes.append(s).append(" or ");
          }
        }

        message.append("For '").append(SQL2008Type).append("' type expected '").append(expectedTypes.toString())
          .append("' but found '").append(XMLType).append("' in ").append(column).append(" at ").append(file);

        return message.toString();
      }
    }

    message.append("'").append(SQL2008Type).append("' type is not a valid SQL:2008 type");
    return message.toString();
  }

  private boolean checkRelativePathExists(final String pathToValidate) {
    final List<String> zipArchiveEntriesPath = zipFileManagerStrategy.getZipArchiveEntriesPath(path);
    if (zipArchiveEntriesPath == null) {
      return false;
    }
    for (String pathInZip : zipArchiveEntriesPath) {
      if (pathInZip.contains(pathToValidate)) {
        return true;
      }
    }
    return false;
  }

  private String createPath(String... parameters) {
    StringBuilder sb = new StringBuilder();
    for (String parameter : parameters) {
      sb.append(parameter).append(Constants.RESOURCE_FILE_SEPARATOR);
    }
    sb.deleteCharAt(sb.length() - 1);

    return sb.toString();
  }

  private boolean validateSQL2008TypeWithXMLType(final String SQL2008Type, final String XMLType) {
    for (Map.Entry<String, List<String>> entry : SQL2008TypeMatchXMLType.entrySet()) {
      if (XMLType != null) {
        if (SQL2008Type.matches(entry.getKey())) {
          final List<String> XMLTypeMatches = entry.getValue();
          return XMLTypeMatches.contains(XMLType);
        }
      }
    }

    return false;
  }

  private void skip(final String step) {
    if (skipped.isEmpty()) {
      observer.notifyValidationStep(MODULE_NAME, step, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(step, ValidationReporterStatus.OK);
    } else {
      for (String skippedReason : skipped) {
        observer.notifyValidationStep(MODULE_NAME, step, ValidationReporterStatus.SKIPPED);
        getValidationReporter().skipValidation(step, skippedReason);
      }
      // observer.notifyValidationStep(MODULE_NAME, step, Status.OK);
      // getValidationReporter().validationStatus(step, Status.OK);
    }
  }

  private void populateSQL2008Types() {
    SQL2008TypeMatchXMLType = new HashMap<>();
    SQL2008TypeMatchXMLType.put("^BIGINT$", Collections.singletonList("xs:integer"));
    SQL2008TypeMatchXMLType.put("^BINARY\\s+LARGE\\s+OBJECT(\\s*\\(\\s*[1-9]\\d*(\\s*(K|M|G))?\\s*\\))?$",
      Collections.singletonList("blobType"));
    SQL2008TypeMatchXMLType.put("^BLOB(\\s*\\(\\s*[1-9]\\d*(\\s*(K|M|G))?\\s*\\))?$",
      Collections.singletonList("blobType"));
    SQL2008TypeMatchXMLType.put("^BLOB", Collections.singletonList("blobType"));
    SQL2008TypeMatchXMLType.put("^(?:BINARY\\s+VARYING|VARBINARY)(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$",
      Arrays.asList("clobType", "xs:hexBinary"));
    SQL2008TypeMatchXMLType.put("^BINARY\\s*(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$",
      Arrays.asList("blobType", "xs:hexBinary"));
    SQL2008TypeMatchXMLType.put("^VARBINARY(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$", Arrays.asList("blobType", "xs:hexBinary"));
    SQL2008TypeMatchXMLType.put("^BOOLEAN$", Collections.singletonList("xs:boolean"));
    SQL2008TypeMatchXMLType.put("^CHARACTER\\s+LARGE\\s+OBJECT(\\s*\\(\\s*[1-9]\\d*(\\s*(K|M|G))?\\s*\\))?$",
      Collections.singletonList("clobType"));
    SQL2008TypeMatchXMLType.put("^CLOB(\\s*\\(\\s*[1-9]\\d*(\\s*(K|M|G))?\\s*\\))?$",
      Collections.singletonList("clobType"));
    SQL2008TypeMatchXMLType.put("^CHARACTER\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$",
      Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("^CHAR\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$",
      Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("^VARCHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$", Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("^CHARACTER(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$", Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("^CHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$", Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("^DATE$", Collections.singletonList("dateType"));
    SQL2008TypeMatchXMLType.put("^DECIMAL(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?$",
      Collections.singletonList("xs:decimal"));
    SQL2008TypeMatchXMLType.put("^DEC(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?$",
      Collections.singletonList("xs:decimal"));
    SQL2008TypeMatchXMLType.put("^DOUBLE PRECISION$", Collections.singletonList("xs:double"));
    SQL2008TypeMatchXMLType.put("^FLOAT(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$", Collections.singletonList("xs:double"));
    SQL2008TypeMatchXMLType.put("^INTEGER$", Collections.singletonList("xs:integer"));
    SQL2008TypeMatchXMLType.put("^INT$", Collections.singletonList("xs:integer"));
    SQL2008TypeMatchXMLType.put(
      "^INTERVAL\\s+(((YEAR|MONTH|DAY|HOUR|MINUTE)(\\s*\\(\\s*[1-9]\\d*\\s*\\))?(\\s+TO\\s+(MONTH|DAY|HOUR|MINUTE|SECOND)(\\s*\\(\\s*[1-9]\\d*\\s*\\))?)?)|(SECOND(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?))$",
      Collections.singletonList("xs:duration"));
    SQL2008TypeMatchXMLType.put(
      "^NATIONAL\\s+CHARACTER\\s+LARGE\\s+OBJECT(\\s*\\(\\s*[1-9]\\d*(\\s*(K|M|G))?\\s*\\))?$",
      Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("^NCHAR\\s+LARGE\\s+OBJECT(\\s*\\(\\s*[1-9]\\d*(\\s*(K|M|G))?\\s*\\))?$",
      Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("^NCLOB(\\s*\\(\\s*[1-9]\\d*(\\s*(K|M|G))?\\s*\\))?$",
      Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("^NATIONAL\\s+CHARACTER\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$",
      Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("^NATIONAL\\s+CHAR\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$",
      Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("^NCHAR\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$",
      Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("^NATIONAL\\s+CHARACTER(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$",
      Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("^NATIONAL\\s+CHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$",
      Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("^NCHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$", Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("^NUMERIC(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?$",
      Collections.singletonList("xs:decimal"));
    SQL2008TypeMatchXMLType.put("^REAL$", Collections.singletonList("xs:float"));
    SQL2008TypeMatchXMLType.put("^SMALLINT$", Collections.singletonList("xs:integer"));
    SQL2008TypeMatchXMLType.put("^TIME(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$", Collections.singletonList("timeType"));
    SQL2008TypeMatchXMLType.put("^TIME\\s+WITH\\s+TIME\\s+ZONE(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$",
      Collections.singletonList("timeType"));
    SQL2008TypeMatchXMLType.put("^TIMESTAMP(\\s*\\(\\s*(0|([1-9]\\d*))\\s*\\))?$",
      Collections.singletonList("dateTimeType"));
    SQL2008TypeMatchXMLType.put("^TIMESTAMP\\s+WITH\\s+TIME\\s+ZONE(\\s*\\(\\s*(0|([1-9]\\d*))\\s*\\))?$",
      Collections.singletonList("dateTimeType"));
    SQL2008TypeMatchXMLType.put("^XML$", Collections.singletonList("clobType"));
  }
}
