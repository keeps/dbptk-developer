package com.databasepreservation.modules.siard.validate.component.formatStructure;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter.Status;
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
  private static final String P_4311 = "P_4.3-11";
  private static final String P_4312 = "P_4.3-12";

  private List<String> P_431_ERRORS = new ArrayList<>();
  private List<String> P_432_ERRORS = new ArrayList<>();
  private List<String> P_433_ERRORS = new ArrayList<>();
  private List<String> P_434_ERRORS = new ArrayList<>();
  private List<String> P_435_ERRORS = new ArrayList<>();
  private List<String> P_436_ERRORS = new ArrayList<>();
  private List<String> P_437_ERRORS = new ArrayList<>();
  private List<String> P_438_ERRORS = new ArrayList<>();
  private List<String> P_439_ERRORS = new ArrayList<>();
  private List<String> P_4310_ERRORS = new ArrayList<>();
  private List<String> P_4311_ERRORS = new ArrayList<>();
  private List<String> P_4312_ERRORS = new ArrayList<>();

  private static HashMap<String, List<String>> SQL2008TypeMatchXMLType;
  private HashMap<String, HashMap<String, String>> sql2008Type = new HashMap<>();
  private HashMap<String, HashMap<String, String>> arrayType = new HashMap<>();
  private HashMap<String, HashMap<String, AdvancedOrStructuredColumn>> advancedOrStructuredDataType = new HashMap<>();
  private TreeMap<String, HashMap<String, String>> numberOfNullable = new TreeMap<>();
  private TreeMap<String, Integer> numberOfRows = new TreeMap<>();
  private List<String> skipped = new ArrayList<>();
  private List<String> warnings = new ArrayList<>();
  private HashMap<String, Type> types = new HashMap<>();

  public MetadataAndTableDataValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    populateSQL2008Types();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, P_43);

    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    try {
      obtainSQL2008TypeForEachColumn();
      getTypesFromMetadataXML();
    } catch (ParserConfigurationException | SAXException | XPathExpressionException | IOException e) {
      LOGGER.debug("Failed to fetch data for validation component {}", MODULE_NAME, e);
      closeZipFile();
      return false;
    }

    getValidationReporter().moduleValidatorHeader(P_43, MODULE_NAME);

    if (validateMetadataStructure()) {
      getValidationReporter().validationStatus(P_431, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_431, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_431, MODULE_NAME, "the metadata.xml must be identical to that in the content/", "Invalid path",
        P_431_ERRORS);
      closeZipFile();
      return false;
    }

    if (validateColumnCount()) {
      observer.notifyValidationStep(MODULE_NAME, P_432, Status.OK);
      getValidationReporter().validationStatus(P_432, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_432, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_432, MODULE_NAME, "", "Invalid table", P_432_ERRORS);
      closeZipFile();
      return false;
    }

    if (validateDataTypeInformation()) {
      observer.notifyValidationStep(MODULE_NAME, P_433, Status.OK);
      getValidationReporter().validationStatus(P_433, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_433, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_433, MODULE_NAME,
        "The data type information on the column definitions in metadata.xml must be identical to that in the corresponding table[number].xsd file",
        "Invalid data type", P_433_ERRORS);
      closeZipFile();
      return false;
    }

    if (validateDistinctXMLDataTypeConversion()) {
      for (String warning : warnings) {
        getValidationReporter().warning(P_434, "Not allow user-defined data type", warning);
      }
      skip(P_434);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_434, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_434, MODULE_NAME, "", "Invalid data type", P_434_ERRORS);
      closeZipFile();
      return false;
    }

    if (validateNumberOfRows()) {
      observer.notifyValidationStep(MODULE_NAME, P_435, Status.OK);
      getValidationReporter().validationStatus(P_435, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_435, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_435, MODULE_NAME, "", "Invalid path", P_435_ERRORS);
      closeZipFile();
      return false;
    }

    if (validateArrayXMLDataTypeConversion()) {
      skip(P_436);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_436, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_436, MODULE_NAME, "", "Invalid data type", P_436_ERRORS);
      closeZipFile();
      return false;
    }

    if (validateUDTXMLDataTypeConversion()) {
      skip(P_437);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_437, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_437, MODULE_NAME, "", "Invalid data type", P_437_ERRORS);
      closeZipFile();
      return false;
    }

    boolean result = validateNillableInformation();
    for (String warning : warnings) {
      getValidationReporter().warning(P_438, "Number of null fields", warning);
    }
    if (result) {
      observer.notifyValidationStep(MODULE_NAME, P_438, Status.OK);
      getValidationReporter().validationStatus(P_438, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_438, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_438, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateColumnSequence()) {
      observer.notifyValidationStep(MODULE_NAME, P_439, Status.OK);
      getValidationReporter().validationStatus(P_439, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_439, Status.OK);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_439, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateFieldSequenceInMetadataXML()) {
      observer.notifyValidationStep(MODULE_NAME, P_4310, Status.OK);
      getValidationReporter().validationStatus(P_4310, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_4310, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_4310, MODULE_NAME, "", "Field Sequence invalid", P_4310_ERRORS);
      closeZipFile();
      return false;
    }

    if (validateFieldSequenceInTableXSD()) {
      observer.notifyValidationStep(MODULE_NAME, P_4311, Status.OK);
      getValidationReporter().validationStatus(P_4311, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_4311, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_4311, MODULE_NAME, "Not identical field sequence", "Field Sequence invalid", P_4311_ERRORS);
      closeZipFile();
      return false;
    }

    if (validateNumberOfLinesInATable()) {
      observer.notifyValidationStep(MODULE_NAME, P_4312, Status.OK);
      getValidationReporter().validationStatus(P_4312, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_4312, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_4312, MODULE_NAME, "", "Number of lines", P_4312_ERRORS);
      closeZipFile();
      return false;
    }
    observer.notifyFinishValidationModule(MODULE_NAME, Status.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, Status.PASSED);
    closeZipFile();

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
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    try {
      final NodeList nodeList = (NodeList) XMLUtils.getXPathResult(
        getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema", XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
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
      return false;
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
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    HashMap<String, Integer> columnCount = new HashMap<>();
    List<String> entries = new ArrayList<>();

    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

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
          final String path = createPath(Constants.SIARD_CONTENT_FOLDER, schemaFolderName, tableFolderName,
            tableFolderName);
          entries.add(path);
          columnCount.put(path, columnNodes.getLength());
        }
      }

      for (String path : entries) {
        String XSDPath = path.concat(Constants.XSD_EXTENSION);
        final InputStream inputStream = getZipInputStream(XSDPath);
        final String evaluate = (String) XMLUtils.getXPathResult(inputStream,
          "count(/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element)", XPathConstants.STRING, null);
        int value = Integer.parseInt(evaluate);

        if (!columnCount.get(path).equals(value)) {
          P_432_ERRORS.add(XSDPath);
        }
      }
    } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
      LOGGER.debug("Failed to validate {}", P_432, e);
      return false;
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
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    for (Map.Entry<String, HashMap<String, String>> entry : sql2008Type.entrySet()) {
      try {
        final List<String> errors = compareSQL2008DataTypeWithXMLType(entry.getKey(), entry.getValue());
        P_433_ERRORS.addAll(errors);
      } catch (SAXException | ParserConfigurationException | IOException | XPathExpressionException e) {
        LOGGER.debug("Failed to validate {}", P_433, e);
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
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    warnings = new ArrayList<>();

    for (Type type : types.values()) {
      if (type.getCategory().equalsIgnoreCase("distinct")) {
        if (!allowedUDTs.contains(type.getName())) {
          warnings.add(type.getName());
        }
      }
    }

    try {
      HashMap<String, HashMap<String, String>> distinctTypes = new HashMap<>();
      // Obtain table[number] where the Type is being used
      for (Map.Entry<String, HashMap<String, AdvancedOrStructuredColumn>> entry : advancedOrStructuredDataType
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
        skipped.add("No distinct type found");
      } else {
        // Compare to the base type with the XML type from table[number].xsd
        for (Map.Entry<String, HashMap<String, String>> entry : distinctTypes.entrySet()) {
          final List<String> errors = compareSQL2008DataTypeWithXMLType(entry.getKey(), entry.getValue());
          P_434_ERRORS.addAll(errors);
        }
      }

    } catch (SAXException | ParserConfigurationException | IOException | XPathExpressionException e) {
      LOGGER.debug("Failed to validate {}", P_434, e);
      return false;
    }

    return P_434_ERRORS.isEmpty();
  }

  /**
   * P_4.3-5
   *
   * The named ROW container type is converted in the table[number].xsd schema
   * files into a sequence of structured XML elements <r1>, <r2>, … containing an
   * entry for each field. The data type of each field is converted to the XML
   * data type just like a column data type.
   *
   * Examining whether the number of rows in the header correspond to the actual
   * number of rows in Content
   *
   * @return true if valid otherwise false
   */
  private boolean validateNumberOfRows() {
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    observer.notifyComponent("P_4.3-5", Status.START);

    InputStream zipInputStream = null;
    try {
      // Count number of row element in table[number].xml
      for (Map.Entry<String, Integer> entry : numberOfRows.entrySet()) {
        String tablePath = entry.getKey();
        String XMLPath = tablePath.concat(Constants.XML_EXTENSION);
        observer.notifyElementValidating(XMLPath);
        zipInputStream = getZipInputStream(XMLPath);

        // Instance of the class which helps on reading tags
        XMLInputFactory factory = XMLInputFactory.newInstance();

        // Initializing the handler to access the tags in the XML file
        XMLStreamReader streamReader = factory.createXMLStreamReader(zipInputStream);
        int count = 0;
        while (streamReader.hasNext()) {
          // Move to next event
          streamReader.next();

          // Check if its 'START_ELEMENT'
          if (streamReader.getEventType() == XMLStreamReader.START_ELEMENT) {
            if (streamReader.getLocalName().equals("row")) {
              count++;
            }
          }
        }

        if (entry.getValue() != count) {
          String message = "Found $1 rows in $2 but expected were $3 rows";
          message = message.replace("$1", String.valueOf(count));
          message = message.replace("$2", XMLPath);
          message = message.replace("$3", String.valueOf(entry.getValue()));
          P_435_ERRORS.add(message);
        }

        zipInputStream.close();
      }
    } catch (IOException | XMLStreamException e) {
      LOGGER.debug("Failed to validate {}", P_435, e);
      return false;
    }

    if (zipInputStream != null) {
      try {
        zipInputStream.close();
      } catch (IOException e) {
        LOGGER.debug("Failed to validate {}", P_435, e);
        return false;
      }
    }

    return P_435_ERRORS.isEmpty();
  }

  /**
   * P_4.3-6
   *
   * The ARRAY container type is converted in the table[number].xsd schema files
   * into a sequence of structured XML elements <a1>, <a2>, … which are converted
   * to the XML data type corresponding to the base type of the array.
   * 
   */
  private boolean validateArrayXMLDataTypeConversion() {
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    warnings.clear();

    if (arrayType.isEmpty()) {
      skipped.clear();
      skipped.add("No Array type found");
      return true;
    }

    for (Map.Entry<String, HashMap<String, String>> entry : arrayType.entrySet()) {
      final String XSDPath = entry.getKey().concat(Constants.XSD_EXTENSION);
      for (Map.Entry<String, String> column : entry.getValue().entrySet()) {
        try {
          final InputStream zipInputStream = getZipInputStream(XSDPath);
          final NodeList nodeList = (NodeList) XMLUtils.getXPathResult(zipInputStream,
            "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element[@name='" + column.getKey()
              + "']/xs:complexType/xs:sequence/xs:element",
            XPathConstants.NODESET, null);

          for (int i = 0; i < nodeList.getLength(); i++) {
            Element element = (Element) nodeList.item(i);
            final String xmlType = element.getAttributes().getNamedItem("type").getNodeValue();
            if (!validateSQL2008TypeWithXMLType(entry.getValue().get(column.getKey()), xmlType)) {
              P_436_ERRORS
                .add(obtainErrorMessage(entry.getValue().get(column.getKey()), xmlType, column.getKey(), XSDPath));
            }
          }
        } catch (IOException | XPathExpressionException | SAXException | ParserConfigurationException e) {
          LOGGER.debug("Failed to validate {}", P_436, e);
          return false;
        }
      }
    }

    return P_436_ERRORS.isEmpty();
  }

  /**
   * P_4.3-7
   *
   * The named user-defined data type (UDT) is converted in the table[number].xsd
   * schema files into a sequence of structured XML elements <u1>, <u2>, … which
   * are converted to the XML data type corresponding to the type of each
   * attribute.
   *
   */
  private boolean validateUDTXMLDataTypeConversion() {
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    if (advancedOrStructuredDataType.isEmpty()) {
      skipped.clear();
      skipped.add("No UDT type found");
      return true;
    }

    // Obtain table[number] where the Type is being used
    for (Map.Entry<String, HashMap<String, AdvancedOrStructuredColumn>> entry : advancedOrStructuredDataType
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
              P_437_ERRORS
                .addAll(compareSQL2008DataTypeWithXMLType(entry.getKey(), advancedOrStructuredColumn.getKey(), map));
            } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException e) {
              LOGGER.debug("Failed to validate {}", P_437, e);
              return false;
            }
          }
        }
      }
    }

    return P_437_ERRORS.isEmpty();
  }

  /**
   * P_4.3-8
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
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }
    observer.notifyComponent("P_4.3-8", Status.START);

    warnings.clear();
    int counter = 0;

    InputStream zipInputStream;

    try {
      for (Map.Entry<String, HashMap<String, String>> entry : numberOfNullable.entrySet()) {
        String tablePath = entry.getKey();
        String XMLPath = tablePath.concat(Constants.XML_EXTENSION);
        observer.notifyElementValidating(XMLPath);
        for (Map.Entry<String, String> values : entry.getValue().entrySet()) {
          if (values.getValue().equals("false")) {

            int nodeCounter = 0;
            zipInputStream = getZipInputStream(XMLPath);

            XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLStreamReader streamReader = factory.createXMLStreamReader(zipInputStream);

            while (streamReader.hasNext()) {
              streamReader.next();
              if (streamReader.getEventType() == XMLStreamReader.START_ELEMENT) {
                if (streamReader.getLocalName().equals(values.getKey())) {
                  nodeCounter++;
                }
              }
            }

            final Integer number = numberOfRows.get(entry.getKey());
            if (number != nodeCounter) {
              counter++;
            }
          }
        }
      }

    } catch (XMLStreamException e) {
      LOGGER.debug("Failed to validate {}", P_438, e);
      return false;
    }

    if (counter != 0)
      warnings.add(String.valueOf(counter));

    return true;
  }

  /**
   * P_4.3-9
   *
   * The column sequence in the metadata.xml must be identical to that in the
   * corresponding table[number].xsd.
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumnSequence() {
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    return validateColumnCount() && validateDataTypeInformation();
  }

  /**
   * P_4.3-10
   *
   *
   * The field sequence in the type definition of a column of the metadata.xml
   * must be identical to the corresponding attribute sequence in the type
   * definition of the metadata.xml.
   *
   * @return true if valid otherwise false
   */
  private boolean validateFieldSequenceInMetadataXML() {
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    for (Map.Entry<String, HashMap<String, AdvancedOrStructuredColumn>> entry : advancedOrStructuredDataType
      .entrySet()) {
      for (Map.Entry<String, AdvancedOrStructuredColumn> advancedOrStructuredColumnEntry : entry.getValue()
        .entrySet()) {
        final List<Field> fields = advancedOrStructuredColumnEntry.getValue().getFields();
        String key = advancedOrStructuredColumnEntry.getValue().getTypeSchema() + "."
          + advancedOrStructuredColumnEntry.getValue().getTypeName();
        final List<Attribute> attributes = types.get(key).getAttributes();

        String errorMessage = validateFieldSequence(fields, attributes);
        if (StringUtils.isNotBlank(errorMessage)) {
          errorMessage = errorMessage.replace("$1", entry.getKey().split("/")[1]);
          errorMessage = errorMessage.replace("$2", entry.getKey().split("/")[2]);

          P_4310_ERRORS.add(errorMessage);
        }
      }
    }

    return P_4310_ERRORS.isEmpty();
  }

  /**
   * P_4.3-11
   * 
   * The field sequence in the table definition of the metadata.xml must be
   * identical to the field sequence in the corresponding table[number].xsd.
   * 
   * 
   * @return true if valid otherwise false
   */
  private boolean validateFieldSequenceInTableXSD() {
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    for (Map.Entry<String, HashMap<String, AdvancedOrStructuredColumn>> entry : advancedOrStructuredDataType
      .entrySet()) {
      for (Map.Entry<String, AdvancedOrStructuredColumn> advancedOrStructuredColumnEntry : entry.getValue()
        .entrySet()) {
        String XSDPath = entry.getKey().concat(Constants.XSD_EXTENSION);
        String xpathExpression = "count(/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element[@name='$1']/xs:complexType/xs:sequence/xs:element)";
        xpathExpression = xpathExpression.replace("$1", advancedOrStructuredColumnEntry.getKey());

        final InputStream zipInputStream = getZipInputStream(XSDPath);

        try {
          String result = (String) XMLUtils.getXPathResult(zipInputStream, xpathExpression, XPathConstants.STRING,
            Constants.NAMESPACE_FOR_TABLE);
          int count = Integer.parseInt(result);
          if (advancedOrStructuredColumnEntry.getValue().getFields().size() != count) {
            P_4311_ERRORS
              .add("Different number of fields in " + advancedOrStructuredColumnEntry.getKey() + " at " + XSDPath);
          }
        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          LOGGER.debug("Failed to validate {}", P_4311, e);
          return false;
        }
      }
    }

    return P_4311_ERRORS.isEmpty();
  }

  /**
   * P_4.3-12
   *
   * The number of lines in a table in metadata.xml must fit into the area
   * specified in the corresponding table[number].xsd. The number of lines in a
   * table in metadata.xml must be identical to the number of lines in the
   * corresponding table[number].xml.
   *
   * @return true if valid otherwise false
   */
  private boolean validateNumberOfLinesInATable() {
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    observer.notifyComponent("P_4.3-12", Status.START);

    for (Map.Entry<String, Integer> entry : numberOfRows.entrySet()) {
      String path = entry.getKey();
      String XSDPath = path.concat(Constants.XSD_EXTENSION);

      String XMLPath = path.concat(Constants.XML_EXTENSION);

      observer.notifyElementValidating(XMLPath);

      final InputStream zipInputStreamXML = getZipInputStream(XMLPath);
      final InputStream zipInputStreamXSD = getZipInputStream(XSDPath);
      int rows = entry.getValue();
      try {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        XMLStreamReader streamReader = factory.createXMLStreamReader(zipInputStreamXML);
        int numberOfRowsInXMLFile = 0;
        while (streamReader.hasNext()) {
          streamReader.next();
          if (streamReader.getEventType() == XMLStreamReader.START_ELEMENT) {
            if (streamReader.getLocalName().equals("row")) {
              numberOfRowsInXMLFile++;
            }
          }
        }

        Node result = (Node) XMLUtils.getXPathResult(zipInputStreamXSD,
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
          P_4312_ERRORS.add("The minOccurs attribute is negative at " + XSDPath);

        if (rows < minOccurs)
          P_4312_ERRORS
            .add("The number of rows at " + XMLPath + " is less than defined by minOccurs attribute at " + XSDPath);

        if (!maxOccursString.equals("unbounded")) {
          int maxOccurs = Integer.parseInt(maxOccursString);
          if (rows > maxOccurs)
            P_4312_ERRORS
              .add("The number of rows at " + XMLPath + " is more than defined by maxOccurs attribute at " + XSDPath);
        }

        /*
         * The number of lines in a table in metadata.xml must be identical to the
         * number of lines in the corresponding table[number].xml.
         */
        if (numberOfRowsInXMLFile != rows)
          P_4312_ERRORS.add(
            "The number of rows at " + XMLPath + " is not identical to " + validatorPathStrategy.getMetadataXMLPath());

      } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException
        | XMLStreamException e) {
        LOGGER.debug("Failed to validate {}", P_4312, e);
        return false;
      }
    }

    return P_4312_ERRORS.isEmpty();
  }

  /*
   * Auxiliary Methods
   */
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
    NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
      "/ns:siardArchive/ns:schemas/ns:schema",
      XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

    // Obtain Types
    for (int i = 0; i < nodes.getLength(); i++) {
      Element schema = (Element) nodes.item(i);
      String schemaName = schema.getElementsByTagName("name").item(0).getTextContent();
      NodeList types = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
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
    }
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
    NodeList resultNodes = (NodeList) XMLUtils.getXPathResult(
      getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), "/ns:siardArchive/ns:schemas/ns:schema",
      XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
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

        final String path = createPath(Constants.SIARD_CONTENT_FOLDER, schemaFolderName, tableFolderName,
          tableFolderName);
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
              sql2008Type.put(path, columnsSQL2008Map);
            } else {
              arrayMap.put(key, type);
              arrayType.put(path, arrayMap);
            }
          } else {
            advanceOrStructuredColumnMap.put(key,
              obtainAdvancedOrStructuredDataType(column, schemaName, tableName, columnName));
            advancedOrStructuredDataType.put(path, advanceOrStructuredColumnMap);
          }
        }
        numberOfNullable.put(path, nullableMap);
        numberOfRows.put(path, Integer.parseInt(rows));
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

    final NodeList resultNodes = (NodeList) XMLUtils.getXPathResult(
      getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), xpathExpression,
      XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

    for (int l = 0; l < resultNodes.getLength(); l++) {
      Element element = (Element) resultNodes.item(l);
      fields.add(getField(element, xpathExpression));
    }

    return new AdvancedOrStructuredColumn(columnName, typeSchema, typeName, fields);
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
    final NodeList fields = (NodeList) XMLUtils.getXPathResult(
      getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), concat,
      XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
    for (int l = 0; l < fields.getLength(); l++) {
      Element element = (Element) fields.item(l);
      field.addFieldToList(getField(element, concat));
    }

    return field;
  }

  private List<String> compareSQL2008DataTypeWithXMLType(String path, String column, HashMap<String, String> map)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    List<String> errors = new ArrayList<>();
    String XSDPath = path.concat(Constants.XSD_EXTENSION);
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

      String XMLType = (String) XMLUtils.getXPathResult(getZipInputStream(XSDPath), xpathExpression,
        XPathConstants.STRING, Constants.NAMESPACE_FOR_TABLE);

      if (!validateSQL2008TypeWithXMLType(entry.getValue(), XMLType)) {
        errors.add(obtainErrorMessage(entry.getValue(), XMLType, key, XSDPath));
      }
    }

    return errors;
  }

  private List<String> compareSQL2008DataTypeWithXMLType(String path, HashMap<String, String> map)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    List<String> errors = new ArrayList<>();
    String XSDPath = path.concat(Constants.XSD_EXTENSION);
    final NodeList nodeList = (NodeList) XMLUtils.getXPathResult(getZipInputStream(XSDPath),
      "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element", XPathConstants.NODESET, null);
    for (int i = 0; i < nodeList.getLength(); i++) {
      String XMLType = "";
      final Node item = nodeList.item(i);
      if (item.getAttributes().getNamedItem("type") != null) {
        XMLType = item.getAttributes().getNamedItem("type").getNodeValue();
      } else {
        Element sequence = (Element) item;
        final NodeList complexTypeNode = sequence.getElementsByTagName("xs:complexType");
        for (int j = 0; j < complexTypeNode.getLength(); j++) {
          Element complexType = (Element) complexTypeNode.item(i);
          final NodeList sequenceNodes = complexType.getElementsByTagName("xs:sequence");
          for (int k = 0; k < sequenceNodes.getLength(); k++) {
            XMLType = sequenceNodes.item(k).getAttributes().getNamedItem("type").getNodeValue();
          }
        }
      }

      String columnName = item.getAttributes().getNamedItem("name").getNodeValue();
      if (map.get(columnName) != null) {
        if (!validateSQL2008TypeWithXMLType(map.get(columnName), XMLType)) {
          errors.add(obtainErrorMessage(map.get(columnName), XMLType, columnName, XSDPath));
        }
      }
    }

    return errors;
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
    for (String pathInZip : getZipFileNames()) {
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
      if (SQL2008Type.matches(entry.getKey())) {
        final List<String> XMLTypeMatches = entry.getValue();
        return XMLTypeMatches.contains(XMLType);
      }
    }

    return false;
  }

  private void skip(final String step) {
    if (skipped.isEmpty()) {
      observer.notifyValidationStep(MODULE_NAME, step, Status.OK);
      getValidationReporter().validationStatus(step, Status.OK);
    } else {
      for (String skippedReason : skipped) {
        observer.notifyValidationStep(MODULE_NAME, step, Status.SKIPPED);
        getValidationReporter().skipValidation(step, skippedReason);
      }
      observer.notifyValidationStep(MODULE_NAME, step, Status.OK);
      getValidationReporter().validationStatus(step, Status.OK);
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
    SQL2008TypeMatchXMLType.put("^BINARY VARYING\\(\\d+\\)$", Arrays.asList("clobType", "xs:hexBinary"));
    SQL2008TypeMatchXMLType.put("^BINARY\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$",
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

  private static class TableNameComparator implements Comparator<String> {
    @Override
    public int compare(String s1, String s2) {
      final String[] split1 = s1.split("/");
      final String[] split2 = s2.split("/");
      final String schemaName1 = split1[1];
      final String schemaName2 = split2[1];
      final String tableName1 = split1[2];
      final String tableName2 = split1[2];

      Pattern patternSchema = Pattern.compile("schema([0-9]+)");
      Matcher matcherSchema1 = patternSchema.matcher(schemaName1);
      Matcher matcherSchema2 = patternSchema.matcher(schemaName2);

      int valueS1 = 0;
      int valueS2 = 0;

      while (matcherSchema1.find()) {
        valueS1 = Integer.parseInt(matcherSchema1.group(1));
      }

      while (matcherSchema2.find()) {
        valueS2 = Integer.parseInt(matcherSchema2.group(1));
      }

      Pattern patternTable = Pattern.compile("table([0-9]+)");
      Matcher matcher1 = patternTable.matcher(tableName1);

      int valueT1 = 0;
      int valueT2 = 0;

      while (matcher1.find()) {
        valueT1 = Integer.parseInt(matcher1.group(1));
      }

      Matcher matcher2 = patternTable.matcher(tableName2);
      while (matcher2.find()) {
        valueT2 = Integer.parseInt(matcher2.group(1));
      }

      return new CompareToBuilder().append(valueS1, valueS2).append(valueT1, valueT2).toComparison();
    }
  }
}
