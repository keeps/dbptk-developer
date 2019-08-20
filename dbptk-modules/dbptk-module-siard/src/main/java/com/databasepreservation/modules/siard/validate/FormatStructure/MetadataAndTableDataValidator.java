package com.databasepreservation.modules.siard.validate.FormatStructure;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
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
import com.databasepreservation.model.modules.validate.ValidatorModule;
import com.databasepreservation.model.reporters.ValidationReporter.Status;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class MetadataAndTableDataValidator extends ValidatorModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataAndTableDataValidator.class);

  private static final String MODULE_NAME = "Correspondence between metadata and table data";
  private static final String METADATA_XML = "header/metadata.xml";
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
  private static final String SIARD_CONTENT = "content";

  private static HashMap<String, List<String>> SQL2008TypeMatchXMLType;
  private HashMap<String, HashMap<String, String>> sql2008Type = new HashMap<>();
  private HashMap<String, HashMap<String, String>> arrayType = new HashMap<>();
  private HashMap<String, HashMap<String, AdvancedOrStructuredColumn>> advancedOrStructuredDataType = new HashMap<>();
  private HashMap<String, HashMap<String, String>> numberOfNullable = new HashMap<>();
  private HashMap<String, Integer> numberOfRows = new HashMap<>();
  private List<String> allowUTDs;
  private List<String> skipped = new ArrayList<>();
  private List<String> warnings = new ArrayList<>();
  private HashMap<String, Type> types = new HashMap<>();

  public static MetadataAndTableDataValidator newInstance() {
    return new MetadataAndTableDataValidator();
  }

  private MetadataAndTableDataValidator() {
    SQL2008TypeMatchXMLType = new HashMap<>();
    SQL2008TypeMatchXMLType.put("BIGINT", Collections.singletonList("xs:integer"));
    SQL2008TypeMatchXMLType.put("BINARY LARGE OBJECT", Collections.singletonList("blobType"));
    SQL2008TypeMatchXMLType.put("BLOB", Collections.singletonList("blobType"));
    SQL2008TypeMatchXMLType.put("BINARY VARYING", Arrays.asList("clobType", "xs:hexBinary"));
    SQL2008TypeMatchXMLType.put("BINARY", Arrays.asList("blobType", "xs:hexBinary"));
    SQL2008TypeMatchXMLType.put("BOOLEAN", Collections.singletonList("xs:boolean"));
    SQL2008TypeMatchXMLType.put("CHARACTER LARGE OBJECT", Collections.singletonList("clobType"));
    SQL2008TypeMatchXMLType.put("CLOB", Collections.singletonList("clobType"));
    SQL2008TypeMatchXMLType.put("CHARACTER VARYING", Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("CHAR VARYING", Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("VARCHAR", Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("CHARACTER", Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("CHAR", Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("DATE", Collections.singletonList("dateType"));
    SQL2008TypeMatchXMLType.put("DECIMAL", Collections.singletonList("xs:decimal"));
    SQL2008TypeMatchXMLType.put("DEC", Collections.singletonList("xs:decimal"));
    SQL2008TypeMatchXMLType.put("DOUBLE PRECISION", Collections.singletonList("xs:double"));
    SQL2008TypeMatchXMLType.put("FLOAT", Collections.singletonList("xs:double"));
    SQL2008TypeMatchXMLType.put("INTEGER", Collections.singletonList("xs:integer"));
    SQL2008TypeMatchXMLType.put("INT", Collections.singletonList("xs:integer"));
    SQL2008TypeMatchXMLType.put("INTERVAL YEAR", Collections.singletonList("xs:duration"));
    SQL2008TypeMatchXMLType.put("INTERVAL DAY", Collections.singletonList("xs:duration"));
    SQL2008TypeMatchXMLType.put("NATIONAL CHARACTER LARGE OBJECT", Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("NCHAR VARYING", Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("NATIONAL CHARACTER", Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("NCHAR", Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("NATIONAL CHAR", Arrays.asList("clobType", "xs:string"));
    SQL2008TypeMatchXMLType.put("NUMERIC", Collections.singletonList("xs:decimal"));
    SQL2008TypeMatchXMLType.put("REAL", Collections.singletonList("xs:float"));
    SQL2008TypeMatchXMLType.put("SMALLINT", Collections.singletonList("xs:integer"));
    SQL2008TypeMatchXMLType.put("TIME", Collections.singletonList("timeType"));
    SQL2008TypeMatchXMLType.put("TIME WITH TIME ZONE", Collections.singletonList("timeType"));
    SQL2008TypeMatchXMLType.put("TIMESTAMP", Collections.singletonList("dateTimeType"));
    SQL2008TypeMatchXMLType.put("TIMESTAMP WITH TIME ZONE", Collections.singletonList("dateTimeType"));
    SQL2008TypeMatchXMLType.put("XML", Collections.singletonList("clobType"));
  }

  public void setAllowUDTs(List<String> allowedUDTs) {
    this.allowUTDs = allowedUDTs;
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    try {
      obtainSQL2008TypeForEachColumn();
      getTypesFromMetadataXML();
    } catch (ParserConfigurationException | SAXException | XPathExpressionException | IOException e) {
      closeZipFile();
      return false;
    }

    getValidationReporter().moduleValidatorHeader(P_43, MODULE_NAME);

    if (validateMetadataStructure()) {
      getValidationReporter().validationStatus(P_431, Status.OK);
    } else {
      validationFailed(P_431, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateColumnCount()) {
      getValidationReporter().validationStatus(P_432, Status.OK);
    } else {
      validationFailed(P_432, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateDataTypeInformation()) {
      getValidationReporter().validationStatus(P_433, Status.OK);
    } else {
      validationFailed(P_433, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateDistinctXMLDataTypeConversion()) {
      for (String warning : warnings) {
        getValidationReporter().warning(P_434, "Not allow user-defined data type", warning);
      }
      if (skipped.isEmpty()) {
        getValidationReporter().validationStatus(P_434, Status.OK);
      } else {
        for (String skippedReason : skipped) {
          getValidationReporter().skipValidation(P_434, skippedReason);
        }
        getValidationReporter().validationStatus(P_434, Status.OK);
      }
    } else {
      validationFailed(P_434, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateNumberOfRows()) {
      getValidationReporter().validationStatus(P_435, Status.OK);
    } else {
      validationFailed(P_435, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateArrayXMLDataTypeConversion()) {
      getValidationReporter().validationStatus(P_436, Status.OK);
    } else {
      validationFailed(P_436, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateUDTXMLDataTypeConversion()) {
      getValidationReporter().validationStatus(P_437, Status.OK);
    } else {
      validationFailed(P_437, MODULE_NAME);
      closeZipFile();
      return false;
    }

    boolean result = validateNillableInformation();
    for (String warning : warnings) {
      getValidationReporter().warning(P_438, "Number of null fields", warning);
    }
    if (result) {
      getValidationReporter().validationStatus(P_438, Status.OK);
    } else {
      validationFailed(P_438, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateColumnSequence()) {
      getValidationReporter().validationStatus(P_439, Status.OK);
    } else {
      validationFailed(P_439, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateFieldSequenceInMetadataXML()) {
      getValidationReporter().validationStatus(P_4310, Status.OK);
    } else {
      validationFailed(P_4310, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateFieldSequenceInTableXSD()) {
      getValidationReporter().validationStatus(P_4311, Status.OK);
    } else {
      validationFailed(P_4311, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateNumberOfLinesInATable()) {
      getValidationReporter().validationStatus(P_4312, Status.OK);
    } else {
      validationFailed(P_4312, MODULE_NAME);
      closeZipFile();
      return false;
    }

    getValidationReporter().moduleValidatorFinished(MODULE_NAME, Status.OK);
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
    if (preValidationRequirements())
      return false;

    final InputStream zipInputStream = getZipInputStream(METADATA_XML);

    try {
      final NodeList nodeList = (NodeList) XMLUtils.getXPathResult(zipInputStream,
        "/ns:siardArchive/ns:schemas/ns:schema", XPathConstants.NODESET, Constants.NAME_SPACE_FOR_METADATA);
      for (int i = 0; i < nodeList.getLength(); i++) {
        Element schema = (Element) nodeList.item(i);
        String schemaFolderName = schema.getElementsByTagName("folder").item(0).getTextContent();
        NodeList tableNodes = schema.getElementsByTagName("table");
        for (int j = 0; j < tableNodes.getLength(); j++) {
          Element table = (Element) tableNodes.item(j);
          String tableFolderName = table.getElementsByTagName("folder").item(0).getTextContent();
          final String pathToValidate = createPath("content", schemaFolderName, tableFolderName);
          if (!checkRelativePathExists(pathToValidate)) {
            return false;
          }
        }
      }
    } catch (IOException | XPathExpressionException | SAXException | ParserConfigurationException e) {
      return false;
    }

    return true;
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
    if (preValidationRequirements())
      return false;

    HashMap<String, Integer> columnCount = new HashMap<>();
    List<String> entries = new ArrayList<>();

    final InputStream zipInputStream = getZipInputStream(METADATA_XML);

    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(zipInputStream, "/ns:siardArchive/ns:schemas/ns:schema",
        XPathConstants.NODESET, Constants.NAME_SPACE_FOR_METADATA);

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
          final String path = createPath(SIARD_CONTENT, schemaFolderName, tableFolderName, tableFolderName);
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
          return false;
        }
      }
    } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
      return false;
    }

    return true;
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
    if (preValidationRequirements())
      return false;

    for (Map.Entry<String, HashMap<String, String>> entry : sql2008Type.entrySet()) {
      try {
        if (!compareSQL2008DataTypeWithXMLType(entry.getKey(), entry.getValue())) {
          return false;
        }
      } catch (SAXException | ParserConfigurationException | IOException | XPathExpressionException e) {
        return false;
      }
    }

    return true;
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
    if (preValidationRequirements())
      return false;

    warnings = new ArrayList<>();

    for (Type type : types.values()) {
      if (type.getCategory().equalsIgnoreCase("distinct")) {
        if (!allowUTDs.contains(type.getName())) {
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
      } // Compare to the base type with the XML type from table[number].xsd
      for (Map.Entry<String, HashMap<String, String>> entry : distinctTypes.entrySet()) {
        if (!compareSQL2008DataTypeWithXMLType(entry.getKey(), entry.getValue()))
          return false;
      }

    } catch (SAXException | ParserConfigurationException | IOException | XPathExpressionException e) {
      return false;
    }

    return true;
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
    if (preValidationRequirements())
      return false;

    try {
      // Count number of row element in table[number].xml
      for (Map.Entry<String, Integer> entry : numberOfRows.entrySet()) {
        String tablePath = entry.getKey();
        String XMLPath = tablePath.concat(Constants.XML_EXTENSION);

        final InputStream zipInputStream = getZipInputStream(XMLPath);

        String count = (String) XMLUtils.getXPathResult(zipInputStream, "count(/ns:table/ns:row)",
          XPathConstants.STRING, Constants.NAME_SPACE_FOR_TABLE);
        if (entry.getValue() != Integer.parseInt(count)) {
          return false;
        }
      }
    } catch (ParserConfigurationException | IOException | XPathExpressionException | SAXException e) {
      return false;
    }

    return true;
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
    if (preValidationRequirements())
      return false;

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
              return false;
            }
          }
        } catch (IOException | XPathExpressionException | SAXException | ParserConfigurationException e) {
          return false;
        }
      }
    }

    return true;
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
    if (preValidationRequirements())
      return false;

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
              if (!compareSQL2008DataTypeWithXMLType(entry.getKey(), advancedOrStructuredColumn.getKey(), map)) {
                System.out.println(entry.getKey());
                System.out.println(advancedOrStructuredColumn.getKey());
                return false;
              }
            } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException e) {
              return false;
            }
          }
        }
      }
    }

    return true;
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
    if (preValidationRequirements())
      return false;

    warnings.clear();
    int counter = 0;
    try {
      for (Map.Entry<String, HashMap<String, String>> entry : numberOfNullable.entrySet()) {
        for (Map.Entry<String, String> values : entry.getValue().entrySet()) {
          if (values.getValue().equals("false")) {
            String tablePath = entry.getKey();
            String XMLPath = tablePath.concat(Constants.XML_EXTENSION);
            String xpathExpression = "/ns:table/ns:row/ns:" + values.getKey() + "/text()";

            final InputStream zipInputStream = getZipInputStream(XMLPath);

            final NodeList nodes = (NodeList) XMLUtils.getXPathResult(zipInputStream, xpathExpression,
              XPathConstants.NODESET, Constants.NAME_SPACE_FOR_TABLE);
            final Integer number = numberOfRows.get(entry.getKey());
            if (number != nodes.getLength()) {
              counter++;
            }
          }
        }
      }

    } catch (ParserConfigurationException | IOException | XPathExpressionException | SAXException e) {
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
    if (preValidationRequirements())
      return false;

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
    if (preValidationRequirements())
      return false;

    for (Map.Entry<String, HashMap<String, AdvancedOrStructuredColumn>> entry : advancedOrStructuredDataType
      .entrySet()) {
      for (Map.Entry<String, AdvancedOrStructuredColumn> advancedOrStructuredColumnEntry : entry.getValue()
        .entrySet()) {
        final List<Field> fields = advancedOrStructuredColumnEntry.getValue().getFields();
        String key = advancedOrStructuredColumnEntry.getValue().getTypeSchema() + "."
          + advancedOrStructuredColumnEntry.getValue().getTypeName();
        final List<Attribute> attributes = types.get(key).getAttributes();

        if (!validateFieldSequence(fields, attributes))
          return false;
      }
    }

    return true;
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
    if (preValidationRequirements())
      return false;

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
            Constants.NAME_SPACE_FOR_TABLE);
          int count = Integer.parseInt(result);
          if (advancedOrStructuredColumnEntry.getValue().getFields().size() != count)
            return false;
        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          return false;
        }
      }
    }

    return true;
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
    if (preValidationRequirements())
      return false;

    for (Map.Entry<String, Integer> entry : numberOfRows.entrySet()) {
      String path = entry.getKey();
      String XSDPath = path.concat(Constants.XSD_EXTENSION);

      String XMLPath = path.concat(Constants.XML_EXTENSION);
      final InputStream zipInputStreamXML = getZipInputStream(XMLPath);
      final InputStream zipInputStreamXSD = getZipInputStream(XSDPath);
      int rows = entry.getValue();
      try {
        int numberOfRowsInXMLFile = Integer
          .parseInt((String) XMLUtils.getXPathResult(zipInputStreamXML, "count(/ns:table/ns:row)",
            XPathConstants.STRING, Constants.NAME_SPACE_FOR_TABLE));

        Node result = (Node) XMLUtils.getXPathResult(zipInputStreamXSD,
          "/xs:schema/xs:element[@name='table']/xs:complexType/xs:sequence/xs:element[@type='recordType']",
          XPathConstants.NODE, Constants.NAME_SPACE_FOR_TABLE);
        String minOccursString = result.getAttributes().getNamedItem("minOccurs").getNodeValue();
        String maxOccursString = result.getAttributes().getNamedItem("maxOccurs").getNodeValue();

        /*
         * The number of lines in a table in metadata.xml must fit into the area
         * specified in the corresponding table[number].xsd.
         */
        int minOccurs = Integer.parseInt(minOccursString);
        if (minOccurs < 0)
          return false;

        if (rows < minOccurs)
          return false;

        if (!maxOccursString.equals("unbounded")) {
          int maxOccurs = Integer.parseInt(maxOccursString);

          if (rows > maxOccurs)
            return false;
        }

        /*
         * The number of lines in a table in metadata.xml must be identical to the
         * number of lines in the corresponding table[number].xml.
         */
        if (numberOfRowsInXMLFile != rows)
          return false;

      } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
        return false;
      }
    }

    return true;
  }

  /*
   * Auxiliary Methods
   */
  private boolean validateFieldSequence(List<Field> fields, List<Attribute> attributes) {
    if (attributes.size() != fields.size())
      return false;
    for (int i = 0; i < attributes.size(); i++) {
      Attribute attribute = attributes.get(i);
      Field field = fields.get(i);

      if (!attribute.getName().equals(field.getName()))
        return false;

      if (attribute.haveSuperType()) {
        String key = attribute.getTypeSchema() + "." + attribute.getTypeName();
        return validateFieldSequence(field.getFields(), types.get(key).getAttributes());
      }
    }

    return true;
  }

  private void getTypesFromMetadataXML()
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    final InputStream zipInputStream = getZipInputStream(METADATA_XML);
    NodeList nodes = (NodeList) XMLUtils.getXPathResult(zipInputStream, "/ns:siardArchive/ns:schemas/ns:schema",
      XPathConstants.NODESET, Constants.NAME_SPACE_FOR_METADATA);

    // Obtain Types
    for (int i = 0; i < nodes.getLength(); i++) {
      Element schema = (Element) nodes.item(i);
      String schemaName = schema.getElementsByTagName("name").item(0).getTextContent();
      final InputStream inputStream = getZipInputStream(METADATA_XML);
      NodeList types = (NodeList) XMLUtils.getXPathResult(inputStream,
        "/ns:siardArchive/ns:schemas/ns:schema[ns:name/text() = '" + schemaName + "']/ns:types/ns:type",
        XPathConstants.NODESET, Constants.NAME_SPACE_FOR_METADATA);

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
    final InputStream zipInputStream = getZipInputStream(METADATA_XML);

    NodeList resultNodes = (NodeList) XMLUtils.getXPathResult(zipInputStream, "/ns:siardArchive/ns:schemas/ns:schema",
      XPathConstants.NODESET, Constants.NAME_SPACE_FOR_METADATA);
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

        final String path = createPath(SIARD_CONTENT, schemaFolderName, tableFolderName, tableFolderName);
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

    final NodeList resultNodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(METADATA_XML), xpathExpression,
      XPathConstants.NODESET, Constants.NAME_SPACE_FOR_METADATA);

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
    final NodeList fields = (NodeList) XMLUtils.getXPathResult(getZipInputStream(METADATA_XML), concat,
      XPathConstants.NODESET, Constants.NAME_SPACE_FOR_METADATA);
    for (int l = 0; l < fields.getLength(); l++) {
      Element element = (Element) fields.item(l);
      field.addFieldToList(getField(element, concat));
    }

    return field;
  }

  private boolean compareSQL2008DataTypeWithXMLType(String path, String column, HashMap<String, String> map)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
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
        XPathConstants.STRING, Constants.NAME_SPACE_FOR_TABLE);

      if (!validateSQL2008TypeWithXMLType(entry.getValue(), XMLType)) {
        return false;
      }
    }

    return true;
  }

  private boolean compareSQL2008DataTypeWithXMLType(String path, HashMap<String, String> map)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    String XSDPath = path.concat(Constants.XSD_EXTENSION);
    final NodeList nodeList = (NodeList) XMLUtils.getXPathResult(getZipInputStream(XSDPath),
      "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element", XPathConstants.NODESET, null);
    for (int i = 0; i < nodeList.getLength(); i++) {
      String XMLType = null;
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
          return false;
        }
      }
    }

    return true;
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
      sb.append(parameter).append("/");
    }
    sb.deleteCharAt(sb.length() - 1);

    return sb.toString();
  }

  private boolean validateSQL2008TypeWithXMLType(final String SQL2008Type, final String XMLType) {
    final int indexOf = SQL2008Type.indexOf("(");
    List<String> XMLTypeMatches;
    if (indexOf != -1) {
      String type = SQL2008Type.substring(0, indexOf);
      XMLTypeMatches = SQL2008TypeMatchXMLType.get(type);
    } else {
      XMLTypeMatches = SQL2008TypeMatchXMLType.get(SQL2008Type);
    }

    if (XMLTypeMatches != null) {
      return XMLTypeMatches.contains(XMLType);
    }

    return false;
  }

  private static class Type {
    private String schemaName;
    private String name;
    private String category;
    private String underSchema;
    private String underType;
    private String instantiable;
    private String _final;
    private String base;
    private List<Attribute> attributes;
    private String description;

    Type(String schemaName, String name, String category, String underSchema, String underType, String instantiable,
      String _final, String base, List<Attribute> attributes, String description) {
      this.schemaName = schemaName;
      this.name = name;
      this.category = category;
      this.underSchema = underSchema;
      this.underType = underType;
      this.instantiable = instantiable;
      this._final = _final;
      this.base = base;
      this.attributes = attributes;
      this.description = description;
    }

    public String getSchemaName() {
      return schemaName;
    }

    public String getName() {
      return name;
    }

    public String getCategory() {
      return category;
    }

    public String getUnderSchema() {
      return underSchema;
    }

    public String getUnderType() {
      return underType;
    }

    public String getInstantiable() {
      return instantiable;
    }

    public String get_final() {
      return _final;
    }

    public String getBase() {
      return base;
    }

    public List<Attribute> getAttributes() {
      return attributes;
    }

    public String getDescription() {
      return description;
    }
  }

  private static class Attribute {
    private String name;
    private String type;
    private String typeOriginal;
    private String nullable;
    private String typeSchema;
    private String typeName;
    private String defaultValue;
    private String description;
    private String cardinality;

    Attribute(String name, String type, String typeOriginal, String nullable, String typeSchema, String typeName,
      String defaultValue, String description, String cardinality) {
      this.name = name;
      this.type = type;
      this.typeOriginal = typeOriginal;
      this.nullable = nullable;
      this.typeSchema = typeSchema;
      this.typeName = typeName;
      this.defaultValue = defaultValue;
      this.description = description;
      this.cardinality = cardinality;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public String getTypeOriginal() {
      return typeOriginal;
    }

    public String getNullable() {
      return nullable;
    }

    public String getTypeSchema() {
      return typeSchema;
    }

    public String getTypeName() {
      return typeName;
    }

    public String getDefaultValue() {
      return defaultValue;
    }

    public String getDescription() {
      return description;
    }

    public String getCardinality() {
      return cardinality;
    }

    public boolean haveSuperType() {
      return StringUtils.isNotBlank(this.typeSchema) && StringUtils.isNotBlank(this.typeName);
    }
  }

  private class Field {
    private String name;
    private List<Field> fields;

    public Field() {
      this.fields = new ArrayList<>();
    }

    public Field(String name, List<Field> fields) {
      this.name = name;
      this.fields = fields;
    }

    public String getName() {
      return name;
    }

    public List<Field> getFields() {
      return fields;
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setFields(List<Field> fields) {
      this.fields = fields;
    }

    public void addFieldToList(Field field) {
      this.fields.add(field);
    }
  }

  private class AdvancedOrStructuredColumn {
    private String name;
    private String typeSchema;
    private String typeName;
    private List<Field> fields;

    public AdvancedOrStructuredColumn(String name, String typeSchema, String typeName, List<Field> fields) {
      this.name = name;
      this.typeSchema = typeSchema;
      this.typeName = typeName;
      this.fields = fields;
    }

    public String getName() {
      return name;
    }

    public String getTypeSchema() {
      return typeSchema;
    }

    public String getTypeName() {
      return typeName;
    }

    public List<Field> getFields() {
      return fields;
    }

    public String getType() {
      return this.typeSchema + "." + this.typeName;
    }
  }
}
