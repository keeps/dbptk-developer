package com.databasepreservation.modules.siard.validate.component.tableData;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter.Status;
import com.databasepreservation.model.validator.SIARDContent;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class AdditionalChecksValidator extends ValidatorComponentImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdditionalChecksValidator.class);

  private final String MODULE_NAME;
  private TreeMap<SIARDContent, List<String>> tableAndColumns = new TreeMap<>();
  private TreeMap<SIARDContent, List<String>> foreignKeyColumns = new TreeMap<>();
  private TreeMap<SIARDContent, List<ImmutablePair<String, String>>> columnTypes = new TreeMap<>();
  private HashMap<SIARDContent, Integer> rows = new HashMap<>();

  private List<String> dataTypeErrors = new ArrayList<>();

  public AdditionalChecksValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    tableAndColumns.clear();
    foreignKeyColumns.clear();
    columnTypes.clear();
    rows.clear();
    dataTypeErrors.clear();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, "");
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    if (!obtainValidationData()) {
      LOGGER.debug("Failed to obtain data for {}", MODULE_NAME);
      closeZipFile();
      return false;
    }

    getValidationReporter().moduleValidatorHeader(MODULE_NAME);

    outputDifferentBlobsTypes();

    numberOfNullValuesForForeignKey();

    if (validateTableDataType()) {
      observer.notifyValidationStep(MODULE_NAME, "Validate Data Type", Status.OK);
      getValidationReporter().validationStatus("Validate Data Type", Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, "Validate Data Type", Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed("Validate Data Type", MODULE_NAME, "", "Data type invalid", dataTypeErrors);
      closeZipFile();
      return false;
    }

    observer.notifyFinishValidationModule(MODULE_NAME, Status.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, Status.PASSED);
    closeZipFile();

    return true;
  }

  /**
   * if <type> is integer then all content should be integer, same with decimal,
   * date, boolean etc.
   * 
   * @return true if valid otherwise false
   */
  private boolean validateTableDataType() {
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    observer.notifyMessage(MODULE_NAME, "Validating data type content", Status.START);

    for (final Map.Entry<SIARDContent, List<ImmutablePair<String, String>>> entry : columnTypes.entrySet()) {
      final String path = validatorPathStrategy.getXMLTablePathFromFolder(entry.getKey().getSchema(),
        entry.getKey().getTable());
      observer.notifyElementValidating(path);
      try {
        boolean rowTag = false;
        StringWriter stringWriter = new StringWriter();
        final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        XMLStreamWriter XMLStreamWriter = null;
        final XMLStreamReader xmlStreamReader = xmlInputFactory.createXMLStreamReader(getZipInputStream(path));
        while (xmlStreamReader.hasNext()) {
          xmlStreamReader.next();

          if (xmlStreamReader.getEventType() == XMLStreamConstants.START_ELEMENT) {
            if (xmlStreamReader.getLocalName().equals("row")) {
              stringWriter = new StringWriter();
              XMLStreamWriter = XMLOutputFactory.newFactory().createXMLStreamWriter(stringWriter);
              XMLStreamWriter.writeStartDocument();
              XMLStreamWriter.writeStartElement(xmlStreamReader.getPrefix(), xmlStreamReader.getLocalName(),
                xmlStreamReader.getNamespaceURI());
              rowTag = true;
            }
          }

          if (rowTag) {
            switch (xmlStreamReader.getEventType()) {
              case XMLStreamConstants.START_ELEMENT:
                if (!xmlStreamReader.getLocalName().equals("row")) {
                  XMLStreamWriter.writeStartElement(xmlStreamReader.getPrefix(), xmlStreamReader.getLocalName(),
                    xmlStreamReader.getNamespaceURI());
                }
                break;
              case XMLStreamConstants.END_ELEMENT:
                if (!xmlStreamReader.getLocalName().equals("row")) {
                  XMLStreamWriter.writeEndElement();
                }
                break;
              case XMLStreamConstants.CHARACTERS:
                int start = xmlStreamReader.getTextStart();
                int length = xmlStreamReader.getTextLength();
                XMLStreamWriter.writeCharacters(xmlStreamReader.getTextCharacters(), start, length);
                break;
            }
          }

          if (xmlStreamReader.getEventType() == XMLStreamConstants.END_ELEMENT) {
            if (xmlStreamReader.getLocalName().equals("row")) {
              if (XMLStreamWriter != null) {
                XMLStreamWriter.writeEndElement();
                XMLStreamWriter.writeEndDocument();
                XMLStreamWriter.flush();
                XMLStreamWriter.close();
              }

              validateDataTypeForEachXMLFraction(entry.getValue(), stringWriter, path);
              rowTag = false;
            }
          }
        }
      } catch (XMLStreamException | ParserConfigurationException | IOException | SAXException
        | XPathExpressionException e) {
        LOGGER.debug("Failed to validate {}", MODULE_NAME, e);
        return false;
      }
    }

    observer.notifyMessage(MODULE_NAME, "Validating data type content", Status.FINISH);
    return dataTypeErrors.isEmpty();
  }

  /**
   * validation should output all the different BLOB file types (pdf, tif, png,
   * wav, mpeg-1, mpeg-2, mpeg-4 as mpg)*
   * 
   */
  private void outputDifferentBlobsTypes() {
    NodeList result;
    try {
      result = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:columns/ns:column/ns:mimeType/text()",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
      LOGGER.error(e.getLocalizedMessage());
      return;
    }

    for (int i = 0; i < result.getLength(); i++) {
      final String nodeValue = result.item(i).getNodeValue();
      getValidationReporter().notice(nodeValue, "MimeType found");
    }
  }

  /**
   * Count the number of null values for each foreign key
   *
   */
  private void numberOfNullValuesForForeignKey() {
    XMLInputFactory factory = XMLInputFactory.newInstance();
    for (Map.Entry<SIARDContent, List<String>> entry : foreignKeyColumns.entrySet()) {
      String path = validatorPathStrategy.getXMLTablePathFromFolder(entry.getKey().getSchema(),
        entry.getKey().getTable());
      observer.notifyElementValidating(path);

      HashMap<String, Integer> countColumnsMap = new HashMap<>();
      int numberOfNulls;
      try {
        XMLStreamReader streamReader = factory.createXMLStreamReader(getZipInputStream(path));
        while (streamReader.hasNext()) {
          streamReader.next();
          if (streamReader.getEventType() == XMLStreamReader.START_ELEMENT) {
            final String tagName = streamReader.getLocalName();
            for (String column : entry.getValue()) {
              final int indexOf = tableAndColumns.get(entry.getKey()).indexOf(column) + 1;
              String columnIndex = "c" + indexOf;
              if (tagName.equals(columnIndex)) {
                updateCounter(countColumnsMap, columnIndex);
              }
            }
          }
        }

        final Integer metadataXMLNumberOfRows = rows.get(entry.getKey());

        for (Map.Entry<String, Integer> counters : countColumnsMap.entrySet()) {
          if (!counters.getValue().equals(metadataXMLNumberOfRows)) {
            numberOfNulls = metadataXMLNumberOfRows - counters.getValue();
            getValidationReporter().notice(numberOfNulls,
              "Number of null values for " + counters.getKey() + " in " + path);
          }
        }

      } catch (XMLStreamException e) {
        LOGGER.debug("Failed to validate {}", MODULE_NAME, e);
        return;
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

  private void validateDataTypeForEachXMLFraction(final List<ImmutablePair<String, String>> pairs,
    final StringWriter stringWriter, final String zipFilePath)
    throws IOException, SAXException, ParserConfigurationException, XPathExpressionException {
    final Document document = XMLUtils.convertStringToDocument(stringWriter.toString());

    for (ImmutablePair<String, String> pair : pairs) {
      String columnContent = pair.getLeft();
      String type = pair.getRight();
      String xpathExpression;
      if (columnContent.contains(".")) {
        final String[] split = columnContent.split("\\.");
        columnContent = split[0];
        String udt = "";
        for (int i = 1; i < split.length; i++) {
          String ns = "/";
          udt = udt.concat(ns.concat(split[i]));
        }
        xpathExpression = "/row/$1";
        xpathExpression = xpathExpression.replace("$1", columnContent);
        xpathExpression = xpathExpression.concat(udt);
      } else {
        xpathExpression = "/row/$1";
        xpathExpression = xpathExpression.replace("$1", columnContent);
      }

      NodeList result = (NodeList) XMLUtils.getXPathResult(document, xpathExpression, XPathConstants.NODESET);
      for (int i = 0; i < result.getLength(); i++) {
        final int size = result.item(i).getChildNodes().getLength();
        if (size > 1) {
          for (int j = 0; j < result.item(i).getChildNodes().getLength(); j++) {
            final String nodeName = result.item(i).getChildNodes().item(j).getNodeName();
            if (nodeName.matches("a[0-9]+")) {
              final String content = result.item(i).getChildNodes().item(j).getTextContent();
              if (!validateType(content, type)) {
                dataTypeErrors.add(content + " is not in conformity with '" + type + "' type in " + zipFilePath);
              }
            }
          }
        } else {
          final String content = result.item(i).getTextContent();
          if (!validateType(content, type)) {
            dataTypeErrors.add(content + " is not in conformity with '" + type + "' type in " + zipFilePath);
          }
        }
      }
    }
  }

  private boolean validateType(String content, String type) {

    final String bigIntRegex = "^BIGINT$";
    final String integerRegex = "^INTEGER$|^INT$";
    final String smallIntRegex = "^SMALLINT$";
    final String decimalRegex = "^DECIMAL(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?$|"
      + "^DEC(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?$|"
      + "^NUMERIC(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?$";
    final String realRegex = "^REAL$";
    final String floatRegex = "^FLOAT(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$";
    final String doublePrecisionRegex = "^DOUBLE PRECISION$";
    final String stringRegex = "^CHARACTER\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$|"
      + "^CHAR\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$|" + "^VARCHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$|"
      + "^CHARACTER(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$|" + "^CHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$|"
      + "^NATIONAL\\s+CHARACTER\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$|"
      + "^NATIONAL\\s+CHAR\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$|"
      + "^NCHAR\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$|" + "^NATIONAL\\s+CHARACTER(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$|"
      + "^NATIONAL\\s+CHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$|" + "^NCHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$";
    final String booleanRegex = "^BOOLEAN$";
    final String dateRegex = "^DATE$";
    final String timeRegex = "^TIME(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$|"
      + "^TIME\\s+WITH\\s+TIME\\s+ZONE(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$";
    final String timestampRegex = "^TIMESTAMP(\\s*\\(\\s*(0|([1-9]\\d*))\\s*\\))?$|"
      + "^TIMESTAMP\\s+WITH\\s+TIME\\s+ZONE(\\s*\\(\\s*(0|([1-9]\\d*))\\s*\\))?$";
    final String binaryRegex = "^BINARY VARYING\\(\\d+\\)$|" + "^BINARY\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$|"
      + "^VARBINARY(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$";

    try {
      if (type.matches(integerRegex))
        Integer.parseInt(content);

      if (type.matches(smallIntRegex))
        Short.parseShort(content);

      if (type.matches(decimalRegex)) {
        return checkDecimalNumericDataType(content, type);
      }

      if (type.matches(booleanRegex)) {
        return Boolean.FALSE.toString().equals(content) || Boolean.TRUE.toString().equals(content);
      }

      if (type.matches(realRegex)) {
        Float.parseFloat(content);
      }

      if (type.matches(floatRegex)) {
        Double.parseDouble(content);
      }

      if (type.matches(bigIntRegex)) {
        Long.parseLong(content);
      }

      if (type.matches(doublePrecisionRegex)) {
        Double.parseDouble(content);
      }

      if (type.matches(dateRegex)) {
        String pattern = "\\d{4}-\\d{2}-\\d{2}Z?";

        return content.matches(pattern);
      }

      if (type.matches(timeRegex)) {
        String pattern = "\\d{2}:\\d{2}:\\d{2}Z?";

        return content.matches(pattern);
      }

      if (type.matches(timestampRegex)) {
        String pattern = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d*)Z?";

        return content.matches(pattern);
      }

      if (type.matches(stringRegex)) {
        final String decodeString = StringEscapeUtils.unescapeJava(content);

        final int size = getDataTypeLength(type);

        if (size != -1) {
          if (decodeString.length() > size) {
            return false;
          }
        }
      }

      if (type.matches(binaryRegex)) {
        final byte[] bytes = DatatypeConverter.parseHexBinary(content);
        final int size = getDataTypeLength(type);

        if (size != -1) {
          if (bytes.length > size) {
            return false;
          }
        }
      }

    } catch (NumberFormatException e) {
      return false;
    }
    return true;
  }

  private int getDataTypeLength(String type) {
    Pattern pattern = Pattern.compile("\\d+");
    Matcher matcher = pattern.matcher(type);

    int size = -1;

    while(matcher.find()) {
      size = Integer.parseInt(matcher.group(0));
    }

    return size;
  }

  private boolean checkDecimalNumericDataType(String content, String type) {
    final BigDecimal bigDecimal = new BigDecimal(content);
    final Pattern compile = Pattern.compile("^(DECIMAL|NUMERIC|DEC)(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?$");
    Matcher matcher = compile.matcher(type);

    String dataTypeName = null;
    String precisionAndScale = null;
    int typePrecision, typeScale;

    while (matcher.find()) {
      dataTypeName = matcher.group(1);
      precisionAndScale = matcher.group(2);
    }

    if (dataTypeName == null)
      dataTypeName = "null";

    if (precisionAndScale != null) {
      precisionAndScale = precisionAndScale.replaceAll("([()])", "");
      precisionAndScale = StringUtils.deleteWhitespace(precisionAndScale);

      if (precisionAndScale.contains(",")) {
        final String[] split = precisionAndScale.split(",");
        typePrecision = Integer.parseInt(split[0]);
        typeScale = Integer.parseInt(split[1]);
      } else {
        typePrecision = Integer.parseInt(precisionAndScale);
        typeScale = 0;
      }
    } else {
      switch (dataTypeName) {
        case "DECIMAL":
        case "DEC":
        case "NUMERIC":
          LOGGER
            .debug("Precision and scale not specified falling back to SQL standard and treat the value as an INTEGER");
          typePrecision = 10;
          typeScale = 0;
          break;
        case "null":
        default:
          LOGGER.debug("Unable to detect the data type name, missing one of this keywords: DECIMAL, DEC or NUMERIC");
          LOGGER.debug("Assuming default precision of a NUMERIC data type");
          typePrecision = 10;
          typeScale = 0;
          break;
      }
    }

    final int precision = bigDecimal.precision();
    final int scale = bigDecimal.scale();

    return precision <= typePrecision && scale <= typeScale;
  }

  private boolean obtainValidationData() {
    NodeList result;
    try {
      result = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:folder/text()", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
      LOGGER.error(e.getLocalizedMessage());
      return false;
    }

    for (int i = 0; i < result.getLength(); i++) {
      final String schemaFolder = result.item(i).getNodeValue();
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table/ns:folder/text()";
      xpathExpression = xpathExpression.replace("$1", schemaFolder);
      try {
        NodeList tables = (NodeList) XMLUtils.getXPathResult(
          getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), xpathExpression, XPathConstants.NODESET,
          Constants.NAMESPACE_FOR_METADATA);

        for (int j = 0; j < tables.getLength(); j++) {
          final String tableFolder = tables.item(j).getNodeValue();
          getColumnNames(schemaFolder, tableFolder);
          getForeignKeys(schemaFolder, tableFolder);
          getNumberOfRows(schemaFolder, tableFolder);
          getColumnType(schemaFolder, tableFolder);
        }
      } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
        LOGGER.error(e.getLocalizedMessage());
        return false;
      }
    }

    return true;
  }

  private void getColumnType(String schemaFolder, String tableFolder)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:columns/ns:column/ns:name/text()";
    xpathExpression = xpathExpression.replace("$1", schemaFolder);
    xpathExpression = xpathExpression.replace("$2", tableFolder);

    NodeList columns = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
      xpathExpression, XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

    List<ImmutablePair<String, String>> pairs = new ArrayList<>();
    for (int j = 0; j < columns.getLength(); j++) {
      String columnName = columns.item(j).getNodeValue();
      xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:columns/ns:column[ns:name/text()='$3']/ns:type/text()";
      xpathExpression = xpathExpression.replace("$1", schemaFolder);
      xpathExpression = xpathExpression.replace("$2", tableFolder);
      xpathExpression = xpathExpression.replace("$3", columnName);

      String type = (String) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        xpathExpression, XPathConstants.STRING, Constants.NAMESPACE_FOR_METADATA);
      ImmutablePair<String, String> pair;
      String index = "c" + (j + 1);
      if (StringUtils.isBlank(type)) {
        xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:columns/ns:column[ns:name/text()='$3']";
        xpathExpression = xpathExpression.replace("$1", schemaFolder);
        xpathExpression = xpathExpression.replace("$2", tableFolder);
        xpathExpression = xpathExpression.replace("$3", columnName);
        pairs.addAll(getAdvancedOrUDTData(xpathExpression, index));
      } else {
        pair = new ImmutablePair<>(index, type);
        pairs.add(pair);
      }
    }

    columnTypes.put(new SIARDContent(schemaFolder, tableFolder), pairs);
  }

  private List<ImmutablePair<String, String>> getAdvancedOrUDTData(final String xpathExpression, final String index)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    NodeList result = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
      xpathExpression, XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

    List<ImmutablePair<String, String>> pairs = new ArrayList<>();

    for (int i = 0; i < result.getLength(); i++) {
      Element element = (Element) result.item(i);
      final String typeSchema = element.getElementsByTagName("typeSchema").item(0).getTextContent();
      final String typeName = element.getElementsByTagName("typeName").item(0).getTextContent();

      pairs.addAll(getUDT(typeSchema, typeName, index));

      String distinctXPathExpression = xpathExpression.replace("$3", Constants.DISTINCT);
      distinctXPathExpression = distinctXPathExpression.concat("/ns:base/text()");

      final ImmutablePair<String, String> distinct = getDistinct(distinctXPathExpression, index);

      if (distinct != null) {
        pairs.add(distinct);
      }
    }

    return pairs;
  }

  private ImmutablePair<String, String> getDistinct(final String xpathExpression, final String index)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {

    String type = (String) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
      xpathExpression, XPathConstants.STRING, Constants.NAMESPACE_FOR_METADATA);

    if (StringUtils.isBlank(type)) {
      return null;
    } else {
      return new ImmutablePair<>(index, type);
    }
  }

  private List<ImmutablePair<String, String>> getUDT(String typeSchema, String typeName, String index)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {

    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:name/text()='$1']/ns:types/ns:type[ns:name/text()='$2' and ns:category/text()='$3']/ns:attributes/ns:attribute";
    xpathExpression = xpathExpression.replace("$1", typeSchema);
    xpathExpression = xpathExpression.replace("$2", typeName);
    xpathExpression = xpathExpression.replace("$3", Constants.UDT);

    List<ImmutablePair<String, String>> pairs = new ArrayList<>();

    NodeList nodeList = (NodeList) XMLUtils.getXPathResult(
      getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), xpathExpression, XPathConstants.NODESET,
      Constants.NAMESPACE_FOR_METADATA);
    for (int i = 0; i < nodeList.getLength(); i++) {
      Element element = (Element) nodeList.item(i);
      String concatIndex = index.concat(".u" + (i + 1));
      if (element.getElementsByTagName("type").item(0) != null) {
        final String type = element.getElementsByTagName("type").item(0).getTextContent();
        ImmutablePair<String, String> pair = new ImmutablePair<>(concatIndex, type);
        pairs.add(pair);
      } else {
        final String recTypeSchema = element.getElementsByTagName("typeSchema").item(0).getTextContent();
        final String recTypeName = element.getElementsByTagName("typeName").item(0).getTextContent();

        pairs.addAll(getUDT(recTypeSchema, recTypeName, concatIndex));
      }
    }

    return pairs;
  }

  private void getColumnNames(String schemaName, String tableName)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:columns/ns:column/ns:name/text()";
    xpathExpression = xpathExpression.replace("$1", schemaName);
    xpathExpression = xpathExpression.replace("$2", tableName);

    getTableFieldFromMetadataXML(schemaName, tableName, xpathExpression, tableAndColumns);
  }

  private void getForeignKeys(String schemaName, String tableName)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:foreignKeys/ns:foreignKey/ns:reference/ns:column/text()";
    xpathExpression = xpathExpression.replace("$1", schemaName);
    xpathExpression = xpathExpression.replace("$2", tableName);

    getTableFieldFromMetadataXML(schemaName, tableName, xpathExpression, foreignKeyColumns);
  }

  private void getNumberOfRows(String schemaFolder, String tableFolder)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:rows/text()";
    xpathExpression = xpathExpression.replace("$1", schemaFolder);
    xpathExpression = xpathExpression.replace("$2", tableFolder);

    NodeList result = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
      xpathExpression, XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
    for (int k = 0; k < result.getLength(); k++) {
      final String rowsValue = result.item(k).getNodeValue();
      rows.put(new SIARDContent(schemaFolder, tableFolder), Integer.parseInt(rowsValue));
    }
  }

  private void getTableFieldFromMetadataXML(String schemaName, String tableName, String xpathExpression,
    Map<SIARDContent, List<String>> map)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    NodeList result = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
      xpathExpression, XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
    List<String> genericList = new ArrayList<>();

    for (int k = 0; k < result.getLength(); k++) {
      final String genericName = result.item(k).getNodeValue();
      genericList.add(genericName);
    }
    map.put(new SIARDContent(schemaName, tableName), genericList);
  }
}
