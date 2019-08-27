package com.databasepreservation.modules.siard.validate.component.tableData;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter.Status;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class AdditionalChecksValidator extends ValidatorComponentImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdditionalChecksValidator.class);

  private final String MODULE_NAME;
  private HashMap<String, List<String>> tableAndColumns = new HashMap<>();
  private HashMap<String, List<String>> foreignKeyColumns = new HashMap<>();
  private HashMap<String, List<ImmutablePair<String, String>>> columnTypes = new HashMap<>();
  private HashMap<String, Integer> rows = new HashMap<>();

  private List<String> dataTypeErrors = new ArrayList<>();

  public AdditionalChecksValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
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
    if (preValidationRequirements())
      return false;

    for (Map.Entry<String, List<ImmutablePair<String, String>>> entry : columnTypes.entrySet()) {
      String path = entry.getKey().concat(Constants.XML_EXTENSION);

      for (ImmutablePair<String, String> pair : entry.getValue()) {
        String columnContent = pair.getLeft();
        String type = pair.getRight();
        String xpathExpression;
        if (columnContent.contains(".")) {
          final String[] split = columnContent.split("\\.");
          columnContent = split[0];
          String udt = "";
          for (int i = 1; i < split.length; i++) {
            String ns = "/ns:";
            udt = udt.concat(ns.concat(split[i]));
          }
          xpathExpression = "/ns:table/ns:row/ns:$1";
          xpathExpression = xpathExpression.replace("$1", columnContent);
          xpathExpression = xpathExpression.concat(udt);
        } else {
          xpathExpression = "/ns:table/ns:row/ns:$1";
          xpathExpression = xpathExpression.replace("$1", columnContent);
        }

        try {
          NodeList result = (NodeList) XMLUtils.getXPathResult(getZipInputStream(path), xpathExpression,
            XPathConstants.NODESET, Constants.NAMESPACE_FOR_TABLE);
          for (int i = 0; i < result.getLength(); i++) {
            final int size = result.item(i).getChildNodes().getLength();
            if (size > 1) {
              for (int j = 0; j < result.item(i).getChildNodes().getLength(); j++) {
                final String nodeName = result.item(i).getChildNodes().item(j).getNodeName();
                if (nodeName.matches("a[0-9]+")) {
                  final String content = result.item(i).getChildNodes().item(j).getTextContent();
                  if (!validateType(content, type)) {
                    dataTypeErrors.add(content + " is not in conformity with '" + type + "' type");
                  }
                }
              }
            } else {
              final String content = result.item(i).getTextContent();
              if (!validateType(content, type)) {
                dataTypeErrors.add(content + " is not in conformity with '" + type + "' type");
              }
            }
          }
        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          return false;
        }
      }
    }

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
    for (Map.Entry<String, List<String>> entry : foreignKeyColumns.entrySet()) {
      String key = entry.getKey();
      int numberOfNulls;

      for (String column : entry.getValue()) {
        final int indexOf = tableAndColumns.get(key).indexOf(column) + 1;

        String path = key.concat(Constants.XML_EXTENSION);
        String xpathExpression = "count(/ns:table/ns:row/ns:c" + indexOf + ")";
        try {
          String value = (String) XMLUtils.getXPathResult(getZipInputStream(path), xpathExpression,
            XPathConstants.STRING, Constants.NAMESPACE_FOR_TABLE);
          int numberOfRows = Integer.parseInt(value);

          final Integer metadataXMLNumberOfRows = rows.get(key);
          if (numberOfRows != metadataXMLNumberOfRows) {
            numberOfNulls = metadataXMLNumberOfRows - numberOfRows;
            getValidationReporter().notice(numberOfNulls, "Number of null values for " + column);
          }

        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          LOGGER.error(e.getLocalizedMessage());
          return;
        }
      }
    }
  }

  /*
   * Auxiliary Methods
   */
  private boolean validateType(String content, String type) {

    /**
     * BINARY LARGE OBJECT(...), BLOB(...)
     * CHARACTER LARGE OBJECT(...), CLOB(...)
     * INTERVAL <start> [TO <end>]
     * NATIONAL CHARACTER LARGE OBJECT(...),
     * NCHAR LARGE OBJECT(...), NCLOB(...)
     * XML
     */

    final String integerRegex = "^INTEGER$";
    final String smallIntRegex = "^SMALLINT$";
    final String decimalRegex = "^DECIMAL\\((\\d+)(\\s*,\\s*(\\d+))?\\)$";
    final String numericRegex = "^NUMERIC\\((\\d+)(\\s*,\\s*(\\d+))?\\)$";
    final String booleanRegex = "^BOOLEAN$";
    final String realRegex = "^REAL$";
    final String floatRegex = "^FLOAT\\((\\d+)\\)$";
    final String bigIntRegex = "^BIGINT$";
    final String doublePrecisionRegex = "^DOUBLE PRECISION$";
    final String dateRegex = "^DATE$";
    final String timeRegex = "^TIME$";
    final String timestampRegex = "^TIMESTAMP$";
    final String stringRegex = "^CHARACTER VARYING\\(\\d+\\)$|" +
        "^CHAR VARYING\\(\\d+\\)$|" +
        "^VARCHAR\\(\\d+\\)$|" +
        "^CHARACTER\\(\\d+\\)$|" +
        "^CHAR\\(\\d+\\)$|" +
        "^NATIONAL CHARACTER VARYING\\(\\d+\\)$|" +
        "^NATIONAL CHAR VARYING\\(\\d+\\)$|" +
        "^NCHAR VARYING\\(\\d+\\)$|" +
        "^NATIONAL CHARACTER\\(\\d+\\)$|" +
        "^NATIONAL CHAR\\(\\d+\\)$|" +
        "^NCHAR\\(\\d+\\)$";
    final String binaryRegex = "^BINARY VARYING\\(\\d+\\)$|" +
        "^VARBINARY\\(\\d+\\)$|" +
        "^BINARY\\(\\d+\\)$";

    try {
      if (type.matches(integerRegex))
        Integer.parseInt(content);

      if (type.matches(smallIntRegex))
        Short.parseShort(content);

      if (type.matches(decimalRegex)) {
        return checkDecimalNumericDataType(content, type, decimalRegex);
      }

      if (type.matches(numericRegex)) {
        return checkDecimalNumericDataType(content, type, numericRegex);
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
        final int size = getDataTypeLength(type);

        if (size != -1) {
          if (content.length() > size) {
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

  private boolean checkDecimalNumericDataType(String content, String type, String regex) {
    final BigDecimal bigDecimal = new BigDecimal(content);
    final Pattern compile = Pattern.compile(regex);
    Matcher matcher = compile.matcher(type);
    int typePrecision = -1, typeScale = -1;

    while (matcher.find()) {
      typePrecision = Integer.parseInt(matcher.group(1));
      try {
        typeScale = Integer.parseInt(matcher.group(3));
      } catch (NumberFormatException e) {
        typeScale = 0;
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
      final String schemaName = result.item(i).getNodeValue();
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table/ns:folder/text()";
      xpathExpression = xpathExpression.replace("$1", schemaName);
      try {
        NodeList tables = (NodeList) XMLUtils.getXPathResult(
          getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), xpathExpression, XPathConstants.NODESET,
          Constants.NAMESPACE_FOR_METADATA);

        for (int j = 0; j < tables.getLength(); j++) {
          final String tableName = tables.item(j).getNodeValue();
          getColumnNames(schemaName, tableName);
          getForeignKeys(schemaName, tableName);
          getNumberOfRows(schemaName, tableName);
          getColumnType(schemaName, tableName);
        }
      } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
        LOGGER.error(e.getLocalizedMessage());
        return false;
      }
    }

    return true;
  }

  private void getColumnType(String schemaName, String tableName)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:columns/ns:column/ns:name/text()";
    xpathExpression = xpathExpression.replace("$1", schemaName);
    xpathExpression = xpathExpression.replace("$2", tableName);

    NodeList columns = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
      xpathExpression, XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

    final String key = "content/" + schemaName + "/" + tableName + "/" + tableName;
    List<ImmutablePair<String, String>> pairs = new ArrayList<>();
    for (int j = 0; j < columns.getLength(); j++) {
      String columnName = columns.item(j).getNodeValue();
      xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:columns/ns:column[ns:name/text()='$3']/ns:type/text()";
      xpathExpression = xpathExpression.replace("$1", schemaName);
      xpathExpression = xpathExpression.replace("$2", tableName);
      xpathExpression = xpathExpression.replace("$3", columnName);

      String type = (String) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        xpathExpression, XPathConstants.STRING, Constants.NAMESPACE_FOR_METADATA);
      ImmutablePair<String, String> pair;
      String index = "c" + (j + 1);
      if (StringUtils.isBlank(type)) {
        xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:columns/ns:column[ns:name/text()='$3']";
        xpathExpression = xpathExpression.replace("$1", schemaName);
        xpathExpression = xpathExpression.replace("$2", tableName);
        xpathExpression = xpathExpression.replace("$3", columnName);
        pairs.addAll(getAdvancedOrUDTData(xpathExpression, index));
      } else {
        pair = new ImmutablePair<>(index, type);
        pairs.add(pair);
      }
    }

    columnTypes.put(key, pairs);
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

  private void getNumberOfRows(String schemaName, String tableName)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:rows/text()";
    xpathExpression = xpathExpression.replace("$1", schemaName);
    xpathExpression = xpathExpression.replace("$2", tableName);

    NodeList result = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
      xpathExpression, XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
    final String key = "content/" + schemaName + "/" + tableName + "/" + tableName;
    for (int k = 0; k < result.getLength(); k++) {
      final String rowsValue = result.item(k).getNodeValue();
      rows.put(key, Integer.parseInt(rowsValue));
    }
  }

  private void getTableFieldFromMetadataXML(String schemaName, String tableName, String xpathExpression,
    HashMap<String, List<String>> map)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    NodeList result = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
      xpathExpression, XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
    List<String> genericList = new ArrayList<>();
    final String key = "content/" + schemaName + "/" + tableName + "/" + tableName;
    for (int k = 0; k < result.getLength(); k++) {
      final String genericName = result.item(k).getNodeValue();
      genericList.add(genericName);
    }
    map.put(key, genericList);
  }
}
