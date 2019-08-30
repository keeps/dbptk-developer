package com.databasepreservation.modules.siard.validate.component.tableData;

import static com.databasepreservation.model.reporters.ValidationReporter.Status;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.validator.CategoryNotFoundException;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class RequirementsForTableDataValidator extends ValidatorComponentImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(RequirementsForTableDataValidator.class);

  private final String MODULE_NAME;
  private static final String P_60 = "T_6.0";
  private static final String P_601 = "T_6.0-1";
  private static final String P_602 = "T_6.0-2";

  private List<String> P_601_ERRORS = new ArrayList<>();
  private List<String> P_602_ERRORS = new ArrayList<>();

  private static List<String> SQL2008Types = null;
  private HashMap<String, Set<String>> primaryKeyData = new HashMap<>();

  public RequirementsForTableDataValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    populateSQL2008Validations();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, P_60);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(P_60, MODULE_NAME);

    if (validateSQL2008Requirements()) {
      observer.notifyValidationStep(MODULE_NAME, P_601, Status.OK);
      getValidationReporter().validationStatus(P_601, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_601, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_601, MODULE_NAME, "", "", P_601_ERRORS);
      closeZipFile();
      return false;
    }

    if (validateTableXSDAgainstXML()) {
      observer.notifyValidationStep(MODULE_NAME, P_602, Status.OK);
      getValidationReporter().validationStatus(P_602, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_602, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_602, MODULE_NAME, "", "Validation against XSD failed", P_602_ERRORS);
      closeZipFile();
      return false;
    }

    observer.notifyFinishValidationModule(MODULE_NAME, Status.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, Status.PASSED);
    closeZipFile();

    return true;
  }

  /**
   * T_6.0-1
   * 
   * All the table data (primary data) must meet the consistency requirements of
   * SQL:2008. A SIARD file that validates syntactically against the various XSDs
   * but infringes the SQL standard semantically is not compliant with this format
   * description. In particular, the table values must correspond to the
   * constraints of the SQL types in the metadata. Additionally, the primary,
   * candidate and foreign key conditions and nullability conditions stored in the
   * metadata must all be met.
   *
   * @return true if valid otherwise false
   */
  private boolean validateSQL2008Requirements() {
    if (preValidationRequirements())
      return false;

    List<String> SQL2008FoundTypes = new ArrayList<>();

    final String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:columns/ns:column";

    try {
      NodeList result = (NodeList) XMLUtils.getXPathResult(
        getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), xpathExpression, XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < result.getLength(); i++) {
        Element element = (Element) result.item(i);
        if (element.getElementsByTagName("type").item(0) != null) {
          SQL2008FoundTypes.add(element.getElementsByTagName("type").item(0).getTextContent());
        } else {
          final String typeSchema = element.getElementsByTagName("typeSchema").item(0).getTextContent();
          final String typeName = element.getElementsByTagName("typeName").item(0).getTextContent();
          SQL2008FoundTypes.addAll(getAdvancedOrUDTData(typeSchema, typeName));
        }
      }
    } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException | ModuleException e) {
      return false;
    }

    for (String type : SQL2008FoundTypes) {
      // Checks if the type is in conformity with SQL:2008
      if (!validateSQL2008Type(type)) {
        System.out.println(type);
        return false;
      }
    }

    try {
      NodeList result = (NodeList) XMLUtils.getXPathResult(
        getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:folder/text()", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < result.getLength(); i++) {
        String schemaFolder = result.item(i).getTextContent();

        NodeList tables = (NodeList) XMLUtils.getXPathResult(
          getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
          "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='" + schemaFolder
            + "']/ns:tables/ns:table/ns:folder/text()",
          XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

        for (int j = 0; j < tables.getLength(); j++) {
          String tableFolder = tables.item(j).getTextContent();
          P_601_ERRORS.addAll(validatePrimaryKeyConstraint(schemaFolder, tableFolder));
        }
      }
    } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException e) {
      return false;
    }

    try {
      NodeList result = (NodeList) XMLUtils.getXPathResult(
        getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:folder/text()", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < result.getLength(); i++) {
        String schemaFolder = result.item(i).getTextContent();

        NodeList tables = (NodeList) XMLUtils.getXPathResult(
          getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
          "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='" + schemaFolder
            + "']/ns:tables/ns:table/ns:folder/text()",
          XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

        for (int j = 0; j < tables.getLength(); j++) {
          String tableFolder = tables.item(j).getTextContent();
          P_601_ERRORS.addAll(validateForeignKeyConstraint(schemaFolder, tableFolder));
        }
      }
    } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException e) {
      return false;
    }

    return P_601_ERRORS.isEmpty();
  }

  /**
   * T_6.0-2
   *
   * The schema definition table[number].xsd must be complied with for the
   * table[number].xml file. This means that table[number].xml must be capable of
   * being positively validated against table[number].xsd.
   * 
   * @return true if valid otherwise false
   */
  private boolean validateTableXSDAgainstXML() {
    if (preValidationRequirements())
      return false;

    Set<String> tableDataSchemaDefinition = new HashSet<>();

    for (String path : getZipFileNames()) {
      String regexPattern = "^(content/schema[0-9]+/table[0-9]+/table[0-9]+)\\.(xsd|xml)$";

      Pattern pattern = Pattern.compile(regexPattern);
      Matcher matcher = pattern.matcher(path);

      while (matcher.find()) {
        tableDataSchemaDefinition.add(matcher.group(1));
      }
    }

    for (String path : tableDataSchemaDefinition) {
      String XSDPath = path.concat(Constants.XSD_EXTENSION);
      String XMLPath = path.concat(Constants.XML_EXTENSION);

      final ZipArchiveEntry XSDEntry = getZipFile().getEntry(XSDPath);
      final ZipArchiveEntry XMLEntry = getZipFile().getEntry(XMLPath);
      InputStream XSDInputStream;
      InputStream XMLInputStream;
      try {
        XSDInputStream = getZipFile().getInputStream(XSDEntry);
        XMLInputStream = getZipFile().getInputStream(XMLEntry);
      } catch (IOException e) {
        return false;
      }

      Source schemaFile = new StreamSource(XSDInputStream);
      Source xmlFile = new StreamSource(XMLInputStream);

      SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      Schema schema;
      try {
        schema = schemaFactory.newSchema(schemaFile);
        Validator validator = schema.newValidator();
        try {
          validator.validate(xmlFile);
        } catch (SAXException | IOException e) {
          P_602_ERRORS.add(e.getLocalizedMessage());
        }
      } catch (SAXException e) {
        P_602_ERRORS.add(e.getLocalizedMessage());
      }
    }

    return P_602_ERRORS.isEmpty();
  }

  /*
   * Auxiliary Methods
   */
  private List<String> getAdvancedOrUDTData(final String typeSchema, final String typeName)
    throws ModuleException, ParserConfigurationException, SAXException, XPathExpressionException, IOException {

    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:name/text()='$1']/ns:types/ns:type[ns:name/text()='$2']";
    xpathExpression = xpathExpression.replace("$1", typeSchema);
    xpathExpression = xpathExpression.replace("$2", typeName);

    NodeList result;
    result = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
      xpathExpression, XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

    List<String> types = new ArrayList<>();

    for (int i = 0; i < result.getLength(); i++) {
      Element element = (Element) result.item(i);
      final String category = element.getElementsByTagName("category").item(0).getTextContent();

      if (category.equals(Constants.DISTINCT)) {
        types.add(element.getElementsByTagName("base").item(0).getTextContent());
      } else if (category.equals(Constants.UDT)) {
        types.addAll(getUDT(typeSchema, typeName));
      } else {
        throw new CategoryNotFoundException("The category: '" + category + "' is not allowed");
      }
    }

    return types;
  }

  private List<String> getUDT(String typeSchema, String typeName)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {

    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:name/text()='$1']/ns:types/ns:type[ns:name/text()='$2']/ns:attributes/ns:attribute";
    xpathExpression = xpathExpression.replace("$1", typeSchema);
    xpathExpression = xpathExpression.replace("$2", typeName);

    List<String> types = new ArrayList<>();

    NodeList nodeList = (NodeList) XMLUtils.getXPathResult(
      getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), xpathExpression, XPathConstants.NODESET,
      Constants.NAMESPACE_FOR_METADATA);
    for (int i = 0; i < nodeList.getLength(); i++) {
      Element element = (Element) nodeList.item(i);
      if (element.getElementsByTagName("type").item(0) != null) {
        final String type = element.getElementsByTagName("type").item(0).getTextContent();
        types.add(type);
      } else {
        final String recTypeSchema = element.getElementsByTagName("typeSchema").item(0).getTextContent();
        final String recTypeName = element.getElementsByTagName("typeName").item(0).getTextContent();

        types.addAll(getUDT(recTypeSchema, recTypeName));
      }
    }

    return types;
  }

  private boolean validateSQL2008Type(String type) {
    for (String s : SQL2008Types) {
      if (type.matches(s))
        return true;
    }

    return false;
  }

  private List<String> validatePrimaryKeyConstraint(String schemaFolder, String tableFolder)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    List<String> errors = new ArrayList<>();
    final List<Integer> indexes = getPrimaryKeyColumnIndexes(schemaFolder, tableFolder);
    Set<String> uniquePrimaryKeys = new HashSet<>();

    final String path = validatorPathStrategy.getXMLTablePathFromFolder(schemaFolder, tableFolder);

    if (indexes.size() == 1) {
      final int index = indexes.get(0);
      String columnIndex = "c" + index;
      String expression = "/ns:table/ns:row/ns:$1";
      expression = expression.replace("$1", columnIndex);

        NodeList result = (NodeList) XMLUtils.getXPathResult(getZipInputStream(path), expression,
          XPathConstants.NODESET, Constants.NAMESPACE_FOR_TABLE);
        for (int i = 0; i < result.getLength(); i++) {
        if (!uniquePrimaryKeys.add(result.item(i).getTextContent())) {
          String tagName = result.item(i).getNodeName();
          String xml = "<" + tagName + ">" + result.item(i).getTextContent() + "</" + tagName + ">";

          P_601_ERRORS.add("Primary key constraint not met " + xml + " at " + path);
        }
      }
    } else if (indexes.size() > 1) {
      Set<List<String>> uniqueCompositePrimaryKeys = new HashSet<>();
      String expression = "/ns:table/ns:row";

        NodeList result = (NodeList) XMLUtils.getXPathResult(getZipInputStream(path), expression,
          XPathConstants.NODESET, Constants.NAMESPACE_FOR_TABLE);
        for (int i = 0; i < result.getLength(); i++) {
          List<String> compositePrimaryKey = new ArrayList<>();
        List<String> XMLNode = new ArrayList<>();
          for (int index : indexes) {
            String columnIndex = "c" + index;
            Element element = (Element) result.item(i);
          String tagName = element.getElementsByTagName(columnIndex).item(0).getNodeName();
          XMLNode.add("<" + tagName + ">" + element.getElementsByTagName(columnIndex).item(0).getTextContent() + "</"
            + tagName + ">");
            compositePrimaryKey.add(element.getElementsByTagName(columnIndex).item(0).getTextContent());
          }
        if (!uniqueCompositePrimaryKeys.add(compositePrimaryKey)) {
          StringBuilder sb = new StringBuilder();
          for (String s : XMLNode) {
            if (XMLNode.indexOf(s) == XMLNode.size() - 1) {
              sb.append(s);
            } else {
              sb.append(s).append(" ");
            }
          }
          errors.add("Primary key duplicated: " + sb.toString() + " at " + path);
        }
      }
    }
    return errors;
  }

  private List<Integer> getPrimaryKeyColumnIndexes(String schemaFolder, String tableFolder)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {

    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:primaryKey/ns:column/text()";
    xpathExpression = xpathExpression.replace("$1", schemaFolder);
    xpathExpression = xpathExpression.replace("$2", tableFolder);

    NodeList nodeList = (NodeList) XMLUtils.getXPathResult(
      getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), xpathExpression, XPathConstants.NODESET,
      Constants.NAMESPACE_FOR_METADATA);

    List<Integer> indexes = new ArrayList<>();

    for (int i = 0; i < nodeList.getLength(); i++) {
      final String columnName = nodeList.item(i).getTextContent();
      indexes.add(getColumnIndexByFolder(schemaFolder, tableFolder, columnName));
    }

    return indexes;
  }

  private void getPrimaryKeyData(final String schemaName, final String tableName, final String columnName)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    String key = schemaName + "." + tableName + "." + columnName;
    String path = validatorPathStrategy.getXMLTablePathFromName(schemaName, tableName);
    if (primaryKeyData.get(key) == null) {
      primaryKeyData.put(key, getColumnDataByIndex(path, getColumnIndexByName(schemaName, tableName, columnName)));
    }
  }

  private List<String> validateForeignKeyConstraint(String schemaFolder, String tableFolder)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    List<String> errors = new ArrayList<>();
    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:foreignKeys/ns:foreignKey";
    xpathExpression = xpathExpression.replace("$1", schemaFolder);
    xpathExpression = xpathExpression.replace("$2", tableFolder);

    NodeList nodeList = (NodeList) XMLUtils.getXPathResult(
      getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), xpathExpression, XPathConstants.NODESET,
      Constants.NAMESPACE_FOR_METADATA);

    for (int i = 0; i < nodeList.getLength(); i++) {
      Element element = (Element) nodeList.item(i);
      final String foreignKeyName = element.getElementsByTagName("name").item(0).getTextContent();
      final String referencedSchema = element.getElementsByTagName("referencedSchema").item(0).getTextContent();
      final String referencedTable = element.getElementsByTagName("referencedTable").item(0).getTextContent();
      Element reference = (Element) element.getElementsByTagName("reference").item(0);
      final String columnName = reference.getElementsByTagName("column").item(0).getTextContent();
      final String referenced = reference.getElementsByTagName("referenced").item(0).getTextContent();

      getPrimaryKeyData(referencedSchema, referencedTable, referenced);

      final String path = validatorPathStrategy.getXMLTablePathFromFolder(schemaFolder, tableFolder);
      final Set<String> data = getColumnDataByIndex(path,
        getColumnIndexByFolder(schemaFolder, tableFolder, columnName));
      String key = referencedSchema + "." + referencedTable + "." + referenced;
      for (String value : data) {
        if (!primaryKeyData.get(key).contains(value)) {
          errors.add("The value ("+ value + ") for foreign key '" + foreignKeyName + "' is not present in '" + referencedTable + "'");
        }
      }
    }

    return errors;
  }

  private Set<String> getColumnDataByIndex(String path, int index)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    Set<String> data = new HashSet<>();
    String columnIndex = "c" + index;
    String xpathExpression = "/ns:table/ns:row/ns:$1/text()";
    xpathExpression = xpathExpression.replace("$1", columnIndex);

    NodeList result = (NodeList) XMLUtils.getXPathResult(getZipInputStream(path), xpathExpression,
      XPathConstants.NODESET, Constants.NAMESPACE_FOR_TABLE);
    for (int i = 0; i < result.getLength(); i++) {
      data.add(result.item(i).getTextContent());
    }

    return data;
  }

  private int getColumnIndexByName(String schemaName, String tableName, String columnName)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    final String schemaFolder = validatorPathStrategy.getSchemaFolder(schemaName);
    final String tableFolder = validatorPathStrategy.getTableFolder(schemaName, tableName);
    return getColumnIndexByFolder(schemaFolder, tableFolder, columnName);
  }

  private int getColumnIndexByFolder(String schemaFolder, String tableFolder, String columnName)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:columns/ns:column/ns:name/text()";
    xpathExpression = xpathExpression.replace("$1", schemaFolder);
    xpathExpression = xpathExpression.replace("$2", tableFolder);

    NodeList nodeList = (NodeList) XMLUtils.getXPathResult(
      getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), xpathExpression, XPathConstants.NODESET,
      Constants.NAMESPACE_FOR_METADATA);

    for (int i = 0; i < nodeList.getLength(); i++) {
      final String name = nodeList.item(i).getTextContent();
      if (columnName.equals(name)) {
        return i + 1;
      }
    }

    return -1;
  }

  private void populateSQL2008Validations() {
    SQL2008Types = new ArrayList<>();
    SQL2008Types.add("^BIGINT$");
    SQL2008Types.add("^BINARY\\s+LARGE\\s+OBJECT(\\s*\\(\\s*[1-9]\\d*(\\s*(K|M|G))?\\s*\\))?$");
    SQL2008Types.add("^BLOB(\\s*\\(\\s*[1-9]\\d*(\\s*(K|M|G))?\\s*\\))?$");
    SQL2008Types.add("^BLOB");
    SQL2008Types.add("^BINARY VARYING\\(\\d+\\)$");
    SQL2008Types.add("^BINARY\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^VARBINARY(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^BOOLEAN$");
    SQL2008Types.add("^CHARACTER\\s+LARGE\\s+OBJECT(\\s*\\(\\s*[1-9]\\d*(\\s*(K|M|G))?\\s*\\))?$");
    SQL2008Types.add("^CLOB(\\s*\\(\\s*[1-9]\\d*(\\s*(K|M|G))?\\s*\\))?$");
    SQL2008Types.add("^CHARACTER\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^CHAR\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^VARCHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^CHARACTER(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^CHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^DATE$");
    SQL2008Types.add("^DECIMAL(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?$");
    SQL2008Types.add("^DEC(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?$");
    SQL2008Types.add("^DOUBLE PRECISION$");
    SQL2008Types.add("^FLOAT(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^INTEGER$");
    SQL2008Types.add("^INT$");
    SQL2008Types.add(
      "^INTERVAL\\s+(((YEAR|MONTH|DAY|HOUR|MINUTE)(\\s*\\(\\s*[1-9]\\d*\\s*\\))?(\\s+TO\\s+(MONTH|DAY|HOUR|MINUTE|SECOND)(\\s*\\(\\s*[1-9]\\d*\\s*\\))?)?)|(SECOND(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?))$");
    SQL2008Types.add("^NATIONAL\\s+CHARACTER\\s+LARGE\\s+OBJECT(\\s*\\(\\s*[1-9]\\d*(\\s*(K|M|G))?\\s*\\))?$");
    SQL2008Types.add("^NCHAR\\s+LARGE\\s+OBJECT(\\s*\\(\\s*[1-9]\\d*(\\s*(K|M|G))?\\s*\\))?$");
    SQL2008Types.add("^NCLOB(\\s*\\(\\s*[1-9]\\d*(\\s*(K|M|G))?\\s*\\))?$");
    SQL2008Types.add("^NATIONAL\\s+CHARACTER\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^NATIONAL\\s+CHAR\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^NCHAR\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^NATIONAL\\s+CHARACTER(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^NATIONAL\\s+CHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^NCHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^NUMERIC(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?$");
    SQL2008Types.add("^REAL$");
    SQL2008Types.add("^SMALLINT$");
    SQL2008Types.add("^TIME(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^TIME\\s+WITH\\s+TIME\\s+ZONE(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    SQL2008Types.add("^TIMESTAMP(\\s*\\(\\s*(0|([1-9]\\d*))\\s*\\))?$");
    SQL2008Types.add("^TIMESTAMP\\s+WITH\\s+TIME\\s+ZONE(\\s*\\(\\s*(0|([1-9]\\d*))\\s*\\))?$");
    SQL2008Types.add("^XML$");
  }
}
