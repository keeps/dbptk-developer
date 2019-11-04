/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component.tableData;

import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.validator.CategoryNotFoundException;
import com.databasepreservation.model.exception.validator.XMLFileNotFoundException;
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.model.validator.SIARDContent;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;
import com.databasepreservation.modules.siard.validate.handlers.CompositePrimaryKeyValidationHandler;
import com.databasepreservation.modules.siard.validate.handlers.PrimaryKeyValidationHandler;
import com.databasepreservation.modules.siard.validate.handlers.TableContentHandler;
import com.databasepreservation.utils.ConfigUtils;
import com.databasepreservation.utils.ListUtils;
import com.databasepreservation.utils.XMLUtils;
import com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class RequirementsForTableDataValidator extends ValidatorComponentImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(RequirementsForTableDataValidator.class);
  // SAXHandler settings
  private static final String JAXP_SCHEMA_LANGUAGE = "http://java.sun.com/xml/jaxp/properties/schemaLanguage";
  private static final String W3C_XML_SCHEMA = "http://www.w3.org/2001/XMLSchema";
  private static final String JAXP_SCHEMA_SOURCE = "http://java.sun.com/xml/jaxp/properties/schemaSource";

  // MapDB configurations
  private static final String FILE_DIRECTORY_LOCATION = ConfigUtils.getProperty(Constants.PROPERTY_UNSET,
    "dbptk.memory.dir");
  private static final String MAPDB_FILENAME = "mapdb.bin";

  private final String MODULE_NAME;
  private final XMLInputFactory factory;
  private final SAXParserFactory saxParserFactory;
  private SAXParser saxParser;
  private static final String T_60 = "T_6.0";
  private static final String T_601 = "T_6.0-1";
  private static final String T_602 = "T_6.0-2";
  private static final String A_T_6011 = "A_T_6.0-1-1";
  private static final String A_T_6012 = "A_T_6.0-1-2";
  private static final String HASH_SET_PREFIX = "hashSet";
  private static final String COLUMN_XML_TAG = "c";
  private static final String MSG_CHECK_LOG = "Please check the log file for more information";
  private static final String MSG_FAILED_OPEN_STREAM = "Failed to open stream";

  private boolean A_T_6012_ERRORS = false;

  private List<String> SQL2008Types = new ArrayList<>();
  private HashMap<String, Set<String>> primaryKeyData = new HashMap<>();
  private TreeMap<SIARDContent, HashMap<String, String>> columnTypes = new TreeMap<>();
  private TreeMap<SIARDContent, List<String>> foreignKeyColumns = new TreeMap<>();
  private TreeMap<SIARDContent, Integer> rows = new TreeMap<>();
  private final DB db;

  private Pattern patternBigIntRegex;
  private Pattern patternIntegerRegex;
  private Pattern patternSmallIntRegex;
  private Pattern patternDecimalRegex;
  private Pattern patternRealRegex;
  private Pattern patternFloatRegex;
  private Pattern patternDoublePrecisionRegex;
  private Pattern patternBinaryRegex;
  private Pattern patternVarcharRegex;
  private Pattern patternStringRegex;
  private Pattern patternNCharRegex;
  private Pattern patternBooleanRegex;
  private Pattern patternDateRegex;
  private Pattern patternTimeRegex;
  private Pattern patternTimestampRegex;
  private Pattern patternSizeRegex;
  private Pattern patternDecimalRegexWithDataType;
  private Pattern patternDateContentRegex;
  private Pattern patternTimeContentRegex;
  private Pattern patternTimestampContentRegex;

  public RequirementsForTableDataValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    populateSQL2008Validations();
    factory = XMLInputFactory.newInstance();
    saxParserFactory = SAXParserFactory.newInstance(SAXParserFactoryImpl.class.getName(), SAXParserFactoryImpl.class.getClassLoader());
    saxParserFactory.setValidating(true);
    saxParserFactory.setNamespaceAware(true);
    preCompileRegexPatterns();
    db = setupMapDB();
  }

  private DB setupMapDB() {
    Path fileDBPath;
    if (FILE_DIRECTORY_LOCATION.equals(Constants.PROPERTY_UNSET)) {
      fileDBPath = Paths.get(ConfigUtils.getMapDBHomeDirectory().normalize().toAbsolutePath().toString(),
        MAPDB_FILENAME);
    } else {
      fileDBPath = Paths.get(FILE_DIRECTORY_LOCATION, MAPDB_FILENAME);
    }
    return DBMaker.fileDB(fileDBPath.toFile()).fileDeleteAfterClose().fileMmapEnable().fileMmapEnableIfSupported()
      .fileMmapPreclearDisable().closeOnJvmShutdown().make();
  }

  @Override
  public void clean() {
    if (SQL2008Types != null) {
      SQL2008Types.clear();
    }
    primaryKeyData.clear();
    primaryKeyData = null;
    columnTypes.clear();
    columnTypes = null;
    foreignKeyColumns.clear();
    foreignKeyColumns = null;
    rows = null;
    zipFileManagerStrategy.closeZipFile();
    db.close();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, T_60);

    if (validateTableXSDAgainstXML()) {
      observer.notifyValidationStep(MODULE_NAME, T_602, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(T_602, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, T_602, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporterStatus.FAILED);
      return false;
    }

    try {
      obtainValidationData();
    } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
      LOGGER.debug("Failed to obtain data for {}", MODULE_NAME, e);
      getValidationReporter().validationStatus(T_60, ValidationReporterStatus.ERROR,
        "Failed to obtain data for " + MODULE_NAME, MSG_CHECK_LOG);
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporterStatus.FAILED);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(T_60, MODULE_NAME);

    if (validateSQL2008Requirements()) {
      observer.notifyValidationStep(MODULE_NAME, T_601, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(T_601, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, T_601, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporterStatus.FAILED);
      return false;
    }

    numberOfNullValuesForForeignKey();
    observer.notifyValidationStep(MODULE_NAME, A_T_6011, ValidationReporterStatus.OK);
    getValidationReporter().validationStatus(A_T_6011, ValidationReporterStatus.OK);

    if (!validateTableDataType()) {
      observer.notifyValidationStep(MODULE_NAME, A_T_6012, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(A_T_6012, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, A_T_6012, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(A_T_6012, MODULE_NAME);
      return false;
    }

    observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporterStatus.PASSED);

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
    boolean valid = true;
    boolean primaryKeyValid = true;
    boolean foreignKeyValid = true;

    List<String> SQL2008FoundTypes = new ArrayList<>();

    final String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:columns/ns:column";

    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      NodeList result = (NodeList) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.NODESET,
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
      LOGGER.debug("Failed to validate {} ({})", MODULE_NAME, T_601, e);
      getValidationReporter().validationStatus(T_601, ValidationReporterStatus.ERROR,
        "Failed to validate due to an exception on " + MODULE_NAME, MSG_CHECK_LOG);
      return false;
    }

    for (String type : SQL2008FoundTypes) {
      // Checks if the type is in conformity with SQL:2008
      if (!validateSQL2008Type(type)) {
        getValidationReporter().validationStatus(T_601, ValidationReporterStatus.ERROR,
          "All the table data (primary data) must meet the consistency requirements of SQL:2008.",
          "The " + type + " type is not in conformity with SQL:2008 data types");
        valid = false;
      }
    }

    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      NodeList result = (NodeList) XMLUtils.getXPathResult(is, "/ns:siardArchive/ns:schemas/ns:schema/ns:folder/text()",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      observer.notifyMessage(MODULE_NAME, T_601, "Validating Primary Keys", ValidationReporterStatus.START);
      for (int i = 0; i < result.getLength(); i++) {
        String schemaFolder = result.item(i).getTextContent();

        try (InputStream tableInputStream = zipFileManagerStrategy.getZipInputStream(path,
          validatorPathStrategy.getMetadataXMLPath())) {
          NodeList tables = (NodeList) XMLUtils.getXPathResult(tableInputStream,
            "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='" + schemaFolder
              + "']/ns:tables/ns:table/ns:folder/text()",
            XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

          for (int j = 0; j < tables.getLength(); j++) {
            String tableFolder = tables.item(j).getTextContent();
            boolean r = validatePrimaryKeyConstraint(schemaFolder, tableFolder);
            if (!r)
              primaryKeyValid = false;
          }
        }
        observer.notifyMessage(MODULE_NAME, T_601, "Validating Primary Keys", ValidationReporterStatus.FINISH);
      }
    } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException
      | XMLFileNotFoundException e) {
      LOGGER.debug("Failed to validate {}({})", MODULE_NAME, T_601, e);
      getValidationReporter().validationStatus(T_601, ValidationReporterStatus.ERROR,
        "Failed to validate due to an exception on " + MODULE_NAME, MSG_CHECK_LOG);
      return false;
    }

    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      NodeList result = (NodeList) XMLUtils.getXPathResult(is, "/ns:siardArchive/ns:schemas/ns:schema/ns:folder/text()",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      observer.notifyMessage(MODULE_NAME, T_601, "Validating Foreign Keys", ValidationReporterStatus.START);
      for (int i = 0; i < result.getLength(); i++) {
        String schemaFolder = result.item(i).getTextContent();
        try (InputStream tableInputStream = zipFileManagerStrategy.getZipInputStream(path,
          validatorPathStrategy.getMetadataXMLPath())) {
          NodeList tables = (NodeList) XMLUtils.getXPathResult(tableInputStream,
            "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='" + schemaFolder
              + "']/ns:tables/ns:table/ns:folder/text()",
            XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

          for (int j = 0; j < tables.getLength(); j++) {
            String tableFolder = tables.item(j).getTextContent();
            boolean r = validateForeignKeyConstraint(schemaFolder, tableFolder);
            if (!r)
              foreignKeyValid = false;
          }
        }
      }
      observer.notifyMessage(MODULE_NAME, T_601, "Validating Foreign Keys", ValidationReporterStatus.FINISH);
    } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException e) {
      LOGGER.debug("Failed to validate {}({})", MODULE_NAME, T_601, e);
      getValidationReporter().validationStatus(T_601, ValidationReporterStatus.ERROR,
        "Failed to validate due to an exception on " + MODULE_NAME, MSG_CHECK_LOG);
      return false;
    }

    return valid && primaryKeyValid && foreignKeyValid;
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
    boolean valid = true;

    Set<SIARDContent> tableDataSchemaDefinition = new TreeSet<>();
    final List<String> zipArchiveEntriesPath = zipFileManagerStrategy.getZipArchiveEntriesPath(path);
    for (String path : zipArchiveEntriesPath) {
      String regexPattern = "^(content/(schema[0-9]+)/(table[0-9]+)/table[0-9]+)\\.(xsd|xml)$";

      Pattern pattern = Pattern.compile(regexPattern);
      Matcher matcher = pattern.matcher(path);

      while (matcher.find()) {
        tableDataSchemaDefinition.add(new SIARDContent(matcher.group(2), matcher.group(3)));
      }
    }
    observer.notifyMessage(MODULE_NAME, T_602, "Validating XML against XSD", ValidationReporterStatus.START);

    for (SIARDContent content : tableDataSchemaDefinition) {
      String XSDPath = validatorPathStrategy.getXSDTablePathFromFolder(content.getSchema(), content.getTable());
      String XMLPath = validatorPathStrategy.getXMLTablePathFromFolder(content.getSchema(), content.getTable());
      observer.notifyElementValidating(T_602, XMLPath);
      try (InputStream xsd = zipFileManagerStrategy.getZipInputStream(path, XSDPath);
        InputStream xml = zipFileManagerStrategy.getZipInputStream(path, XMLPath)) {
        Source schemaFile = new StreamSource(xsd);
        Source xmlFile = new StreamSource(xml);

        SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema schema;
        try {
          schema = schemaFactory.newSchema(schemaFile);
          Validator validator = schema.newValidator();
          try {
            validator.validate(xmlFile);
          } catch (SAXException e) {
            valid = false;
            getValidationReporter().validationStatus(T_602, ValidationReporterStatus.ERROR,
              "Validation against XSD failed.",
              e.getLocalizedMessage() + " At line " + ((SAXParseException) e).getLineNumber() + ", column "
                + ((SAXParseException) e).getColumnNumber() + " on " + XMLPath);
          } catch (IOException e) {
            LOGGER.debug(MSG_FAILED_OPEN_STREAM, e);
            valid = false;
            LOGGER.debug(MSG_FAILED_OPEN_STREAM, e);
            getValidationReporter().validationStatus(T_602, ValidationReporterStatus.ERROR,
              "Validation against XSD failed." + " on " + XMLPath, MSG_CHECK_LOG);
          }
        } catch (SAXException e) {
          valid = false;
          getValidationReporter().validationStatus(T_602, ValidationReporterStatus.ERROR,
            "Validation against XSD failed.",
            e.getLocalizedMessage() + " At line " + ((SAXParseException) e).getLineNumber() + ", column "
              + ((SAXParseException) e).getColumnNumber() + " on " + XMLPath);
        }
      } catch (IOException e) {
        valid = false;
        LOGGER.debug(MSG_FAILED_OPEN_STREAM, e);
        getValidationReporter().validationStatus(T_602, ValidationReporterStatus.ERROR,
          "Validation against XSD failed." + " on " + XMLPath);
      }
    }
    observer.notifyMessage(MODULE_NAME, T_602, "Validating XML against XSD", ValidationReporterStatus.FINISH);
    return valid;
  }

  /*
   * Additional Checks
   */

  /**
   * if <type> is integer then all content should be integer, same with decimal,
   * date, boolean etc.
   *
   * @return true if valid otherwise false
   */
  private boolean validateTableDataType() {
    observer.notifyMessage(MODULE_NAME, A_T_6012, "Validating data type", ValidationReporterStatus.START);

    for (final Map.Entry<SIARDContent, HashMap<String, String>> entry : columnTypes.entrySet()) {
      final String path = validatorPathStrategy.getXMLTablePathFromFolder(entry.getKey().getSchema(),
        entry.getKey().getTable());
      observer.notifyElementValidating(A_T_6012, path);
      try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path, path)) {
        boolean rowTag = false;
        String columnElement = "";
        StringBuilder content = new StringBuilder();
        List<String> toClose = new ArrayList<>();

        final XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(is);
        while (xmlStreamReader.hasNext()) {
          xmlStreamReader.next();

          if (xmlStreamReader.getEventType() == START_ELEMENT) {
            if (xmlStreamReader.getLocalName().equals("row")) {
              rowTag = true;
            }
          }

          if (rowTag) {
            if (xmlStreamReader.getEventType() == START_ELEMENT) {
              if (!xmlStreamReader.getLocalName().equals("row")) {
                if (xmlStreamReader.getLocalName().startsWith("u")) {
                  toClose.add(xmlStreamReader.getLocalName());
                } else if (xmlStreamReader.getLocalName().startsWith("a")) {
                  toClose.add(xmlStreamReader.getLocalName());
                } else {
                  columnElement = xmlStreamReader.getLocalName();
                }
              }
            }

            if (xmlStreamReader.getEventType() == CHARACTERS) {
              if (!xmlStreamReader.getText().trim().equals("")) {
                content.append(xmlStreamReader.getText().trim());
              }
            }

            if (xmlStreamReader.getEventType() == END_ELEMENT) {
              if (toClose.contains(xmlStreamReader.getLocalName())) {
                String key = columnElement + "."
                  + ListUtils.convertListToStringWithSeparator(toClose, Constants.FILE_EXTENSION_SEPARATOR);
                if (StringUtils.isNotBlank(content.toString())) {
                  reportDataTypeValidation(content.toString(), entry.getValue().get(key), path);
                }
                final int lastOccurrence = toClose.lastIndexOf(xmlStreamReader.getLocalName());
                toClose.remove(lastOccurrence);
                content = new StringBuilder();
              }

              if (xmlStreamReader.getLocalName().equals(columnElement)) {
                reportDataTypeValidation(content.toString(), entry.getValue().get(columnElement), path);
                content = new StringBuilder();
                columnElement = "";
              }

              if (xmlStreamReader.getLocalName().equals("row")) { // When the row end validate the data content
                rowTag = false;
              }
            }
          }
        }
      } catch (XMLStreamException | IOException e) {
        LOGGER.debug("Failed to validate {}", MODULE_NAME, e);
        getValidationReporter().validationStatus(A_T_6012, ValidationReporterStatus.ERROR,
          "Failed to validate due to an exception on " + MODULE_NAME, MSG_CHECK_LOG);
        return false;
      }
    }

    observer.notifyMessage(MODULE_NAME, A_T_6012, "Validating data type", ValidationReporterStatus.FINISH);
    return A_T_6012_ERRORS;
  }

  /**
   * Count the number of null values for each foreign key
   *
   */
  private void numberOfNullValuesForForeignKey() {
    observer.notifyMessage(MODULE_NAME, A_T_6011, "Validating foreign key null values", ValidationReporterStatus.START);
    HashMap<SIARDContent, HashMap<String, Integer>> countColumnsMap = new HashMap<>();

    for (Map.Entry<SIARDContent, List<String>> entry : foreignKeyColumns.entrySet()) {
      String path = validatorPathStrategy.getXMLTablePathFromFolder(entry.getKey().getSchema(),
        entry.getKey().getTable());
      observer.notifyElementValidating(A_T_6011, path);

      if (entry.getValue().isEmpty())
        continue;

      try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path, path)) {
        XMLStreamReader streamReader = factory.createXMLStreamReader(is);
        while (streamReader.hasNext()) {
          streamReader.next();
          if (streamReader.getEventType() == START_ELEMENT) {
            final String tagName = streamReader.getLocalName();
            if (!tagName.equals("row") && !tagName.equals("table")) {
              if (countColumnsMap.get(entry.getKey()) != null) {
                updateCounter(countColumnsMap.get(entry.getKey()), tagName);
              } else {
                final HashMap<String, Integer> map = new HashMap<>();
                updateCounter(map, tagName);
                countColumnsMap.put(entry.getKey(), map);
              }
            }
          }
        }

      } catch (XMLStreamException | IOException e) {
        LOGGER.debug("Failed to validate {}[{}]", MODULE_NAME, A_T_6011, e);
        getValidationReporter().validationStatus(A_T_6011, ValidationReporterStatus.ERROR,
          "Failed to validate due to an exception on  " + MODULE_NAME,
          "Please check the log file for more information");
        return;
      }
    }

    for (Map.Entry<SIARDContent, List<String>> entry : foreignKeyColumns.entrySet()) {
      for (String columnIndex : entry.getValue()) {
        int numberOfNulls;

        final int metadataXMLNumberOfRows = rows.get(entry.getKey());
        final int numberOfRows;
        if (countColumnsMap.get(entry.getKey()) != null) {
          if (countColumnsMap.get(entry.getKey()).get(columnIndex) != null) {
            numberOfRows = countColumnsMap.get(entry.getKey()).get(columnIndex);
          } else {
            numberOfRows = 0;
          }
        } else {
          numberOfRows = 0;
        }

        if (numberOfRows != metadataXMLNumberOfRows) {
          numberOfNulls = metadataXMLNumberOfRows - numberOfRows;
          getValidationReporter().notice(A_T_6011, numberOfNulls,
            "Number of missing values for foreign key " + columnIndex + " in "
              + validatorPathStrategy.getXMLTablePathFromFolder(entry.getKey().getSchema(), entry.getKey().getTable()));
        }
      }
    }

    observer.notifyMessage(MODULE_NAME, A_T_6011, "Validating foreign key null values",
      ValidationReporterStatus.FINISH);
  }

  /*
   * Auxiliary Methods
   */
  private void reportDataTypeValidation(String content, String dataType, String archivePath) {
    if (dataType != null) {
      if (!validateType(content, dataType)) {
        A_T_6012_ERRORS = true;
        String message = content + " is not in conformity with '" + dataType + "' type in " + archivePath;
        getValidationReporter().validationStatus(A_T_6012, ValidationReporterStatus.ERROR, "Data type invalid",
          message);
      }
    }
  }

  private void updateCounter(HashMap<String, Integer> countColumnsMap, String columnIndex) {
    if (countColumnsMap.get(columnIndex) != null) {
      Integer integer = countColumnsMap.get(columnIndex);
      countColumnsMap.put(columnIndex, ++integer);
    } else {
      countColumnsMap.put(columnIndex, 1);
    }
  }

  private void obtainValidationData()
    throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
    NodeList result;
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path,
      validatorPathStrategy.getMetadataXMLPath())) {
      result = (NodeList) XMLUtils.getXPathResult(is, "/ns:siardArchive/ns:schemas/ns:schema/ns:folder/text()",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
    }

    for (int i = 0; i < result.getLength(); i++) {
      final String schemaFolder = result.item(i).getNodeValue();
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table/ns:folder/text()";
      xpathExpression = xpathExpression.replace("$1", schemaFolder);
      try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path,
        validatorPathStrategy.getMetadataXMLPath())) {
        NodeList tables = (NodeList) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.NODESET,
          Constants.NAMESPACE_FOR_METADATA);

        for (int j = 0; j < tables.getLength(); j++) {
          final String tableFolder = tables.item(j).getNodeValue();
          getColumnType(schemaFolder, tableFolder);
          getNumberOfRows(schemaFolder, tableFolder);

        }
      }
    }
  }

  private void getColumnIndexFromMetadataXML(String schemaName, String tableName, String columnName,
    Map<SIARDContent, List<String>> map)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:columns/ns:column/ns:name/text()";
    xpathExpression = xpathExpression.replace("$1", schemaName);
    xpathExpression = xpathExpression.replace("$2", tableName);

    try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path,
      validatorPathStrategy.getMetadataXMLPath())) {

      NodeList result = (NodeList) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);
      List<String> genericList = new ArrayList<>();

      for (int k = 0; k < result.getLength(); k++) {
        final String genericName = result.item(k).getNodeValue();
        if (columnName.equals(genericName)) {
          int index = k + 1;
          final String columnIndex = "c" + index;
          genericList.add(columnIndex);
        }
      }

      final SIARDContent content = new SIARDContent(schemaName, tableName);

      if (map.get(content) != null) {
        final List<String> strings = map.get(content);
        strings.addAll(genericList);
        map.put(content, strings);
      } else {
        map.put(content, genericList);
      }
    }
  }

  private void getNumberOfRows(String schemaFolder, String tableFolder)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:rows/text()";
    xpathExpression = xpathExpression.replace("$1", schemaFolder);
    xpathExpression = xpathExpression.replace("$2", tableFolder);
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      NodeList result = (NodeList) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);
      for (int k = 0; k < result.getLength(); k++) {
        final String rowsValue = result.item(k).getNodeValue();
        rows.put(new SIARDContent(schemaFolder, tableFolder), Integer.parseInt(rowsValue));
      }
    }
  }

  private boolean validateType(String content, String type) {
    try {
      if (patternStringRegex.matcher(type).matches() || patternVarcharRegex.matcher(type).matches()
        || patternNCharRegex.matcher(type).matches()) {
        String decodeString = XMLUtils.decode(content);
        int size = getDataTypeLength(type);
        if (size != -1) {
          if (decodeString.length() > size) {
            return false;
          }
        }
        decodeString = null;
      } else if (patternBinaryRegex.matcher(type).matches()) {
        byte[] bytes = DatatypeConverter.parseHexBinary(content);
        final int size = getDataTypeLength(type);
        if (size != -1) {
          if (bytes.length > size) {
            bytes = null;
            return false;
          }
        }
        bytes = null;
      } else if (patternIntegerRegex.matcher(type).matches()) {
        Integer.parseInt(content);
      } else if (patternSmallIntRegex.matcher(type).matches()) {
        Short.parseShort(content);
      } else if (patternDecimalRegex.matcher(type).matches()) {
        return checkDecimalNumericDataType(content, type);
      } else if (patternBooleanRegex.matcher(type).matches()) {
        return Boolean.FALSE.toString().equals(content) || Boolean.TRUE.toString().equals(content);
      } else if (patternRealRegex.matcher(type).matches()) {
        Float.parseFloat(content);
      } else if (patternFloatRegex.matcher(type).matches()) {
        Double.parseDouble(content);
      } else if (patternBigIntRegex.matcher(type).matches()) {
        Long.parseLong(content);
      } else if (patternDoublePrecisionRegex.matcher(type).matches()) {
        Double.parseDouble(content);
      } else if (patternDateRegex.matcher(type).matches()) {
        return patternDateContentRegex.matcher(content).matches();
      } else if (patternTimeRegex.matcher(type).matches()) {
        return patternTimeContentRegex.matcher(content).matches();
      } else if (patternTimestampRegex.matcher(type).matches()) {
        return patternTimestampContentRegex.matcher(content).matches();
      }
    } catch (NumberFormatException e) {
      return false;
    }
    return true;
  }

  private int getDataTypeLength(String type) {
    Matcher matcher = patternSizeRegex.matcher(type);

    int size = -1;

    while (matcher.find()) {
      size = Integer.parseInt(matcher.group(0));
    }

    return size;
  }

  private boolean checkDecimalNumericDataType(String content, String type) {
    BigDecimal bigDecimal = new BigDecimal(content);
    Matcher matcher = patternDecimalRegexWithDataType.matcher(type);

    String dataTypeName = null;
    String precisionAndScale = null;
    int typePrecision;
    int typeScale;

    while (matcher.find()) {
      dataTypeName = matcher.group(1);
      precisionAndScale = matcher.group(2);
    }

    if (dataTypeName == null) {
      dataTypeName = "null";
    }

    if (precisionAndScale != null) {
      precisionAndScale = precisionAndScale.substring(1, precisionAndScale.length() - 1);
      precisionAndScale = StringUtils.deleteWhitespace(precisionAndScale);

      if (precisionAndScale.contains(",")) {
        String[] split = StringUtils.split(precisionAndScale, ",");
        typePrecision = Integer.parseInt(split[0]);
        typeScale = Integer.parseInt(split[1]);
        split = null;
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

  private List<String> getAdvancedOrUDTData(final String typeSchema, final String typeName)
    throws ModuleException, ParserConfigurationException, SAXException, XPathExpressionException, IOException {

    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:name/text()='$1']/ns:types/ns:type[ns:name/text()='$2']";
    xpathExpression = xpathExpression.replace("$1", typeSchema);
    xpathExpression = xpathExpression.replace("$2", typeName);

    try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path,
      validatorPathStrategy.getMetadataXMLPath())) {
      NodeList result = (NodeList) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

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
  }

  private List<String> getUDT(String typeSchema, String typeName)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {

    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:name/text()='$1']/ns:types/ns:type[ns:name/text()='$2']/ns:attributes/ns:attribute";
    xpathExpression = xpathExpression.replace("$1", typeSchema);
    xpathExpression = xpathExpression.replace("$2", typeName);

    List<String> types = new ArrayList<>();
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path,
      validatorPathStrategy.getMetadataXMLPath())) {
      NodeList nodeList = (NodeList) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.NODESET,
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
  }

  private boolean validateSQL2008Type(String type) {
    for (String s : SQL2008Types) {
      if (type.matches(s))
        return true;
    }

    return false;
  }

  private boolean validatePrimaryKeyConstraint(String schemaFolder, String tableFolder)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException, XMLFileNotFoundException {
    boolean valid = true;
    final List<String> indexes = getPrimaryKeyColumnIndexes(schemaFolder, tableFolder);
    LOGGER.debug("Starting validating schema: {} table: {}", schemaFolder, tableFolder);

    final String tablePathFromFolder = validatorPathStrategy.getXMLTablePathFromFolder(schemaFolder, tableFolder);
    observer.notifyElementValidating(T_601, tablePathFromFolder);
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path, tablePathFromFolder)) {
      if (is == null) {
        throw new XMLFileNotFoundException("Missing XML file " + tablePathFromFolder);
      } else {
        if (indexes.size() == 1) {
          Map<String, String> map = db
            .hashMap("uniqPrimaryKeys" + schemaFolder + tableFolder, Serializer.STRING, Serializer.STRING)
            .createOrOpen();
          PrimaryKeyValidationHandler handler = new PrimaryKeyValidationHandler(tablePathFromFolder, indexes.get(0),
            T_601, map, getValidationReporter());

          final String xsdTablePathFromFolder = validatorPathStrategy.getXSDTablePathFromFolder(schemaFolder,
            tableFolder);
          final InputStream zipInputStream = zipFileManagerStrategy.getZipInputStream(this.path,
            xsdTablePathFromFolder);

          saxParser = saxParserFactory.newSAXParser();
          saxParser.setProperty(JAXP_SCHEMA_LANGUAGE, W3C_XML_SCHEMA);
          saxParser.setProperty(JAXP_SCHEMA_SOURCE, zipInputStream);

          InputStreamReader tableInputStreamReader = new InputStreamReader(new BOMInputStream(is),
            StandardCharsets.UTF_8);
          InputSource tableInputSource = new InputSource(tableInputStreamReader);
          tableInputSource.setEncoding("UTF-8");
          final XMLReader xmlReader = saxParser.getXMLReader();
          xmlReader.setContentHandler(handler);
          xmlReader.parse(tableInputSource);

          valid = handler.getValidationResult();
          LOGGER.debug("status: {}", valid);
          zipInputStream.close();
          map.clear();
        } else if (indexes.size() > 1) {
          Map<String, String> map = db
            .hashMap("compPrimaryKeys" + schemaFolder + tableFolder, Serializer.STRING, Serializer.STRING)
            .createOrOpen();
          CompositePrimaryKeyValidationHandler handler = new CompositePrimaryKeyValidationHandler(indexes, map,
            getValidationReporter(), T_601, tablePathFromFolder);

          final String xsdTablePathFromFolder = validatorPathStrategy.getXSDTablePathFromFolder(schemaFolder,
            tableFolder);
          final InputStream zipInputStream = zipFileManagerStrategy.getZipInputStream(this.path,
            xsdTablePathFromFolder);

          saxParser = saxParserFactory.newSAXParser();
          saxParser.setProperty(JAXP_SCHEMA_LANGUAGE, W3C_XML_SCHEMA);
          saxParser.setProperty(JAXP_SCHEMA_SOURCE, zipInputStream);
          InputStreamReader tableInputStreamReader = new InputStreamReader(new BOMInputStream(is),
            StandardCharsets.UTF_8);
          InputSource tableInputSource = new InputSource(tableInputStreamReader);
          tableInputSource.setEncoding("UTF-8");
          final XMLReader xmlReader = saxParser.getXMLReader();
          xmlReader.setContentHandler(handler);
          xmlReader.parse(tableInputSource);

          valid = handler.getValidationStatus();
          LOGGER.debug("status: {}", valid);
          zipInputStream.close();
          map.clear();
        }
      }
    }

    return valid;
  }

  private List<String> getPrimaryKeyColumnIndexes(String schemaFolder, String tableFolder)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {

    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:primaryKey/ns:column/text()";
    xpathExpression = xpathExpression.replace("$1", schemaFolder);
    xpathExpression = xpathExpression.replace("$2", tableFolder);
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path,
      validatorPathStrategy.getMetadataXMLPath())) {
      NodeList nodeList = (NodeList) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      List<String> indexes = new ArrayList<>();

      for (int i = 0; i < nodeList.getLength(); i++) {
        final String columnName = nodeList.item(i).getTextContent();
        indexes.add(COLUMN_XML_TAG + getColumnIndexByFolder(schemaFolder, tableFolder, columnName));
      }

      return indexes;
    }
  }

  private void getPrimaryKeyData(final String schemaName, final String tableName, final String columnName)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    String key = schemaName + "." + tableName + "." + columnName;
    String path = validatorPathStrategy.getXMLTablePathFromName(schemaName, tableName);
    LOGGER.debug("Obtaining primary key data for {}", path);
    if (primaryKeyData.get(key) == null) {
      primaryKeyData.put(key, getColumnDataByIndex(path, getColumnIndexByName(schemaName, tableName, columnName)));
    }
    LOGGER.debug("Finish obtaining primary key data for {}", path);
  }

  private boolean validateForeignKeyConstraint(String schemaFolder, String tableFolder)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    boolean valid = true;
    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:foreignKeys/ns:foreignKey";
    xpathExpression = xpathExpression.replace("$1", schemaFolder);
    xpathExpression = xpathExpression.replace("$2", tableFolder);
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path,
      validatorPathStrategy.getMetadataXMLPath())) {
      NodeList nodeList = (NodeList) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      final String path = validatorPathStrategy.getXMLTablePathFromFolder(schemaFolder, tableFolder);
      observer.notifyElementValidating(T_601, path);

      for (int i = 0; i < nodeList.getLength(); i++) {
        Element element = (Element) nodeList.item(i);
        final String foreignKeyName = element.getElementsByTagName("name").item(0).getTextContent();
        final String referencedSchema = element.getElementsByTagName("referencedSchema").item(0).getTextContent();
        final String referencedTable = element.getElementsByTagName("referencedTable").item(0).getTextContent();
        Element reference = (Element) element.getElementsByTagName("reference").item(0);
        final String columnName = reference.getElementsByTagName("column").item(0).getTextContent();
        final String referenced = reference.getElementsByTagName("referenced").item(0).getTextContent();

        getColumnIndexFromMetadataXML(schemaFolder, tableFolder, columnName, foreignKeyColumns);

        getPrimaryKeyData(referencedSchema, referencedTable, referenced);
        final Set<String> data = getColumnDataByIndex(path,
          getColumnIndexByFolder(schemaFolder, tableFolder, columnName));
        String key = referencedSchema + "." + referencedTable + "." + referenced;
        for (String value : data) {
          if (!primaryKeyData.get(key).contains(value)) {
            getValidationReporter().validationStatus(T_601, ValidationReporterStatus.ERROR,
              "All the table data (primary data) must meet the consistency requirements of SQL:2008.", "The value ("
                + value + ") for foreign key '" + foreignKeyName + "' is not present in '" + referencedTable + "' on "
                + validatorPathStrategy.getXMLTablePathFromFolder(schemaFolder, tableFolder));
            valid = false;
          }
        }
      }

      return valid;
    }
  }

  private Set<String> getColumnDataByIndex(String path, int index)
    throws SAXException, IOException, ParserConfigurationException {
    if (db.exists(HASH_SET_PREFIX + path + "c" + index)) {
      return db.get(HASH_SET_PREFIX + path + "c" + index);
    }

    try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path, path)) {
      final String columnIndex = "c" + index;
      Set<String> data = db.hashSet(HASH_SET_PREFIX + path + "c" + index, Serializer.STRING).createOrOpen();
      TableContentHandler tableContentHandler = new TableContentHandler(columnIndex, data);
      saxParser = saxParserFactory.newSAXParser();
      saxParser.parse(is, tableContentHandler);
      return data;
    }
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

    try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path,
      validatorPathStrategy.getMetadataXMLPath())) {
      NodeList nodeList = (NodeList) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodeList.getLength(); i++) {
        final String name = nodeList.item(i).getTextContent();
        if (columnName.equals(name)) {
          return i + 1;
        }
      }

      return -1;
    }
  }

  private void getColumnType(String schemaFolder, String tableFolder)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:columns/ns:column/ns:name/text()";
    xpathExpression = xpathExpression.replace("$1", schemaFolder);
    xpathExpression = xpathExpression.replace("$2", tableFolder);
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path,
      validatorPathStrategy.getMetadataXMLPath())) {
      NodeList columns = (NodeList) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      HashMap<String, String> columnToType = new HashMap<>();
      for (int j = 0; j < columns.getLength(); j++) {
        String columnName = columns.item(j).getNodeValue();
        xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:columns/ns:column[ns:name/text()='$3']/ns:type/text()";
        xpathExpression = xpathExpression.replace("$1", schemaFolder);
        xpathExpression = xpathExpression.replace("$2", tableFolder);
        xpathExpression = xpathExpression.replace("$3", columnName);

        try (InputStream inputStream = zipFileManagerStrategy.getZipInputStream(this.path,
          validatorPathStrategy.getMetadataXMLPath())) {
          String type = (String) XMLUtils.getXPathResult(inputStream, xpathExpression, XPathConstants.STRING,
            Constants.NAMESPACE_FOR_METADATA);
          String index = "c" + (j + 1);
          if (StringUtils.isBlank(type)) {
            xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table[ns:folder/text()='$2']/ns:columns/ns:column[ns:name/text()='$3']";
            xpathExpression = xpathExpression.replace("$1", schemaFolder);
            xpathExpression = xpathExpression.replace("$2", tableFolder);
            xpathExpression = xpathExpression.replace("$3", columnName);
            columnToType.putAll(getAdvancedOrUDTDataType(xpathExpression, index));
          } else {
            columnToType.put(index, type);
          }
        }
      }

      columnTypes.put(new SIARDContent(schemaFolder, tableFolder), columnToType);
    }
  }

  private HashMap<String, String> getAdvancedOrUDTDataType(final String xpathExpression, final String index)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path,
      validatorPathStrategy.getMetadataXMLPath())) {
      NodeList result = (NodeList) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      HashMap<String, String> pairs = new HashMap<>();

      for (int i = 0; i < result.getLength(); i++) {
        Element element = (Element) result.item(i);
        final String typeSchema = element.getElementsByTagName("typeSchema").item(0).getTextContent();
        final String typeName = element.getElementsByTagName("typeName").item(0).getTextContent();

        pairs.putAll(getUDT(typeSchema, typeName, index));

        String distinctXPathExpression = xpathExpression.replace("$3", Constants.DISTINCT);
        distinctXPathExpression = distinctXPathExpression.concat("/ns:base/text()");

        final HashMap<String, String> distinct = getDistinct(distinctXPathExpression, index);

        if (distinct != null) {
          pairs.putAll(distinct);
        }
      }

      return pairs;
    }
  }

  private HashMap<String, String> getDistinct(final String xpathExpression, final String index)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path,
      validatorPathStrategy.getMetadataXMLPath())) {

      String type = (String) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.STRING,
        Constants.NAMESPACE_FOR_METADATA);

      if (StringUtils.isBlank(type)) {
        return null;
      } else {
        HashMap<String, String> columnToType = new HashMap<>();
        columnToType.put(index, type);
        return columnToType;
      }
    }
  }

  private HashMap<String, String> getUDT(String typeSchema, String typeName, String index)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {

    String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:name/text()='$1']/ns:types/ns:type[ns:name/text()='$2' and ns:category/text()='$3']/ns:attributes/ns:attribute";
    xpathExpression = xpathExpression.replace("$1", typeSchema);
    xpathExpression = xpathExpression.replace("$2", typeName);
    xpathExpression = xpathExpression.replace("$3", Constants.UDT);

    HashMap<String, String> columnToType = new HashMap<>();
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path,
      validatorPathStrategy.getMetadataXMLPath())) {
      NodeList nodeList = (NodeList) XMLUtils.getXPathResult(is, xpathExpression, XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);
      for (int i = 0; i < nodeList.getLength(); i++) {
        Element element = (Element) nodeList.item(i);
        String concatIndex = index.concat(".u" + (i + 1));
        if (element.getElementsByTagName("type").item(0) != null) {
          final String type = element.getElementsByTagName("type").item(0).getTextContent();
          columnToType.put(concatIndex, type);
        } else {
          final String recTypeSchema = element.getElementsByTagName("typeSchema").item(0).getTextContent();
          final String recTypeName = element.getElementsByTagName("typeName").item(0).getTextContent();

          columnToType.putAll(getUDT(recTypeSchema, recTypeName, concatIndex));
        }
      }

      return columnToType;
    }
  }

  private void populateSQL2008Validations() {
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

  private void preCompileRegexPatterns() {
    patternBigIntRegex = Pattern.compile("^BIGINT$");
    patternIntegerRegex = Pattern.compile("^INTEGER$|^INT$");
    patternSmallIntRegex = Pattern.compile("^SMALLINT$");
    patternDecimalRegex = Pattern.compile("^(?:DECIMAL|DEC|NUMERIC)(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?$");
    patternDecimalRegexWithDataType = Pattern
      .compile("^(DECIMAL|DEC|NUMERIC)(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?$");
    patternRealRegex = Pattern.compile("^REAL$");
    patternFloatRegex = Pattern.compile("^FLOAT(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    patternDoublePrecisionRegex = Pattern.compile("^DOUBLE PRECISION$");
    patternBinaryRegex = Pattern.compile("^(?:BINARY\\s+VARYING|VARBINARY)(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    patternVarcharRegex = Pattern.compile("^VARCHAR\\s*(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    patternStringRegex = Pattern
      .compile("^(?:NATIONAL\\s+)?(?:CHARACTER|CHAR)(?:\\s+VARYING)?(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    patternNCharRegex = Pattern.compile("^NCHAR(?:\\s+VARYING\\s*)?(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    patternBooleanRegex = Pattern.compile("^BOOLEAN$");
    patternDateRegex = Pattern.compile("^DATE$");
    patternTimeRegex = Pattern.compile("^(?:TIME|TIME\\s+WITH\\s+TIME\\s+ZONE)(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$");
    patternTimestampRegex = Pattern
      .compile("^(?:TIMESTAMP|TIMESTAMP\\s+WITH\\s+TIME\\s+ZONE)(\\s*\\(\\s*(0|([1-9]\\d*))\\s*\\))?$");
    patternSizeRegex = Pattern.compile("\\d+");
    patternDateContentRegex = Pattern.compile("\\d{4}-\\d{2}-\\d{2}Z?");
    patternTimeContentRegex = Pattern.compile("\\d{2}:\\d{2}:\\d{2}Z?");
    patternTimestampContentRegex = Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d*)Z?");
  }
}
