package com.databasepreservation.modules.siard.validate.TableData;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.validate.ValidatorModule;
import com.databasepreservation.model.reporters.ValidationReporter;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class TableSchemaDefinitionValidator extends ValidatorModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSchemaDefinitionValidator.class);

  private static final String MODULE_NAME = "Table schema definition";
  private static final String P_61 = "T_6.1";
  private static final String P_611 = "T_6.1-1";
  private static final String P_612 = "T_6.1-2";
  private static final String P_613 = "T_6.1-3";
  private static final String P_614 = "T_6.1-4";
  private static final String XSD_EXTENSION = ".xsd";
  private static final String TABLE = "table";

  private static ZipFile zipFile = null;
  private static List<String> zipFileNames = null;

  public static TableSchemaDefinitionValidator newInstance() {
    return new TableSchemaDefinitionValidator();
  }

  private TableSchemaDefinitionValidator() {
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(P_61, MODULE_NAME);

    if (validateXMLSchemaDefinition()) {
      getValidationReporter().validationStatus(P_611, ValidationReporter.Status.OK);
    } else {
      validationFailed(P_611, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateColumnsTag()) {
      getValidationReporter().validationStatus(P_612, ValidationReporter.Status.OK);
    } else {
      validationFailed(P_612, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateXMLSchemaStandardTypes()) {
      getValidationReporter().validationStatus(P_613, ValidationReporter.Status.OK);
    } else {
      validationFailed(P_613, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateAdvancedOrStructuredType()) {
      getValidationReporter().validationStatus(P_614, ValidationReporter.Status.OK);
    } else {
      validationFailed(P_614, MODULE_NAME);
      closeZipFile();
      return false;
    }

    getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.OK);
    closeZipFile();

    return true;
  }

  /**
   * T_6.1-1
   *
   * There must be an XML schema definition for each table that indicates the XML
   * storage format of the table data.
   *
   * @return true if valid otherwise false
   */
  private boolean validateXMLSchemaDefinition() {
    if (preValidationRequirements())
      return false;

    if (zipFileNames == null) {
      try {
        retrieveFilesInsideZip();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    List<String> tableData = new ArrayList<>();

    for (String zipFileName : zipFileNames) {
      String regexPattern = "^(content/schema[0-9]+/table[0-9]+/table[0-9]+)\\.xml$";

      Pattern pattern = Pattern.compile(regexPattern);
      Matcher matcher = pattern.matcher(zipFileName);

      while (matcher.find()) {
        tableData.add(matcher.group(1));
      }
    }

    for (String path : tableData) {
      final String XSDPath = path.concat(XSD_EXTENSION);
      if (!zipFileNames.contains(XSDPath)) {
        return false;
      }
    }

    return true;
  }

  /**
   * T_6.1-2
   *
   * This schema definition reflects the SQL schema metadata of the table and
   * indi- cates that the table is stored as a sequence of lines containing a
   * sequence of column entries with various XML types. The name of the table tag
   * is table, that of the dataset tag is row, while the column tags are called
   * c1, c2, ....
   *
   * The column tags always start with c1 and increase by 1. There must be no gap,
   * because a NULL value is expressed by a missing corresponding column in the
   * XML file.
   * 
   * @return true if valid otherwise false
   */
  private boolean validateColumnsTag() {
    if (preValidationRequirements())
      return false;

    if (zipFileNames == null) {
      try {
        retrieveFilesInsideZip();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    for (String zipFileName : zipFileNames) {
      String regexPattern = "^(content/schema[0-9]+/table[0-9]+/table[0-9]+)\\.xsd$";

      if (zipFileName.matches(regexPattern)) {
        try {
          String rowName = (String) getXPathResult(zipFileName,
            "/xs:schema/xs:element[@name='table']/xs:complexType/xs:sequence/xs:element/@name", XPathConstants.STRING,
            TABLE);
          if (StringUtils.isNotBlank(rowName) && !rowName.equals("row"))
            return false;

          NodeList resultNodes = (NodeList) getXPathResult(zipFileName,
            "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element", XPathConstants.NODESET, TABLE);

          for (int i = 0; i < resultNodes.getLength(); i++) {
            final String name = resultNodes.item(i).getAttributes().getNamedItem("name").getNodeValue();
            if (!validateSequence(name, "^c[0-9]+$", i+1)) return false;
          }
        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * T_6.1-3
   *
   * The type mapping to be used in table schema definitions is specified in
   * P_4.3-3. Apart from XML Schema standard types the following special type are
   * used:
   *
   * clobType, blobType, dateType, timeType, dateTimeType.
   *
   * @return true if valid otherwise false
   */
  private boolean validateXMLSchemaStandardTypes() {
    if (preValidationRequirements())
      return false;

    if (zipFileNames == null) {
      try {
        retrieveFilesInsideZip();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    for (String zipFileName : zipFileNames) {
      String regexPattern = "^(content/schema[0-9]+/table[0-9]+/table[0-9]+)\\.xsd$";

      if (zipFileName.matches(regexPattern)) {
        try {
          NodeList resultNodes = (NodeList) getXPathResult(zipFileName,
            "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element/@type", XPathConstants.NODESET,
            TABLE);

          for (int i = 0; i < resultNodes.getLength(); i++) {
            String type = resultNodes.item(i).getNodeValue();
            if (!type.startsWith("xs")) {
              if (!(type.equals("clobType") || type.equals("blobType") || type.equals("dateType")
                || type.equals("timeType") || type.equals("dateTimeType"))) {
                return false;
              }
            }
          }
        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * T_6.1-4
   *
   * Multiple cell values of advanced or structured types are to be stored as
   * separate elements inside the cell tags. The names of the individual elements
   * of an ARRAY are a1, a2, .... The names of the individual elements of a UDT
   * are u1, u2, .... The names always start with a1 or u1, respectively, and
   * increase by 1. There must be no gap, because a NULL value is expressed by a
   * missing corresponding column in the XML file.
   *
   * @return true if valid otherwise false
   */
  private boolean validateAdvancedOrStructuredType() {
    if (preValidationRequirements())
      return false;

    if (zipFileNames == null) {
      try {
        retrieveFilesInsideZip();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    for (String zipFileName : zipFileNames) {
      String regexPattern = "^(content/schema[0-9]+/table[0-9]+/table[0-9]+)\\.xsd$";

      if (zipFileName.matches(regexPattern)) {
        try {
          NodeList resultNodes = (NodeList) getXPathResult(zipFileName,
            "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element[not(@type)]/@name",
            XPathConstants.NODESET, TABLE);

          for (int i = 0; i < resultNodes.getLength(); i++) {
            final String nodeValue = resultNodes.item(i).getNodeValue();
            String xpathExpression = "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element[@name='$1']/xs:complexType/xs:sequence/xs:element";
            xpathExpression = xpathExpression.replace("$1", nodeValue);
            if (!checkAdvancedOrStruturedSequence(zipFileName, xpathExpression, null)) return false;
          }
        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          return false;
        }
      }
    }

    return true;
  }

  /*
   * Auxiliary Methods
   */

  private boolean preValidationRequirements() {
    if (getSIARDPackagePath() == null) {
      return true;
    }

    if (zipFile == null) {
      try {
        getZipFile();
      } catch (IOException e) {
        return true;
      }
    }

    return false;
  }

  private void getZipFile() throws IOException {
    if (zipFile == null)
      zipFile = new ZipFile(getSIARDPackagePath().toFile());
  }

  private void retrieveFilesInsideZip() throws IOException {
    zipFileNames = new ArrayList<>();
    if (zipFile == null) {
      getZipFile();
    }
    final Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();
    while (entries.hasMoreElements()) {
      zipFileNames.add(entries.nextElement().getName());
    }
  }

  private boolean checkAdvancedOrStruturedSequence(String zipFileName, String xpathExpression, String userDefinedColumnName) throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    if (userDefinedColumnName != null) {
      xpathExpression = xpathExpression.concat("[@name='$1']/xs:complexType/xs:sequence/xs:element");
      xpathExpression = xpathExpression.replace("$1", userDefinedColumnName);
    }

    NodeList result = (NodeList) getXPathResult(zipFileName, xpathExpression, XPathConstants.NODESET, TABLE);
    for (int i = 0; i < result.getLength(); i++) {
      final Node type = result.item(i).getAttributes().getNamedItem("type");
      final String name;
      if (type == null) {
        name = result.item(i).getAttributes().getNamedItem("name").getNodeValue();
        if (!checkAdvancedOrStruturedSequence(zipFileName, xpathExpression, name)) return false;
      } else {
        name = result.item(i).getAttributes().getNamedItem("name").getNodeValue();
      }
      if (!validateSequence(name, "^a[0-9]+$", i+1)) {
        if (!validateSequence(name, "u[0-9]+$", i+1)) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean validateSequence(String name, String regex, int sequenceValue) {
    if (!name.matches(regex))
      return false;

    int value = Integer.parseInt(name.substring(1));
    return value == sequenceValue;
  }

  private Object getXPathResult(final String pathToEntry, final String xpathExpression, QName constants,
    final String type) throws IOException, ParserConfigurationException, SAXException, XPathExpressionException {
    final ZipArchiveEntry entry = zipFile.getEntry(pathToEntry);
    final InputStream inputStream = zipFile.getInputStream(entry);
    Document document = getDocument(inputStream);

    XPathFactory xPathFactory = XPathFactory.newInstance();
    XPath xpath = xPathFactory.newXPath();

    xpath = setXPath(xpath, type);

    XPathExpression expression = xpath.compile(xpathExpression);

    return expression.evaluate(document, constants);
  }

  private static Document getDocument(InputStream inputStream)
    throws IOException, SAXException, ParserConfigurationException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder = factory.newDocumentBuilder();

    return builder.parse(inputStream);
  }

  private static XPath setXPath(XPath xPath, final String type) {
    xPath.setNamespaceContext(new NamespaceContext() {
      @Override
      public Iterator getPrefixes(String arg0) {
        return null;
      }

      @Override
      public String getPrefix(String arg0) {
        return null;
      }

      @Override
      public String getNamespaceURI(String arg0) {
        if ("xs".equals(arg0)) {
          return "http://www.w3.org/2001/XMLSchema";
        }
        if ("ns".equals(arg0)) {
          if (TABLE.equals(type)) {
            return "http://www.bar.admin.ch/xmlns/siard/2/table.xsd";
          }
          return "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd";
        }
        return null;
      }
    });

    return xPath;
  }

  private void closeZipFile() throws ModuleException {
    if (zipFile != null) {
      try {
        zipFile.close();
      } catch (IOException e) {
        throw new ModuleException().withCause(e.getCause()).withMessage("Error trying to close the SIARD file");
      }
      zipFile = null;
    }
  }
}
