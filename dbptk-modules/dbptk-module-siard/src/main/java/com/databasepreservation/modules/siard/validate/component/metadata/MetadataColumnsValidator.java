package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataColumnsValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataColumnsValidator.class);
  private final String MODULE_NAME;
  private static final String M_56 = "5.6";
  private static final String M_561 = "M_5.6-1";
  private static final String M_561_1 = "M_5.6-1-1";
  private static final String M_561_3 = "M_5.6-1-3";
  private static final String M_561_5 = "M_5.6-1-5";
  private static final String M_561_12 = "M_5.6-1-12";

  private Set<String> typeOriginalSet = new HashSet<>();

  public MetadataColumnsValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_561, M_561_1, M_561_3, M_561_5, M_561_12);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_56);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_56, MODULE_NAME);

    if (!validateMandatoryXSDFields(M_561, COLUMN_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:columns/ns:column")) {
      reportValidations(M_561, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (!readXMLMetadataColumnLevel()) {
      reportValidations(M_561, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    noticeTypeOriginalUsed();

    if (reportValidations(MODULE_NAME)) {
      metadataValidationPassed(MODULE_NAME);
      return true;
    }
    return false;
  }

  private boolean readXMLMetadataColumnLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element table = (Element) nodes.item(i);
        String tableFolderName = XMLUtils.getChildTextContext(table, Constants.FOLDER);
        String tableName = XMLUtils.getChildTextContext(table, Constants.NAME);

        Element schemaElement = (Element) table.getParentNode().getParentNode();
        String schemaFolderName = XMLUtils.getChildTextContext(schemaElement, Constants.FOLDER);
        String schemaName = XMLUtils.getChildTextContext(schemaElement, Constants.NAME);

        Element columns = ((Element) table.getElementsByTagName(Constants.COLUMNS).item(0));
        NodeList columnNodes = columns.getElementsByTagName(Constants.COLUMN);

        for (int j = 0; j < columnNodes.getLength(); j++) {
          Element column = (Element) columnNodes.item(j);
          String path = buildPath(Constants.SCHEMA, schemaName, Constants.TABLE, tableName, Constants.COLUMN,
            Integer.toString(j));

          // * M_5.6-1 The column name in SIARD is mandatory.
          String name = XMLUtils.getChildTextContext(column, Constants.NAME);
          if (!validateColumnName(name, path))
            continue; // next column

          path = buildPath(Constants.SCHEMA, schemaName, Constants.TABLE, tableName, Constants.COLUMN, name);

          // * M_5.6-1 (SIARD specification) The column type in SIARD is mandatory.
          String type = XMLUtils.getChildTextContext(column, Constants.TYPE);
          if (type == null || type.isEmpty()) {
            String typeName = XMLUtils.getChildTextContext(column, Constants.TYPE_NAME);
            if (typeName == null || typeName.isEmpty()) {
              setError(M_561, String.format("Column type cannot be null (%s)", path));
              continue; // next column
            }
            type = typeName;
          }

          if (type.equals(Constants.CHARACTER_LARGE_OBJECT) || type.equals(Constants.BINARY_LARGE_OBJECT)
            || type.equals(Constants.BLOB) || type.equals(Constants.CLOB) || type.equals(Constants.XML_LARGE_OBJECT)) {
            String folder = XMLUtils.getChildTextContext(column, Constants.LOB_FOLDER);
            String columnNumber = "c" + (j + 1);
            if (!validateColumnLobFolder(schemaFolderName, tableFolderName, type, folder, columnNumber, name, path))
              continue; // next column
          }

          String typeOriginal = XMLUtils.getChildTextContext(column, Constants.TYPE_ORIGINAL);
          if (typeOriginal != null) {
            typeOriginalSet.add(typeOriginal);
          }

          String description = XMLUtils.getChildTextContext(column, Constants.DESCRIPTION);
          validateColumnDescription(description, path);
        }
      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read columns from SIARD file";
      setError(M_561, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }
    return true;
  }

  /**
   * M_5.6-1-1 The column name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumnName(String name, String path) {
    return validateXMLField(M_561_1, name, Constants.NAME, true, false, path);
  }

  /**
   * M_5.6-1-3 If the column is large object type (e.g. BLOB, CLOB or XML) which
   * are stored internally in the SIARD archive and the column has data in it then
   * this element should exist.
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumnLobFolder(String schemaFolder, String tableFolder, String type, String folder,
    String column, String name, String path) {
    String pathToTableColumn = createPath(Constants.SIARD_CONTENT_FOLDER, schemaFolder, tableFolder);

    try {
      XMLInputFactory factory = XMLInputFactory.newInstance();
      InputStream zipInputStream = getZipInputStream(
        validatorPathStrategy.getXMLTablePathFromFolder(schemaFolder, tableFolder));

      XMLStreamReader streamReader = factory.createXMLStreamReader(zipInputStream);

      // xPath doesn't work well with files over 1gb
      int rowNumber = 0;
      while (streamReader.hasNext()) {
        streamReader.next();
        if (streamReader.getEventType() == XMLStreamReader.START_ELEMENT) {
          if (streamReader.getLocalName().equalsIgnoreCase("row"))
            rowNumber++;
          if (streamReader.getLocalName().equalsIgnoreCase(column)) {
            if (streamReader.getAttributeCount() > 0) {
              String fileName = streamReader.getAttributeValue(null, "file");
              if (!fileName.isEmpty()) {
                if (getZipFile().getEntry(fileName) == null) {
                  setError(M_561_3, String.format("not found record '%s' required by '%s'", fileName,
                    String.format("%s [row: %s column: %s]", pathToTableColumn, Integer.toString(rowNumber), column)));
                } else if (folder == null || folder.isEmpty()) {
                  addWarning(M_561_3, String.format("lobFolder must be set for column type  %s", type), path);
                }
              }
            }
          }
        }
      }
    } catch (XMLStreamException e) {
      String errorMessage = "Unable to read table.xml";
      LOGGER.debug(errorMessage, e);
      return false;
    }

    return true;
  }

  private void noticeTypeOriginalUsed() {
    if (!typeOriginalSet.isEmpty()) {
      addNotice(M_561_5, String.format("Different data types used %s", typeOriginalSet.toString()), "");
    }
  }

  /**
   * M_5.6-1-12 The column name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private void validateColumnDescription(String description, String path) {
    validateXMLField(M_561_12, description, Constants.DESCRIPTION, false, true, path);
  }
}
