/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
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
import com.databasepreservation.model.exception.validator.XMLFileNotFoundException;
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
  private static final String A_M_561_2 = "A_M_5.6-1-2";
  private static final String M_561_3 = "M_5.6-1-3";
  private static final String A_M_561_5 = "A_M_5.6-1-5";
  private static final String A_M_561_12 = "A_M_5.6-1-12";

  private Set<String> typeOriginalSet = new HashSet<>();

  public MetadataColumnsValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_561, M_561_1, A_M_561_2, M_561_3, A_M_561_5, A_M_561_12);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_56);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_56, MODULE_NAME);

    validateMandatoryXSDFields(M_561, COLUMN_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:columns/ns:column");

    if (!readXMLMetadataColumnLevel()) {
      reportValidations(M_561, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    noticeTypeOriginalUsed();

    return reportValidations(MODULE_NAME);
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

          String name = XMLUtils.getChildTextContext(column, Constants.NAME);
          validateColumnName(name, path);

          path = buildPath(Constants.SCHEMA, schemaName, Constants.TABLE, tableName, Constants.COLUMN, name);

          String type = validateColumnType(XMLUtils.getChildTextContext(column, Constants.TYPE), column, path);

          if (type == null) {
            setError(A_M_561_2, String.format("Aborted because column type is mandatory (%s)", path));
          } else if (type.equals(Constants.CHARACTER_LARGE_OBJECT) || type.equals(Constants.BINARY_LARGE_OBJECT)
            || type.equals(Constants.BLOB) || type.equals(Constants.CLOB) || type.equals(Constants.XML_LARGE_OBJECT)) {
            String folder = XMLUtils.getChildTextContext(column, Constants.LOB_FOLDER);
            String columnNumber = "c" + (j + 1);
            validateColumnLobFolder(schemaFolderName, tableFolderName, type, folder, columnNumber, name, path);
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
   * M_5.6-1-1 The column name is mandatory in SIARD 2.1 specification
   */
  private void validateColumnName(String name, String path) {
    validateXMLField(M_561_1, name, Constants.NAME, true, false, path);
  }

  /**
   * M_5.6-1-3 The column type is mandatory in SIARD 2.1 specification
   *
   * @return If the data type of this column is a built-in data type, this field
   *         must be used. Otherwise the field typeName must refer to a defined
   *         type in the types list.
   */
  private String validateColumnType(String type, Element column, String path) {
    if (type == null || type.isEmpty()) {
      String typeName = XMLUtils.getChildTextContext(column, Constants.TYPE_NAME);
      if (typeName == null || typeName.isEmpty()) {
        setError(M_561_3, String.format("Column type cannot be empty (%s)", path));
      }
      type = typeName;
    } else {
      // TODO check SQL:2008 built-in types
    }
    return type;
  }

  /**
   * A_M_5.6-1-2 If the column is large object type (e.g. BLOB, CLOB or XML) which
   * are stored internally in the SIARD archive and the column has data in it then
   * this element should exist.
   */
  private void validateColumnLobFolder(String schemaFolder, String tableFolder, String type, String folder,
    String column, String name, String path) {
    String pathToTableColumn = createPath(Constants.SIARD_CONTENT_FOLDER, schemaFolder, tableFolder);

    if (schemaFolder == null || schemaFolder.isEmpty()) {
      setError(A_M_561_2, String.format("Aborted because schemaFolder is mandatory (%s)", path));
      return;
    }

    if (tableFolder == null || tableFolder.isEmpty()) {
      setError(A_M_561_2, String.format("Aborted because tableFolder is mandatory (%s)", path));
      return;
    }

    try {
      XMLInputFactory factory = XMLInputFactory.newInstance();
      InputStream zipInputStream = getZipInputStream(
        validatorPathStrategy.getXMLTablePathFromFolder(schemaFolder, tableFolder));
      if (zipInputStream == null) {
        throw new XMLFileNotFoundException(
          "Missing XML file " + validatorPathStrategy.getXMLTablePathFromFolder(schemaFolder, tableFolder));
      }

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
                  setError(A_M_561_2, String.format("not found record '%s' required by '%s'", fileName,
                    String.format("%s [row: %s column: %s]", pathToTableColumn, Integer.toString(rowNumber), column)));
                } else if (folder == null || folder.isEmpty()) {
                  addWarning(A_M_561_2, String.format("lobFolder must be set for column type  %s", type), path);
                }
              }
            }
          }
        }
      }
    } catch (XMLStreamException e) {
      String errorMessage = "Unable to read "
        + validatorPathStrategy.getXMLTablePathFromFolder(schemaFolder, tableFolder);
      setError(A_M_561_2, errorMessage);
      LOGGER.debug(errorMessage, e);
    } catch (XMLFileNotFoundException e) {
      setError(A_M_561_2, e.getMessage());
      LOGGER.debug(e.getMessage(), e);
    }
  }

  /**
   * A_M_5.6-1-5
   */
  private void noticeTypeOriginalUsed() {
    if (!typeOriginalSet.isEmpty()) {
      addNotice(A_M_561_5, String.format("Different data types used %s", typeOriginalSet.toString()), "");
    }
  }

  /**
   * A_M_5.6-1-12 The column name in SIARD file must not be empty.
   */
  private void validateColumnDescription(String description, String path) {
    validateXMLField(A_M_561_12, description, Constants.DESCRIPTION, false, true, path);
  }
}
