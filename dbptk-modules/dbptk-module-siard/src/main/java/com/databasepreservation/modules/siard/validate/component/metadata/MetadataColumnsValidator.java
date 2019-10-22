/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
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
import com.databasepreservation.model.reporters.ValidationReporterStatus;
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
  }

  @Override
  public void clean() {
    typeOriginalSet.clear();
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_56);

    getValidationReporter().moduleValidatorHeader(M_56, MODULE_NAME);

    validateMandatoryXSDFields(M_561, COLUMN_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:columns/ns:column");

    NodeList columns;
    NodeList tables;
    try (
      InputStream isColumns = zipFileManagerStrategy.getZipInputStream(path,
        validatorPathStrategy.getMetadataXMLPath());
      InputStream isTables = zipFileManagerStrategy.getZipInputStream(path,
        validatorPathStrategy.getMetadataXMLPath())) {
      columns = (NodeList) XMLUtils.getXPathResult(isColumns,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:columns/ns:column", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      tables = (NodeList) XMLUtils.getXPathResult(isTables,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read columns from SIARD file";
      setError(M_561, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    if (validateColumnNames(columns)) {
      validationOk(MODULE_NAME, M_561_1);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_561_1, ValidationReporterStatus.ERROR);
    }

    if (validateColumnLobFolders(tables)) {
      validationOk(MODULE_NAME, A_M_561_2);
    } else {
      observer.notifyValidationStep(MODULE_NAME, A_M_561_2, ValidationReporterStatus.ERROR);
    }

    if (validateColumnTypes(columns)) {
      validationOk(MODULE_NAME, M_561_3);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_561_3, ValidationReporterStatus.ERROR);
    }

    noticeTypeOriginalUsed(columns);

    validateColumnDescription(columns);
    validationOk(MODULE_NAME, A_M_561_12);

    return reportValidations(MODULE_NAME);
  }

  private boolean validateColumnNames(NodeList nodes) {
    boolean hasErrors = false;

    for (int i = 0; i < nodes.getLength(); i++) {
      Element column = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(column, Constants.SCHEMA),
        Constants.TABLE, XMLUtils.getParentNameByTagName(column, Constants.TABLE), Constants.COLUMN,
        Integer.toString(i));
      String name = XMLUtils.getChildTextContext(column, Constants.NAME);

      if (!validateXMLField(M_561_1, name, Constants.NAME, true, false, path)) {
        hasErrors = true;
      }
    }

    return !hasErrors;
  }

  /**
   * A_M_5.6-1-2
   */
  private boolean validateColumnLobFolders(NodeList nodes) {
    boolean hasErrors = false;

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
        String name = XMLUtils.getChildTextContext(column, Constants.NAME);
        String type = XMLUtils.getChildTextContext(column, Constants.TYPE);

        String path = buildPath(Constants.SCHEMA, schemaName, Constants.TABLE, tableName, Constants.COLUMN, name);

        if (type == null || type.isEmpty()) {
          setError(A_M_561_2, String.format("Aborted because column type is mandatory (%s)", path));
          hasErrors = true;
        } else if (type.equals(Constants.CHARACTER_LARGE_OBJECT) || type.equals(Constants.BINARY_LARGE_OBJECT)
          || type.equals(Constants.BLOB) || type.equals(Constants.CLOB) || type.equals(Constants.XML_LARGE_OBJECT)) {
          String folder = XMLUtils.getChildTextContext(column, Constants.LOB_FOLDER);
          String columnNumber = "c" + (j + 1);
          if (!validateLobFolder(schemaFolderName, tableFolderName, type, folder, columnNumber, path)) {
            hasErrors = true;
          }
          ;
        }
      }
    }

    return !hasErrors;
  }

  /**
   * A_M_5.6-1-2 If the column is large object type (e.g. BLOB, CLOB or XML) which
   * are stored internally in the SIARD archive and the column has data in it then
   * this element should exist.
   */
  private boolean validateLobFolder(String schemaFolder, String tableFolder, String type, String folder, String column,
    String path) {
    boolean hasErrors = false;
    String pathToTableColumn = createPath(Constants.SIARD_CONTENT_FOLDER, schemaFolder, tableFolder);

    if (schemaFolder == null || schemaFolder.isEmpty()) {
      setError(A_M_561_2, String.format("Aborted because schemaFolder is mandatory (%s)", path));
      return false;
    }

    if (tableFolder == null || tableFolder.isEmpty()) {
      setError(A_M_561_2, String.format("Aborted because tableFolder is mandatory (%s)", path));
      return false;
    }

    try (InputStream is = zipFileManagerStrategy.getZipInputStream(this.path,
      validatorPathStrategy.getXMLTablePathFromFolder(schemaFolder, tableFolder))) {
      XMLInputFactory factory = XMLInputFactory.newInstance();
      if (is == null) {
        throw new XMLFileNotFoundException(
          "Missing XML file " + validatorPathStrategy.getXMLTablePathFromFolder(schemaFolder, tableFolder));
      }

      XMLStreamReader streamReader = factory.createXMLStreamReader(is);

      // Used XMLStreamReader because xPath doesn't work well with files over 1gb
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
                if (Paths.get(fileName).getName(0).toString().equals(Constants.SIARD_CONTENT_FOLDER)) {
                  // Internal LOB
                  if (zipFileManagerStrategy.getZipArchiveEntry(this.path, fileName) == null) {
                    setError(A_M_561_2, String.format("not found record '%s' required by '%s'", fileName, String
                      .format("%s [row: %s column: %s]", pathToTableColumn, Integer.toString(rowNumber), column)));
                    hasErrors = true;
                  } else if (folder == null || folder.isEmpty()) {
                    addWarning(A_M_561_2, String.format("lobFolder must be set for column type  %s", type), path);
                  }
                } else {
                  // External LOB
                  File SIARDFile = getSIARDPackagePath().resolve(fileName).toFile();
                  File lobFolderFile = SIARDFile.getCanonicalFile();

                  if (!lobFolderFile.exists()) {
                    setError(A_M_561_2, String.format("not found external lob '%s' required by '%s'", fileName, String
                      .format("%s [row: %s column: %s]", pathToTableColumn, Integer.toString(rowNumber), column)));
                    hasErrors = true;
                  } else if (folder == null || folder.isEmpty()) {
                    addWarning(A_M_561_2, String.format("lobFolder must be set for column type  %s", type), path);
                  }
                }
              }
            }
          }
        }
      }
    } catch (XMLStreamException | IOException e) {
      String errorMessage = "Unable to read "
        + validatorPathStrategy.getXMLTablePathFromFolder(schemaFolder, tableFolder);
      setError(A_M_561_2, errorMessage);
      LOGGER.debug(errorMessage, e);
      hasErrors = true;
    } catch (XMLFileNotFoundException e) {
      setError(A_M_561_2, e.getMessage());
      LOGGER.debug(e.getMessage(), e);
      hasErrors = true;
    }
    return !hasErrors;
  }

  /**
   * M_5.6-1-3 The column type is mandatory in SIARD 2.1 specification
   *
   * @return If the data type of this column is a built-in data type, this field
   *         must be used. Otherwise the field typeName must refer to a defined
   *         type in the types list.
   */
  private boolean validateColumnTypes(NodeList nodes) {
    boolean hasErrors = false;

    for (int i = 0; i < nodes.getLength(); i++) {
      Element column = (Element) nodes.item(i);

      // schema/table/column/name
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(column, Constants.SCHEMA),
        Constants.TABLE, XMLUtils.getParentNameByTagName(column, Constants.TABLE), Constants.COLUMN,
        XMLUtils.getChildTextContext(column, Constants.NAME));

      String type = XMLUtils.getChildTextContext(column, Constants.TYPE);

      if (type == null || type.isEmpty()) {
        String typeName = XMLUtils.getChildTextContext(column, Constants.TYPE_NAME);
        if (typeName == null || typeName.isEmpty()) {
          setError(M_561_3, String.format("Column type cannot be empty (%s)", path));
          hasErrors = true;
        }
        type = typeName;
      } else {
        // TODO check SQL:2008 built-in types
      }
    }

    return !hasErrors;
  }

  private void noticeTypeOriginalUsed(NodeList nodes) {
    for (int i = 0; i < nodes.getLength(); i++) {
      String typeOriginal = XMLUtils.getChildTextContext((Element) nodes.item(i), Constants.TYPE_ORIGINAL);
      if (typeOriginal != null) {
        typeOriginalSet.add(typeOriginal);
      }
    }

    if (!typeOriginalSet.isEmpty()) {
      addNotice(A_M_561_5, String.format("Different data types used %s", typeOriginalSet.toString()), "");
    }
  }

  /**
   * A_M_5.6-1-12 The column description in SIARD file must not be empty.
   */
  private void validateColumnDescription(NodeList nodes) {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element column = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(column, Constants.SCHEMA),
        Constants.TABLE, XMLUtils.getParentNameByTagName(column, Constants.TABLE), Constants.COLUMN,
        XMLUtils.getChildTextContext(column, Constants.NAME));
      String description = XMLUtils.getChildTextContext(column, Constants.DESCRIPTION);
      validateXMLField(A_M_561_12, description, Constants.DESCRIPTION, false, true, path);
    }
  }
}
