package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataColumnsValidator extends MetadataValidator {
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
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(M_56, MODULE_NAME);

    if (!readXMLMetadataColumnLevel()) {
      reportValidations(M_561, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (reportValidations(M_561, MODULE_NAME) && reportValidations(M_561_1, MODULE_NAME)
      && reportValidations(M_561_3, MODULE_NAME) && noticeTypeOriginalUsed()
      && reportValidations(M_561_12, MODULE_NAME)) {
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.PASSED);
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

          // * M_5.6-1 The column name in SIARD is mandatory.
          String name = XMLUtils.getChildTextContext(column, Constants.NAME);
          String path = buildPath(Constants.SCHEMA, schemaName, Constants.TABLE, tableName, Constants.COLUMN, name);
          if (!validateColumnName(name, path))
            break;

          // * M_5.6-1 The column type in SIARD is mandatory.
          String type = XMLUtils.getChildTextContext(column, Constants.TYPE);
          if (type == null || type.isEmpty()) {
            String typeName = XMLUtils.getChildTextContext(column, Constants.TYPE_NAME);
            if (typeName == null || typeName.isEmpty()) {
              setError(M_561, String.format("Column type cannot be null (%s)", path));
              return false;
            }
            type = typeName;
          }

          if (type.equals(Constants.CHARACTER_LARGE_OBJECT) || type.equals(Constants.BINARY_LARGE_OBJECT)
            || type.equals(Constants.BLOB) || type.equals(Constants.CLOB) || type.equals(Constants.XML_LARGE_OBJECT)) {
            String folder = XMLUtils.getChildTextContext(column, Constants.LOB_FOLDER);
            String columnNumber = "c" + (j + 1);
            if (!validateColumnLobFolder(schemaFolderName, tableFolderName, type, folder, columnNumber, name, path))
              break;
          }

          String typeOriginal = XMLUtils.getChildTextContext(column, Constants.TYPE_ORIGINAL);
          if (typeOriginal != null) {
            typeOriginalSet.add(typeOriginal);
          }

          String description = XMLUtils.getChildTextContext(column, Constants.DESCRIPTION);
          if (!validateColumnDescription(description, path))
            break;

        }
      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
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
    if (!HasReferenceToLobFolder(pathToTableColumn, schemaFolder, tableFolder, column,
      folder)) {
      if (folder == null || folder.isEmpty()) {
        addWarning(M_561_3, String.format("lobFolder must be set for column type  %s", type), path);
      } else {
        setError(M_561_3, "not found lobFolder(" + folder + ") required by "
          + createPath(pathToTableColumn, name, column));
      }
      return false;
    }
    return true;
  }

  private boolean HasReferenceToLobFolder(String path, String schemaFolder, String tableFolder, String column,
    String folder) {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(
        getZipInputStream(validatorPathStrategy.getXMLTablePathFromFolder(schemaFolder, tableFolder)),
        "/ns:table/ns:row/ns:" + column, XPathConstants.NODESET, Constants.NAMESPACE_FOR_TABLE);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element columnNumber = (Element) nodes.item(i);
        String fileName = columnNumber.getAttribute("file");

        if (!fileName.isEmpty() && getZipFile().getEntry(createPath(path, folder, fileName)) == null) {
          return false;
        }
      }

    } catch (IOException | XPathExpressionException | ParserConfigurationException | SAXException e) {
      return false;
    }
    return true;
  }

  /**
   * M_5.6-1-1 The column name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean noticeTypeOriginalUsed() {
    getValidationReporter().validationStatus(M_561_5, ValidationReporter.Status.OK);
    getValidationReporter().notice(M_561_5, typeOriginalSet.toString());
    return true;
  }

  /**
   * M_5.6-1-12 The column name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumnDescription(String description, String path) {
    return validateXMLField(M_561_12, description, Constants.DESCRIPTION, false, true, path);
  }
}
