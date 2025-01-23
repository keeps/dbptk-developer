/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.path;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD1ContentPathImportStrategy implements ContentPathImportStrategy {
  // constant directories
  private static final String CONTENT_FOLDER = "content";

  // extensions for files
  private static final String XML_EXTENSION = "xml";
  private static final String XSD_EXTENSION = "xsd";

  // control characters
  private static final String RESOURCE_FILE_SEPARATOR = "/";
  private static final String FILE_EXTENSION_SEPARATOR = ".";

  // < schema name , schema folder >
  private Map<String, String> schemaFolders = new HashMap<String, String>();

  // < table id , table folder >
  private Map<String, String> tableFolders = new HashMap<String, String>();

  // < column id , column folder >
  private Map<String, String> columnFolders = new HashMap<String, String>();

  public SIARD1ContentPathImportStrategy() {

  }

  @Override
  public String getLobPath(String basePath, String schemaName, String tableId, String columnId, String lobFileName) {
    throw new NotImplementedException("getLobPath in ContentPathImportStrategy method is not needed for SIARD1");
  }

  @Override
  public String getLobPathFallback(String basePath, String columnId, String lobFileName) {
    throw new NotImplementedException("getLobPathFallback in ContentPathImportStrategy method is not needed for SIARD1");
  }

  @Override
  public void associateSchemaWithFolder(String schemaName, String schemaFolder) {
    schemaFolders.put(schemaName, schemaFolder);
  }

  @Override
  public void associateTableWithFolder(String tableName, String tableFolder) {
    tableFolders.put(tableName, tableFolder);
  }

  @Override
  public void associateColumnWithFolder(String columnId, String columnFolder) {
    columnFolders.put(columnId, columnFolder);
  }

  @Override
  public String getTableXMLFilePath(String schemaName, String tableId) throws ModuleException {
    if (StringUtils.isBlank(schemaName)) {
      throw new ModuleException().withMessage("schema name can not be null");
    }
    if (StringUtils.isBlank(tableId)) {
      throw new ModuleException().withMessage("table id can not be null");
    }
    String schemaFolder = schemaFolders.get(schemaName);
    String tableFolder = tableFolders.get(tableId);

    if (StringUtils.isBlank(schemaFolder)) {
      throw new ModuleException().withMessage("No folder name for schema name \"" + schemaName + "\"");
    }
    if (StringUtils.isBlank(tableFolder)) {
      throw new ModuleException().withMessage("No folder name for table id \"" + tableId + "\"");
    }

    return new StringBuilder().append(CONTENT_FOLDER).append(RESOURCE_FILE_SEPARATOR).append(schemaFolder)
      .append(RESOURCE_FILE_SEPARATOR).append(tableFolder).append(RESOURCE_FILE_SEPARATOR).append(tableFolder)
      .append(FILE_EXTENSION_SEPARATOR).append(XML_EXTENSION).toString();
  }

  @Override
  public String getTableXSDFilePath(String schemaName, String tableId) throws ModuleException {
    if (StringUtils.isBlank(schemaName)) {
      throw new ModuleException().withMessage("schema name can not be null");
    }
    if (StringUtils.isBlank(tableId)) {
      throw new ModuleException().withMessage("table id can not be null");
    }
    String schemaFolder = schemaFolders.get(schemaName);
    String tableFolder = tableFolders.get(tableId);

    if (StringUtils.isBlank(schemaFolder)) {
      throw new ModuleException().withMessage("No folder name for schema name \"" + schemaName + "\"");
    }
    if (StringUtils.isBlank(tableFolder)) {
      throw new ModuleException().withMessage("No folder name for table id \"" + tableId + "\"");
    }

    return new StringBuilder().append(CONTENT_FOLDER).append(RESOURCE_FILE_SEPARATOR).append(schemaFolder)
      .append(RESOURCE_FILE_SEPARATOR).append(tableFolder).append(RESOURCE_FILE_SEPARATOR).append(tableFolder)
      .append(FILE_EXTENSION_SEPARATOR).append(XSD_EXTENSION).toString();
  }
}
