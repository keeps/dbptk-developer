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

import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ContentPathImportStrategy implements ContentPathImportStrategy {

  // constant directories
  private static final String CONTENT_FOLDER = "content";

  // extensions for files
  private static final String XML_EXTENSION = "xml";
  private static final String XSD_EXTENSION = "xsd";

  private static final String RESOURCE_FILE_SEPARATOR = "/";
  private static final String FILE_EXTENSION_SEPARATOR = ".";

  private String metadataLobFolder;

  // < schema name , schema folder >
  private Map<String, String> schemaFolders = new HashMap<String, String>();

  // < table id , table folder >
  private Map<String, String> tableFolders = new HashMap<String, String>();

  // < column id , column folder >
  private Map<String, String> columnFolders = new HashMap<String, String>();

  public SIARD2ContentPathImportStrategy() {

  }

  public void setMetadataLobFolder(String metadataLobFolder) {
    // from SIARD2 specification: this path defaults to "." (current
    // directory) if not specified
    if (metadataLobFolder != null) {
      this.metadataLobFolder = metadataLobFolder;
    } else {
      this.metadataLobFolder = ".";
    }
  }

  @Override
  public String getLobPath(String basePath, String schemaName, String tableId, String columnId, String lobFileName) {
    if (StringUtils.isBlank(basePath)) {
      basePath = metadataLobFolder;
    }

    String schemaPart = schemaFolders.get(schemaName);
    String tablePart = tableFolders.get(tableId);
    String columnPart = columnFolders.get(columnId);

    // from SIARD2 specification: these path parts default to "." (current
    // directory) if not specified
    if (schemaPart == null) {
      schemaPart = ".";
    }
    if (tablePart == null) {
      tablePart = ".";
    }
    if (columnPart == null) {
      columnPart = ".";
    }

    if (".".equals(basePath) && ".".equals(columnPart) && lobFileName.startsWith("..")) {
      return lobFileName.substring(3);
    } else if (".".equals(columnPart)) {
      return lobFileName;
    } else {
      return new StringBuilder().append(basePath).append(RESOURCE_FILE_SEPARATOR).append(schemaPart)
        .append(RESOURCE_FILE_SEPARATOR).append(tablePart).append(RESOURCE_FILE_SEPARATOR).append(columnPart)
        .append(RESOURCE_FILE_SEPARATOR).append(lobFileName).toString();
    }
  }

  @Override
  public void associateSchemaWithFolder(String schemaName, String schemaFolder) {
    schemaFolders.put(schemaName, schemaFolder);
  }

  @Override
  public void associateTableWithFolder(String tableId, String tableFolder) {
    tableFolders.put(tableId, tableFolder);
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
