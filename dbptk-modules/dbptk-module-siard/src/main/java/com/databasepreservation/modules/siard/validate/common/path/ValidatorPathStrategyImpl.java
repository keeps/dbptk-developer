/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.common.path;

import java.util.HashMap;
import java.util.Map;

import com.databasepreservation.Constants;
import com.databasepreservation.common.ValidatorPathStrategy;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class ValidatorPathStrategyImpl implements ValidatorPathStrategy {

  private HashMap<String, String> schemas = null;
  private HashMap<String, Map<String, String>> tables = null;

  @Override
  public String getXMLTablePathFromFolder(String schemaFolder, String tableFolder) {
    return Constants.SIARD_CONTENT_FOLDER + Constants.RESOURCE_FILE_SEPARATOR + schemaFolder
      + Constants.RESOURCE_FILE_SEPARATOR + tableFolder + Constants.RESOURCE_FILE_SEPARATOR + tableFolder
      + Constants.XML_EXTENSION;
  }

  @Override
  public String getXMLTablePathFromName(String schemaName, String tableName) {
    final String schemaFolder = schemas.get(schemaName);
    final String tableFolder = getTableFolder(schemaName, tableName);
    return getXMLTablePathFromFolder(schemaFolder, tableFolder);
  }

  @Override
  public String getXSDTablePathFromFolder(String schemaFolder, String tableFolder) {
    return Constants.SIARD_CONTENT_FOLDER + Constants.RESOURCE_FILE_SEPARATOR + schemaFolder
      + Constants.RESOURCE_FILE_SEPARATOR + tableFolder + Constants.RESOURCE_FILE_SEPARATOR + tableFolder
      + Constants.XSD_EXTENSION;
  }

  @Override
  public String getXSDTablePathFromName(String schemaName, String tableName) {
    final String schemaFolder = schemas.get(schemaName);
    final String tableFolder = getTableFolder(schemaName, tableName);
    return getXSDTablePathFromFolder(schemaFolder, tableFolder);
  }

  @Override
  public String getMetadataXMLPath() {
    return Constants.SIARD_HEADER_FOLDER + Constants.RESOURCE_FILE_SEPARATOR + Constants.SIARD_METADATA_FILE
      + Constants.XML_EXTENSION;
  }

  @Override
  public String getMetadataXSDPath() {
    return Constants.SIARD_HEADER_FOLDER + Constants.RESOURCE_FILE_SEPARATOR + Constants.SIARD_METADATA_FILE
      + Constants.XSD_EXTENSION;
  }

  @Override
  public void registerSchema(String schemaName, String schemaFolder) {
    if (schemas == null) {
      schemas = new HashMap<>();
    }
    schemas.put(schemaName, schemaFolder);
  }

  @Override
  public void registerTable(String schemaName, String tableName, String tableFolder) {
    if (tables == null) {
      tables = new HashMap<>();
    }
    if (tables.get(schemaName) != null) {
      tables.get(schemaName).put(tableName, tableFolder);
    } else {
      Map<String, String> map = new HashMap<>();
      map.put(tableName, tableFolder);
      tables.put(schemaName, map);
    }
  }

  @Override
  public String getSchemaName(String schemaFolder) {
    for (Map.Entry<String, String> entry : schemas.entrySet()) {
      if (entry.getValue().equals(schemaFolder))
        return entry.getKey();
    }
    return null;
  }

  @Override
  public String getTableName(String schemaFolder, String tableFolder) {
    final String schemaName = getSchemaName(schemaFolder);
    if (tables.get(schemaName) != null) {
      for (Map.Entry<String, String> entry : tables.get(schemaName).entrySet()) {
        if (entry.getValue().equals(tableFolder))
          return entry.getKey();
      }
    }

    return null;
  }

  @Override
  public String getSchemaFolder(String schemaName) {
    return schemas.get(schemaName);
  }

  @Override
  public String getTableFolder(String schemaName, String tableName) {
    return tables.get(schemaName).get(tableName);
  }

  @Override
  public boolean isReady() {
    return (schemas != null && tables != null);
  }

  @Override
  public String getSIARDVersionPath() {
    return Constants.SIARD_HEADER_FOLDER + Constants.RESOURCE_FILE_SEPARATOR + Constants.SIARD_VERSION_FOLDER + Constants.RESOURCE_FILE_SEPARATOR;
  }
}
