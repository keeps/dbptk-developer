package com.databasepreservation.common;

import java.nio.file.Path;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public interface ValidatorPathStrategy {

  String getXMLTablePathFromFolder(String schemaFolder, String tableFolder);

  String getXMLTablePathFromName(String schemaName, String tableName);

  String getXSDTablePathFromFolder(String schemaFolder, String tableFolder);

  String getXSDTablePathFromName(String schemaName, String tableName);

  String getMetadataXMLPath();

  String getMetadataXSDPath();

  void registerSchema(String schemaName, String schemaFolder);

  void registerTable(String schemaName, String tableName, String tableFolder);

  String getSchemaName(String schemaFolder);

  String getTableName(String schemaFolder, String tableFolder);

  String getSchemaFolder(String schemaName);

  String getTableFolder(String schemaName, String tableName);

  boolean isReady();

  String getSIARDVersionPath();
}
