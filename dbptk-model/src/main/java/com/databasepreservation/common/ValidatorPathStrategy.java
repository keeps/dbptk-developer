/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.common;

import java.util.HashMap;
import java.util.Map;

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

  boolean pathExists(String schemaName, String tableName);

  String getSIARDVersionPath();
}
