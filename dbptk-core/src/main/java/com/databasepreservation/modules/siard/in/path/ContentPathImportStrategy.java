package com.databasepreservation.modules.siard.in.path;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ContentPathImportStrategy {
  String getLobPath(String basePath, String schemaName, String tableId, String columnId, String lobFileName);

  void associateSchemaWithFolder(String schemaName, String schemaFolder);

  void associateTableWithFolder(String tableId, String tableFolder);

  void associateColumnWithFolder(String columnId, String columnFolder);

  String getTableXMLFilePath(String schemaName, String tableId) throws ModuleException;

  String getTableXSDFilePath(String schemaName, String tableId) throws ModuleException;
}
