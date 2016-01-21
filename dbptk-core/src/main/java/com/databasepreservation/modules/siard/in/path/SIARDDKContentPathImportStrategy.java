package com.databasepreservation.modules.siard.in.path;

import com.databasepreservation.model.exception.ModuleException;

//TODO: Implement this!
/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 */
public class SIARDDKContentPathImportStrategy implements ContentPathImportStrategy {

  @Override
  public String getLobPath(String basePath, String schemaName, String tableId, String columnId, String lobFileName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void associateSchemaWithFolder(String schemaName, String schemaFolder) {
    // TODO Auto-generated method stub

  }

  @Override
  public void associateTableWithFolder(String tableId, String tableFolder) {
    // TODO Auto-generated method stub

  }

  @Override
  public void associateColumnWithFolder(String columnId, String columnFolder) {
    // TODO Auto-generated method stub

  }

  @Override
  public String getTableXMLFilePath(String schemaName, String tableId) throws ModuleException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getTableXSDFilePath(String schemaName, String tableId) throws ModuleException {
    // TODO Auto-generated method stub
    return null;
  }

}
