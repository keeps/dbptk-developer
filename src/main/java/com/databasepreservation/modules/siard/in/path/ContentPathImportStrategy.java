package com.databasepreservation.modules.siard.in.path;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ContentPathImportStrategy {
	public String getTableXMLFilePath(String schemaName, String tableId) throws ModuleException;
	public String getTableXSDFilePath(String schemaName, String tableId) throws ModuleException;
}
