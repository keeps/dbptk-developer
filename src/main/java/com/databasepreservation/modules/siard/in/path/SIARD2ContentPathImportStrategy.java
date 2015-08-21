package com.databasepreservation.modules.siard.in.path;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ContentPathImportStrategy implements ContentPathImportStrategy {
        public SIARD2ContentPathImportStrategy() {

        }

        public void associateSchemaWithFolder(String schemaName, String schemaFolder) {

        }

        public void associateTableWithFolder(String tableName, String tableFolder) {

        }

        public void associateColumnWithFolder(String columnId, String columnFolder) {

        }

        @Override public String getTableXMLFilePath(String schemaName, String tableId) throws ModuleException {
                return null;
        }

        @Override public String getTableXSDFilePath(String schemaName, String tableId) throws ModuleException {
                return null;
        }
}
