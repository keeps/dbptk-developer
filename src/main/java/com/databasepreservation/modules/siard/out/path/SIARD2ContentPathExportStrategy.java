package com.databasepreservation.modules.siard.out.path;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ContentPathExportStrategy implements ContentPathExportStrategy {
        @Override public String getClobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {
                return null;
        }

        @Override public String getBlobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {
                return null;
        }

        @Override public String getSchemaFolderName(int schemaIndex) {
                return null;
        }

        @Override public String getTableFolderName(int tableIndex) {
                return null;
        }

        @Override public String getTableXsdFilePath(int schemaIndex, int tableIndex) {
                return null;
        }

        @Override public String getTableXmlFilePath(int schemaIndex, int tableIndex) {
                return null;
        }

        @Override public String getTableXsdNamespace(String base, int schemaIndex, int tableIndex) {
                return null;
        }

        @Override public String getTableXsdFileName(int tableIndex) {
                return null;
        }
}
