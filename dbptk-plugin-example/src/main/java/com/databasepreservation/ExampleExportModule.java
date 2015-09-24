package com.databasepreservation;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.structure.DatabaseStructure;

import java.util.Set;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ExampleExportModule implements DatabaseExportModule {
        private final String text;

        public ExampleExportModule(String text) {
                this.text = text;
        }

        @Override public void initDatabase() throws ModuleException {
                System.out.println("[ExampleExportModule] active = <not supported by export module>");
        }

        @Override public void finishDatabase() throws ModuleException {
                System.out.println("[ExampleExportModule] text = " + text);
        }

        @Override public void setIgnoredSchemas(Set<String> ignoredSchemas) {
                // nothing in this example
        }

        @Override public void handleStructure(DatabaseStructure structure)
          throws ModuleException, UnknownTypeException {
                // nothing in this example
        }

        @Override public void handleDataOpenSchema(String schemaName) throws ModuleException {
                // nothing in this example
        }

        @Override public void handleDataOpenTable(String tableId) throws ModuleException {
                // nothing in this example
        }

        @Override public void handleDataRow(Row row) throws InvalidDataException, ModuleException {
                // nothing in this example
        }

        @Override public void handleDataCloseTable(String tableId) throws ModuleException {
                // nothing in this example
        }

        @Override public void handleDataCloseSchema(String schemaName) throws ModuleException {
                // nothing in this example
        }
}
