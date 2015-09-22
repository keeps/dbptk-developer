/**
 *
 */
package com.databasepreservation.model.modules;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.DatabaseStructure;

import java.util.Set;

/**
 * @author Luis Faria
 */
public interface DatabaseExportModule {
        /**
         * Initialize the database, this will be the first method called
         *
         * @throws ModuleException
         */
        public void initDatabase() throws ModuleException;

        /**
         * Set ignored schemas. Ignored schemas won't be exported.
         * This method should be called before handleStructure.
         * However, if not called it will be assumed there are not ignored schemas.
         *
         * @param ignoredSchemas the set of schemas to ignored
         */
        public void setIgnoredSchemas(Set<String> ignoredSchemas);

        /**
         * Handle the database structure.
         * This method will called after setIgnoredSchemas.
         *
         * @param structure the database structure
         * @throws ModuleException
         * @throws UnknownTypeException
         */
        public void handleStructure(DatabaseStructure structure) throws ModuleException, UnknownTypeException;

        /**
         * Prepare to handle the data of a new schema. This method will be called
         * after handleStructure or handleDataCloseSchema.
         *
         * @param schemaName the schema name
         * @throws ModuleException
         */
        public void handleDataOpenSchema(String schemaName) throws ModuleException;

        /**
         * Prepare to handle the data of a new table. This method will be called
         * after the handleDataOpenSchema, and before some calls to handleDataRow.
         * If there are no rows in the table, then handleDataCloseTable is called
         * after this method.
         *
         * @param tableId the table id
         * @throws ModuleException
         */
        public void handleDataOpenTable(String tableId) throws ModuleException;

        /**
         * Handle a table row. This method will be called after the table was open
         * and before it was closed, by row index order.
         *
         * @param row the table row
         * @throws InvalidDataException
         * @throws ModuleException
         */
        public void handleDataRow(Row row) throws InvalidDataException, ModuleException;

        /**
         * Finish handling the data of a table. This method will be called after all
         * table rows for the table where requested to be handled.
         *
         * @param tableId the table id
         * @throws ModuleException
         */
        public void handleDataCloseTable(String tableId) throws ModuleException;

        /**
         * Finish handling the data of a schema. This method will be called after all
         * tables of the schema were requested to be handled.
         *
         * @param schemaName the schema name
         * @throws ModuleException
         */
        public void handleDataCloseSchema(String schemaName) throws ModuleException;

        /**
         * Finish the database. This method will be called when all data was
         * requested to be handled. This is the last method.
         *
         * @throws ModuleException
         */
        public void finishDatabase() throws ModuleException;
}
