/**
 *
 */
package com.databasepreservation.model.modules;

import java.util.Set;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.DatabaseStructure;

/**
 * @author Luis Faria
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface DatabaseExportModule {
  /**
   * Gets custom settings set by the export module that modify behaviour of the
   * import module.
   * 
   * @throws ModuleException
   */
  ModuleSettings getModuleSettings() throws ModuleException;

  /**
   * Initialize the database, this will be the first method called
   *
   * @throws ModuleException
   */
  void initDatabase() throws ModuleException;

  /**
   * Set ignored schemas. Ignored schemas won't be exported. This method should
   * be called before handleStructure. However, if not called it will be assumed
   * there are not ignored schemas.
   *
   * @param ignoredSchemas
   *          the set of schemas to ignored
   */
  void setIgnoredSchemas(Set<String> ignoredSchemas);

  /**
   * Handle the database structure. This method will called after
   * setIgnoredSchemas.
   *
   * @param structure
   *          the database structure
   * @throws ModuleException
   * @throws UnknownTypeException
   */
  void handleStructure(DatabaseStructure structure) throws ModuleException, UnknownTypeException;

  /**
   * Prepare to handle the data of a new schema. This method will be called
   * after handleStructure or handleDataCloseSchema.
   *
   * @param schemaName
   *          the schema name
   * @throws ModuleException
   */
  void handleDataOpenSchema(String schemaName) throws ModuleException;

  /**
   * Prepare to handle the data of a new table. This method will be called after
   * the handleDataOpenSchema, and before some calls to handleDataRow. If there
   * are no rows in the table, then handleDataCloseTable is called after this
   * method.
   *
   * @param tableId
   *          the table id
   * @throws ModuleException
   */
  void handleDataOpenTable(String tableId) throws ModuleException;

  /**
   * Handle a table row. This method will be called after the table was open and
   * before it was closed, by row index order.
   *
   * @param row
   *          the table row
   * @throws InvalidDataException
   * @throws ModuleException
   */
  void handleDataRow(Row row) throws InvalidDataException, ModuleException;

  /**
   * Finish handling the data of a table. This method will be called after all
   * table rows for the table where requested to be handled.
   *
   * @param tableId
   *          the table id
   * @throws ModuleException
   */
  void handleDataCloseTable(String tableId) throws ModuleException;

  /**
   * Finish handling the data of a schema. This method will be called after all
   * tables of the schema were requested to be handled.
   *
   * @param schemaName
   *          the schema name
   * @throws ModuleException
   */
  void handleDataCloseSchema(String schemaName) throws ModuleException;

  /**
   * Finish the database. This method will be called when all data was requested
   * to be handled. This is the last method.
   *
   * @throws ModuleException
   */
  void finishDatabase() throws ModuleException;
}
