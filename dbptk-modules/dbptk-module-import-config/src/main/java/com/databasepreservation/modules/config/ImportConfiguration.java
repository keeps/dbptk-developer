/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.DefaultExceptionNormalizer;
import com.databasepreservation.utils.ModuleConfigurationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

/**
 * Export module that produces a list of tables contained in the database. This
 * list can then be used by other modules (e.g. the SIARD2 export module) to
 * specify the tables that should be processed.
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class ImportConfiguration implements DatabaseFilterModule {
  private DatabaseStructure dbStructure;
  private SchemaStructure currentSchema;
  private ModuleConfiguration moduleConfiguration;
  private Path outputFile;
  private ObjectMapper mapper;

  public ImportConfiguration(Path outputFile) {
    this.outputFile = outputFile;
  }

  /**
   * Initialize the database, this will be the first method called
   *
   * @throws ModuleException
   */
  @Override
  public void initDatabase() {
    mapper = new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));
    moduleConfiguration = ModuleConfigurationUtils.getDefaultModuleConfiguration();
  }

  /**
   * Set ignored schemas. Ignored schemas won't be exported. This method should be
   * called before handleStructure. However, if not called it will be assumed
   * there are not ignored schemas.
   *
   * @param ignoredSchemas
   *          the set of schemas to ignored
   */
  @Override
  public void setIgnoredSchemas(Set<String> ignoredSchemas) {
    // nothing to do here
  }

  /**
   * Handle the database structure. This method will called after
   * setIgnoredSchemas.
   *
   * @param structure
   *          the database structure
   * @throws ModuleException
   * @throws UnknownTypeException
   */
  @Override
  public void handleStructure(DatabaseStructure structure) throws ModuleException {
    if (structure == null) {
      throw new ModuleException().withMessage("Database structure must not be null");
    }

    dbStructure = structure;
  }

  /**
   * Prepare to build the data of a new schema. This method will be called after
   * handleStructure or handleDataCloseSchema.
   *
   * @param schemaName
   *          the schema name
   * @throws ModuleException
   */
  @Override
  public void handleDataOpenSchema(String schemaName) throws ModuleException {
    currentSchema = dbStructure.getSchemaByName(schemaName);

    if (currentSchema == null) {
      throw new ModuleException().withMessage("Couldn't find schema with name: " + schemaName);
    }
  }

  /**
   * Prepare to build the data of a new table. This method will be called after
   * the handleDataOpenSchema, and before some calls to handleDataRow. If there
   * are no rows in the table, then handleDataCloseTable is called after this
   * method.
   *
   * @param tableId
   *          the table id
   * @throws ModuleException
   */
  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    TableStructure currentTable = dbStructure.getTableById(tableId);
    if (currentTable == null) {
      throw new ModuleException().withMessage("Couldn't find table with id: " + tableId);
    }

    ModuleConfigurationUtils.addTableConfiguration(moduleConfiguration, currentTable);
  }

  /**
   * Handle a table row. This method will be called after the table was open and
   * before it was closed, by row index order.
   *
   * @param row
   *          the table row
   * @throws InvalidDataException
   * @throws ModuleException
   */
  @Override
  public void handleDataRow(Row row) throws ModuleException {
    // nothing to do
  }

  /**
   * Finish handling the data of a table. This method will be called after all
   * table rows for the table where requested to be handled.
   *
   * @param tableId
   *          the table id
   * @throws ModuleException
   */
  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {
    // nothing to do
  }

  /**
   * Finish handling the data of a schema. This method will be called after all
   * tables of the schema were requested to be handled.
   *
   * @param schemaName
   *          the schema name
   * @throws ModuleException
   */
  @Override
  public void handleDataCloseSchema(String schemaName) throws ModuleException {
    if (!currentSchema.getViews().isEmpty()) {
      currentSchema.getViews().forEach(
        view -> ModuleConfigurationUtils.addViewConfiguration(moduleConfiguration, view, currentSchema.getName()));
    }
  }

  /**
   * Finish the database. This method will be called when all data was requested
   * to be handled. This is the last method.
   *
   * @throws ModuleException
   */
  @Override
  public void finishDatabase() throws ModuleException {
    try {
      mapper.writeValue(new File(outputFile.toAbsolutePath().toString()), moduleConfiguration);
    } catch (IOException e) {
      throw new ModuleException()
        .withMessage("Could not close file writer stream (file: " + outputFile.toAbsolutePath().toString() + ")")
        .withCause(e);
    }
  }

  @Override
  public void updateModuleConfiguration(String moduleName, Map<String, String> properties,
    Map<String, String> remoteProperties) {
    ModuleConfigurationUtils.addImportParameters(moduleConfiguration, moduleName, properties, remoteProperties);
  }

  /**
   * Provide a reporter through which potential conversion problems should be
   * reported. This reporter should be provided only once for the export module
   * instance.
   *
   * @param reporter
   *          The initialized reporter instance.
   */
  @Override
  public void setOnceReporter(Reporter reporter) {
    // do nothing
  }

  /**
   * Import the database model.
   *
   * @param databaseExportModule The database model handler to be called when importing the database.
   * @return Return itself, to allow chaining multiple getDatabase methods
   * @throws ModuleException generic module exception
   */
  @Override
  public DatabaseFilterModule migrateDatabaseTo(DatabaseFilterModule databaseExportModule) throws ModuleException {
    return this;
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    return DefaultExceptionNormalizer.getInstance().normalizeException(exception, contextMessage);
  }
}
