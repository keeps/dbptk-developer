package com.databasepreservation.modules.listTables;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;

/**
 * Export module that produces a list of tables contained in the database. This
 * list can then be used by other modules (e.g. the SIARD2 export module) to
 * specify the tables that should be processed.
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ListTables implements DatabaseExportModule {
  public static final String schemaTableSeparator = ".";

  private DatabaseStructure dbStructure;
  private SchemaStructure currentSchema;
  private TableStructure currentTable;
  private Path outputFile;
  private OutputStream outStream;
  private Writer out;
  private Reporter reporter;

  public ListTables(Path outputFile) {
    this.outputFile = outputFile;
  }

  /**
   * Gets custom settings set by the export module that modify behaviour of the
   * import module.
   *
   * @throws ModuleException
   */
  @Override
  public ModuleSettings getModuleSettings() throws ModuleException {
    return new ModuleSettings() {
      @Override
      public boolean shouldFetchRows() {
        return false;
      }
    };
  }

  /**
   * Initialize the database, this will be the first method called
   *
   * @throws ModuleException
   */
  @Override
  public void initDatabase() throws ModuleException {
    try {
      outStream = Files.newOutputStream(outputFile);
      out = new OutputStreamWriter(outStream, "UTF8");

    } catch (IOException e) {
      throw new ModuleException("Could not create file " + outputFile.toAbsolutePath().toString(), e);
    }
  }

  /**
   * Set ignored schemas. Ignored schemas won't be exported. This method should
   * be called before handleStructure. However, if not called it will be assumed
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
  public void handleStructure(DatabaseStructure structure) throws ModuleException, UnknownTypeException {
    if (structure == null) {
      throw new ModuleException("Database structure must not be null");
    }

    dbStructure = structure;
  }

  /**
   * Prepare to handle the data of a new schema. This method will be called
   * after handleStructure or handleDataCloseSchema.
   *
   * @param schemaName
   *          the schema name
   * @throws ModuleException
   */
  @Override
  public void handleDataOpenSchema(String schemaName) throws ModuleException {
    currentSchema = dbStructure.getSchemaByName(schemaName);

    if (currentSchema == null) {
      throw new ModuleException("Couldn't find schema with name: " + schemaName);
    }
  }

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
  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    try {
      currentTable = dbStructure.lookupTableStructure(tableId);
      if (currentTable == null) {
        throw new ModuleException("Couldn't find table with id: " + tableId);
      }

      out.append(currentSchema.getName()).append(schemaTableSeparator).append(currentTable.getName()).append("\n");
    } catch (IOException e) {
      throw new ModuleException("Could not write to file (" + outputFile.toAbsolutePath().toString() + ")", e);
    }
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
  public void handleDataRow(Row row) throws InvalidDataException, ModuleException {
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
    // nothing to do
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
      out.close();
    } catch (IOException e) {
      throw new ModuleException("Could not close file writer stream (file: " + outputFile.toAbsolutePath().toString()
        + ")", e);
    }

    try {
      outStream.close();
    } catch (IOException e) {
      throw new ModuleException("Could not close file stream (file: " + outputFile.toAbsolutePath().toString() + ")", e);
    }
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
    this.reporter = reporter;
  }
}
