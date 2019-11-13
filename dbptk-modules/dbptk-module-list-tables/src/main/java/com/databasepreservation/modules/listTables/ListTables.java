/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.listTables;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.regex.Pattern;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.modules.DefaultExceptionNormalizer;

/**
 * Export module that produces a list of tables contained in the database. This
 * list can then be used by other modules (e.g. the SIARD2 export module) to
 * specify the tables that should be processed.
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ListTables implements DatabaseExportModule {
  public static final String SCHEMA_TABLE_SEPARATOR = ".";
  public static final String COLUMNS_START = "{";
  public static final String COLUMNS_END = "}";
  public static final String COLUMNS_SEPARATOR = ";";

  /**
   * Pattern that should be followed by all lines in the table list file. DEV
   * NOTE: Depends on the separators declared above, keep them in sync.
   *
   * Examples:
   * 
   * <code>
   *     schema.table{col1}
   *     schema.table{col1;col2}
   *     schema.table{col1;col2;col3}
   *     schema.table{col1;;col3}
   *     schema.table{col1;;col3;;}
   * </code>
   */
  public static final Pattern LINE_PATTERN = Pattern
    .compile("^([^\\.\\{\\}\\;]+)((?:\\.(?:[^\\.\\{\\}\\;]+))+)\\{((?:[^\\s][^\\.\\{\\}\\;]*\\;)*[^\\s\\.\\{\\}\\;]*)\\}$");

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

      @Override
      public boolean shouldCountRows() {
        return false;
      }

      @Override
      public boolean fetchMetadataInformation() {
        return false;
      }

      @Override
      public boolean fetchWithViewAsTable() { return false; }
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
      out = new OutputStreamWriter(outStream, StandardCharsets.UTF_8);

    } catch (IOException e) {
      throw new ModuleException().withMessage("Could not create file " + outputFile.toAbsolutePath().toString())
        .withCause(e);
    }
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
    try {
      currentTable = dbStructure.getTableById(tableId);
      if (currentTable == null) {
        throw new ModuleException().withMessage("Couldn't find table with id: " + tableId);
      }

      out.append(currentSchema.getName()).append(SCHEMA_TABLE_SEPARATOR).append(currentTable.getName())
        .append(COLUMNS_START);

      for (ColumnStructure column : currentTable.getColumns()) {
        out.append(column.getName()).append(COLUMNS_SEPARATOR);
      }

      out.append(COLUMNS_END).append("\n");
    } catch (IOException e) {
      throw new ModuleException()
        .withMessage("Could not write to file (" + outputFile.toAbsolutePath().toString() + ")").withCause(e);
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
    // nothing to do
    // Add the views for this schema
    try {
      if (currentSchema == null) {
        throw new ModuleException().withMessage("Couldn't find schema with name: " + schemaName);
      }

      for (ViewStructure view : currentSchema.getViews()) {
        out.append(currentSchema.getName()).append(SCHEMA_TABLE_SEPARATOR).append(view.getName()).append(COLUMNS_START);

        for (ColumnStructure column : view.getColumns()) {
          out.append(column.getName()).append(COLUMNS_SEPARATOR);
        }

        out.append(COLUMNS_END).append("\n");
      }
    } catch (IOException e) {
      throw new ModuleException()
          .withMessage("Could not write to file (" + outputFile.toAbsolutePath().toString() + ")").withCause(e);
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
      out.close();
    } catch (IOException e) {
      throw new ModuleException()
        .withMessage("Could not close file writer stream (file: " + outputFile.toAbsolutePath().toString() + ")")
        .withCause(e);
    }

    try {
      outStream.close();
    } catch (IOException e) {
      throw new ModuleException()
        .withMessage("Could not close file stream (file: " + outputFile.toAbsolutePath().toString() + ")").withCause(e);
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

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    return DefaultExceptionNormalizer.getInstance().normalizeException(exception, contextMessage);
  }
}
