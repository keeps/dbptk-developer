package com.databasepreservation.modules.siard.out.output;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.listTables.ListTables;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARDExportDefault implements DatabaseExportModule {
  private final SIARDArchiveContainer mainContainer;
  private final WriteStrategy writeStrategy;
  private final MetadataExportStrategy metadataStrategy;
  private final ContentExportStrategy contentStrategy;
  private final Path tableFilter;

  private ModuleSettings moduleSettings = null;

  private DatabaseStructure dbStructure;
  private SchemaStructure currentSchema;
  private TableStructure currentTable;

  public SIARDExportDefault(ContentExportStrategy contentStrategy, SIARDArchiveContainer mainContainer,
    WriteStrategy writeStrategy, MetadataExportStrategy metadataStrategy, Path tableFilter) {
    this.contentStrategy = contentStrategy;
    this.mainContainer = mainContainer;
    this.writeStrategy = writeStrategy;
    this.metadataStrategy = metadataStrategy;
    this.tableFilter = tableFilter;
  }

  /**
   * Gets custom settings set by the export module that modify behaviour of the
   * import module.
   *
   * @throws ModuleException
   */
  @Override
  public ModuleSettings getModuleSettings() throws ModuleException {
    if (moduleSettings != null) {
      return moduleSettings;
    }

    if (tableFilter == null) {
      moduleSettings = new ModuleSettings();
    } else {
      try {
        // attempt to get a table list from the file at tableFilter and use that
        // list as selectedTables in the ModuleSettings
        InputStream inputStream = Files.newInputStream(tableFilter);
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "UTF8");
        BufferedReader reader = new BufferedReader(inputStreamReader);

        final HashSet<Pair<String, String>> selectedTables = new HashSet<>();
        String line;
        while ((line = reader.readLine()) != null) {
          if (StringUtils.isNotBlank(line)) {
            if (StringUtils.countMatches(line, ListTables.schemaTableSeparator) != 1) {
              throw new ModuleException("Malformed entry in table list: " + line);
            }

            String[] parts = line.split(Pattern.quote(ListTables.schemaTableSeparator));
            selectedTables.add(new ImmutablePair<String, String>(parts[0], parts[1]));
          }
        }

        moduleSettings = new ModuleSettings() {
          @Override
          public Set<Pair<String, String>> selectedTables() {
            return selectedTables;
          }
        };
      } catch (IOException e) {
        throw new ModuleException("Could not read table list from file " + tableFilter.toAbsolutePath().toString(), e);
      }
    }
    return moduleSettings;
  }

  @Override
  public void initDatabase() throws ModuleException {
    writeStrategy.setup(mainContainer);
  }

  @Override
  public void setIgnoredSchemas(Set<String> ignoredSchemas) {
    // nothing to do
  }

  @Override
  public void handleStructure(DatabaseStructure structure) throws ModuleException, UnknownTypeException {
    if (structure == null) {
      throw new ModuleException("Database structure must not be null");
    }

    dbStructure = structure;
  }

  @Override
  public void handleDataOpenSchema(String schemaName) throws ModuleException {
    currentSchema = dbStructure.getSchemaByName(schemaName);

    if (currentSchema == null) {
      throw new ModuleException("Couldn't find schema with name: " + schemaName);
    }

    contentStrategy.openSchema(currentSchema);
  }

  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    currentTable = dbStructure.lookupTableStructure(tableId);

    if (currentTable == null) {
      throw new ModuleException("Couldn't find table with id: " + tableId);
    }

    contentStrategy.openTable(currentTable);
  }

  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {
    currentTable = dbStructure.lookupTableStructure(tableId);

    if (currentTable == null) {
      throw new ModuleException("Couldn't find table with id: " + tableId);
    }

    contentStrategy.closeTable(currentTable);
  }

  @Override
  public void handleDataCloseSchema(String schemaName) throws ModuleException {
    currentSchema = dbStructure.getSchemaByName(schemaName);

    if (currentSchema == null) {
      throw new ModuleException("Couldn't find schema with name: " + schemaName);
    }

    contentStrategy.closeSchema(currentSchema);
  }

  @Override
  public void handleDataRow(Row row) throws InvalidDataException, ModuleException {
    contentStrategy.tableRow(row);
  }

  @Override
  public void finishDatabase() throws ModuleException {
    metadataStrategy.writeMetadataXML(dbStructure, mainContainer, writeStrategy);
    metadataStrategy.writeMetadataXSD(dbStructure, mainContainer, writeStrategy);
    writeStrategy.finish(mainContainer);
  }
}
