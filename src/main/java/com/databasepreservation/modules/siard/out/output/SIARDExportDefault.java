package com.databasepreservation.modules.siard.out.output;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.DatabaseExportModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

import java.util.Set;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARDExportDefault implements DatabaseExportModule {
  private final SIARDArchiveContainer mainContainer;
  private final WriteStrategy writeStrategy;
  private final MetadataExportStrategy metadataStrategy;
  private final ContentExportStrategy contentStrategy;

  private DatabaseStructure dbStructure;
  private SchemaStructure currentSchema;
  private TableStructure currentTable;

  public SIARDExportDefault(ContentExportStrategy contentStrategy, SIARDArchiveContainer mainContainer,
    WriteStrategy writeStrategy, MetadataExportStrategy metadataStrategy) {
    this.contentStrategy = contentStrategy;
    this.mainContainer = mainContainer;
    this.writeStrategy = writeStrategy;
    this.metadataStrategy = metadataStrategy;
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
