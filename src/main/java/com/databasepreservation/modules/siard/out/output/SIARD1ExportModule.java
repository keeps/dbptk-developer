package com.databasepreservation.modules.siard.out.output;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.DatabaseHandler;
import com.databasepreservation.modules.siard.out.content.ContentStrategy;
import com.databasepreservation.modules.siard.out.content.SIARD1ContentStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARD1MetadataStrategy;
import com.databasepreservation.modules.siard.out.path.PathStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD1PathStrategy;
import com.databasepreservation.modules.siard.out.write.OutputContainer;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.modules.siard.out.write.ZipWriteStrategy;

import java.nio.file.Path;
import java.util.Set;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD1ExportModule implements DatabaseHandler {
	private final PathStrategy pathStrategy = new SIARD1PathStrategy();

	private final OutputContainer mainContainer;
	private final WriteStrategy writeStrategy;

	private MetadataStrategy metadataStrategy;
	private ContentStrategy contentStrategy;

	private DatabaseStructure dbStructure;
	private SchemaStructure currentSchema;
	private TableStructure currentTable;

	public SIARD1ExportModule(Path siardPackage, boolean compressZip) {
		if(compressZip){
			writeStrategy = new ZipWriteStrategy(ZipWriteStrategy.CompressionMethod.DEFLATE);
		}else{
			writeStrategy = new ZipWriteStrategy(ZipWriteStrategy.CompressionMethod.STORE);
		}
		//writeStrategy = new FolderWriteStrategy();
		mainContainer = new OutputContainer(siardPackage, OutputContainer.OutputContainerType.INSIDE_ARCHIVE);

		contentStrategy = new SIARD1ContentStrategy(pathStrategy, writeStrategy,mainContainer);
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
		metadataStrategy = new SIARD1MetadataStrategy(dbStructure, pathStrategy, writeStrategy);
	}

	@Override
	public void handleDataOpenTable(String schemaName, String tableId) throws ModuleException {
		currentSchema = dbStructure.getSchemaByName(schemaName);
		currentTable = dbStructure.lookupTableStructure(tableId);

		if (currentSchema == null) {
			throw new ModuleException("Couldn't find schema with name: " + schemaName);
		}

		if (currentTable == null) {
			throw new ModuleException("Couldn't find table with id: " + tableId);
		}

		contentStrategy.openTable(currentSchema, currentTable);
	}

	@Override
	public void handleDataCloseTable(String schemaName, String tableId) throws ModuleException {
		currentSchema = dbStructure.getSchemaByName(schemaName);
		currentTable = dbStructure.lookupTableStructure(tableId);

		if (currentSchema == null) {
			throw new ModuleException("Couldn't find schema with name: " + schemaName);
		}

		if (currentTable == null) {
			throw new ModuleException("Couldn't find table with id: " + tableId);
		}

		contentStrategy.closeTable(currentSchema, currentTable);
	}

	@Override
	public void handleDataRow(Row row) throws InvalidDataException, ModuleException {
		contentStrategy.tableRow(row);
	}

	@Override
	public void finishDatabase() throws ModuleException {
		metadataStrategy.writeMetadataXML(mainContainer);
		metadataStrategy.writeMetadataXSD(mainContainer);
		writeStrategy.finish(mainContainer);
	}
}
