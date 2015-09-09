package dk.magenta.siarddk;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

import dk.magenta.common.SIARDMarshaller;

public class SIARDDKMetadataExportStrategy implements MetadataExportStrategy {

	private WriteStrategy writeStrategy;
	private SIARDMarshaller siardMarshaller;
	private MetadataPathStrategy metadataPathStrategy;
	private FileIndexFileStrategy fileIndexFileStrategy;
	private List<String> exportModuleArgs;

	public SIARDDKMetadataExportStrategy(SIARDDKExportModule siarddkExportModule) {
		writeStrategy = siarddkExportModule.getWriteStrategy();
		siardMarshaller = siarddkExportModule.getSiardMarshaller();
		fileIndexFileStrategy = siarddkExportModule.getFileIndexFileStrategy();
		metadataPathStrategy = siarddkExportModule.getMetadataPathStrategy();
		exportModuleArgs = siarddkExportModule.getExportModuleArgs();
	}

	@Override
	public void writeMetadataXML(DatabaseStructure dbStructure,
			SIARDArchiveContainer outputContainer) throws ModuleException {

		// Generate tableIndex.xml

		try {
			IndexFileStrategy tableIndexFileStrategy = new TableIndexFileStrategy();
			String path = metadataPathStrategy.getXmlFilePath("tableIndex");
			OutputStream writer = fileIndexFileStrategy.getWriter(
					outputContainer, path);
			siardMarshaller
					.marshal(
							"dk.magenta.siarddk.tableindex",
							"/siarddk/tableIndex.xsd",
							"http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/tableIndex.xsd",
							writer,
							tableIndexFileStrategy.generateXML(dbStructure));
			writer.close();

			fileIndexFileStrategy.addFile(path);

		} catch (IOException e) {
			throw new ModuleException(
					"Error writing tableIndex.xml to the archive.", e);
		}

		
		// Generate archiveIndex.xml

		try {
			String path = metadataPathStrategy.getXmlFilePath("archiveIndex");
			OutputStream writer = fileIndexFileStrategy.getWriter(
					outputContainer, path);
			IndexFileStrategy archiveIndexFileStrategy = new ArchiveIndexFileStrategy(
					exportModuleArgs, writer);
			archiveIndexFileStrategy.generateXML(null);
			writer.close();

			fileIndexFileStrategy.addFile(path);

		} catch (IOException e) {
			throw new ModuleException(
					"Error writing archiveIndex.xml to the archive");
		}

		try {
			fileIndexFileStrategy.generateXML(null);
		} catch (ModuleException e) {
			throw new ModuleException("Error writing fileIndex.xml", e);
		}

		
		// Generate fileIndex.xml

		try {
			String path = metadataPathStrategy.getXmlFilePath("fileIndex");
			OutputStream writer = fileIndexFileStrategy.getWriter(
					outputContainer, path);
			siardMarshaller
					.marshal(
							"dk.magenta.siarddk.fileindex",
							"/siarddk/fileIndex.xsd",
							"http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/fileIndex.xsd",
							writer, fileIndexFileStrategy.generateXML(null));
			writer.close();
		} catch (IOException e) {
			throw new ModuleException(
					"Error writing fileIndex to the archive.", e);
		}
	}

	
	@Override
	public void writeMetadataXSD(DatabaseStructure dbStructure,
			SIARDArchiveContainer outputContainer) throws ModuleException {

		// Write contents to Schemas/standard
		writeSchemaFile(outputContainer, "XMLSchema.xsd");
		writeSchemaFile(outputContainer, "tableIndex.xsd");
		writeSchemaFile(outputContainer, "archiveIndex.xsd");

		// Remember to add files

	}

	private void writeSchemaFile(SIARDArchiveContainer container,
			String filename) throws ModuleException {
		InputStream inputStream = this.getClass().getResourceAsStream(
				"/siarddk/" + filename);
		OutputStream outputStream = writeStrategy.createOutputStream(container,
				"Schemas/standard/" + filename);

		try {
			IOUtils.copy(inputStream, outputStream);
			inputStream.close();
			outputStream.close();
		} catch (IOException e) {
			throw new ModuleException("There was an error writing " + filename,
					e);
		}

	}

	private DatabaseStructure generateDatabaseStructure() {

		// For testing marshaller

		// ////////////////// Create database structure //////////////////////

		ColumnStructure columnStructure = new ColumnStructure();
		columnStructure.setName("c1");
		Type type = new SimpleTypeString(20, true);
		type.setSql99TypeName("boolean"); // Giving a non-sql99 type will make
											// marshaller fail
		columnStructure.setType(type);
		List<ColumnStructure> columnList = new ArrayList<ColumnStructure>();
		columnList.add(columnStructure);
		TableStructure tableStructure = new TableStructure();
		tableStructure.setName("table1");
		tableStructure.setColumns(columnList);
		List<TableStructure> tableList = new ArrayList<TableStructure>();
		tableList.add(tableStructure);
		SchemaStructure schemaStructure = new SchemaStructure();
		schemaStructure.setTables(tableList);
		List<SchemaStructure> schemaList = new ArrayList<SchemaStructure>();
		schemaList.add(schemaStructure);
		DatabaseStructure dbStructure = new DatabaseStructure();
		dbStructure.setName("test");
		dbStructure.setSchemas(schemaList);

		return dbStructure;

		// ///////////////////////////////////////////////////////////////////

	}

}
