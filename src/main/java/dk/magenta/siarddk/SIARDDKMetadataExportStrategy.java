package dk.magenta.siarddk;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.io.IOUtils;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

import dk.magenta.common.SIARDMarshaller;

public class SIARDDKMetadataExportStrategy implements MetadataExportStrategy {

	private WriteStrategy writeStrategy;
	private SIARDMarshaller siardMarshaller;
	private SIARDDKExportModule siarddkExportModule;

	public SIARDDKMetadataExportStrategy(WriteStrategy writeStrategy,
			SIARDMarshaller siardMarshaller,
			SIARDDKExportModule siarddkExportModule) {
		this.writeStrategy = writeStrategy;
		this.siardMarshaller = siardMarshaller;
		this.siarddkExportModule = siarddkExportModule;
	}

	@Override
	public void writeMetadataXML(DatabaseStructure dbStructure,
			SIARDArchiveContainer outputContainer) throws ModuleException {

		// Get FileIndexFileStrategy from SIARDDKExportModule
		// This should be done before writing any files, since the files need 
		// to be added to fileIndex.xml
		
		FileIndexFileStrategy fileIndexFileStrategy = siarddkExportModule.getFileIndexFileStrategy();
		
		
		// Generate tableIndex.xml

//		try {
//			IndexFileStrategy tableIndexFileStrategy = new TableIndexFileStrategy();
//			OutputStream writer = writeStrategy.createOutputStream(
//					outputContainer, "Indices/tableIndex.xml");
//			siardMarshaller
//					.marshal(
//							"dk.magenta.siarddk.tableindex",
//							"/siarddk/tableIndex.xsd",
//							"http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/tableIndex.xsd",
//							writer,
//							tableIndexFileStrategy.generateXML(dbStructure));
//			writer.close();
//		} catch (IOException e) {
//			throw new ModuleException(
//					"Error writing tableIndex.xml to the archive.", e);
//		}

		try {
			IndexFileStrategy tableIndexFileStrategy = new TableIndexFileStrategy();
			String path = "Indices/tableIndex.xml";
			OutputStream writer = fileIndexFileStrategy.getWriter(outputContainer, path);
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
			String path = "Indices/archiveIndex.xml";
			OutputStream writer = fileIndexFileStrategy.getWriter(outputContainer, path);
			IndexFileStrategy archiveIndexFileStrategy = new ArchiveIndexFileStrategy(
					siarddkExportModule, writer);
			archiveIndexFileStrategy.generateXML(null);
			writer.close();
			
			fileIndexFileStrategy.addFile(path);
			
		} catch (IOException e) {
			throw new ModuleException("Error writing archiveIndex.xml to the archive");
		}
		
		try {
			fileIndexFileStrategy.generateXML(null);
		} catch (ModuleException e) {
			throw new ModuleException("Error writing fileIndex.xml", e);
		}
	}

	@Override
	public void writeMetadataXSD(DatabaseStructure dbStructure,
			SIARDArchiveContainer outputContainer) throws ModuleException {

		// Write contents to Schemas/standard
		writeSchemaFile(outputContainer, "XMLSchema.xsd");
		writeSchemaFile(outputContainer, "tableIndex.xsd");
		writeSchemaFile(outputContainer, "archiveIndex.xsd");

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
