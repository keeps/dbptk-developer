package com.databasepreservation.modules.siard.in.input;

import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.DatabaseHandler;
import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD1MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARDImportDefault implements DatabaseImportModule {
	private final ReadStrategy readStrategy;
	private final SIARDArchiveContainer mainContainer;
	private final ContentImportStrategy contentStrategy;
	private final MetadataImportStrategy metadataStrategy;

	public SIARDImportDefault(ContentImportStrategy contentStrategy, SIARDArchiveContainer mainContainer,
							  ReadStrategy readStrategy, MetadataImportStrategy metadataStrategy) {
		this.readStrategy = readStrategy;
		this.mainContainer = mainContainer;
		this.contentStrategy = contentStrategy;
		this.metadataStrategy = metadataStrategy;
	}

	@Override
	public void getDatabase(DatabaseHandler handler) throws ModuleException, UnknownTypeException, InvalidDataException {
		handler.initDatabase();
		try {


			if (!validateSchema()) {
				throw new ModuleException("Schema is not valid!");
			}
			setHeader();
			saxParser.parse(header, siardHeaderSAXHandler);
			if (siardHeaderSAXHandler.getErrors().size() > 0) {
				throw new ModuleException(siardHeaderSAXHandler.getErrors());
			}
			header.close();

			dbStructure = siardHeaderSAXHandler.getDatabaseStructure();
			for (SchemaStructure schema : dbStructure.getSchemas()) {
				for (TableStructure table : schema.getTables()) {
					setCurrentInputStream(
							schema, siardHeaderSAXHandler.schemaFolders.get(schema.getName()),
							table, siardHeaderSAXHandler.tableFolders.get(table.getId()));
					// TODO siardContentSAXHandler.setCurrentSchema(schema)
					siardContentSAXHandler.setCurrentTable(table);
					saxParser.parse(currentInputStream, siardContentSAXHandler);
					if (siardContentSAXHandler.getErrors().size() > 0) {
						throw new ModuleException(
								siardHeaderSAXHandler.getErrors());
					}
					currentInputStream.close();
				}
			}
			zipFile.close();
			handler.finishDatabase();

		} catch (SAXException e) {
			throw new ModuleException(
					"An error occurred while importing SIARD", e);
		} catch (IOException e) {
			throw new ModuleException("Error reading SIARD", e);
		}
	}
}
