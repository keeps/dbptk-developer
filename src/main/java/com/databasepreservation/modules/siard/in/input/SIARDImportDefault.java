package com.databasepreservation.modules.siard.in.input;

import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.modules.DatabaseHandler;
import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

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
	public void getDatabase(DatabaseHandler databaseHandler) throws ModuleException, UnknownTypeException, InvalidDataException {
		//TODO: use code from import module
	}
}
