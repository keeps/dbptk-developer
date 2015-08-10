package com.databasepreservation.modules.siard.in.input;

import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.modules.DatabaseHandler;
import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARDImportDefault implements DatabaseImportModule {
	private final ReadStrategy readStrategy;
	private final SIARDArchiveContainer mainContainer;

	public SIARDImportDefault(SIARDArchiveContainer mainContainer, ReadStrategy readStrategy) {
		this.readStrategy = readStrategy;
		this.mainContainer = mainContainer;
	}

	@Override
	public void getDatabase(DatabaseHandler databaseHandler) throws ModuleException, UnknownTypeException, InvalidDataException {
		readStrategy.setup(mainContainer);
		System.out.println(readStrategy.listFiles(mainContainer, ""));
	}
}
