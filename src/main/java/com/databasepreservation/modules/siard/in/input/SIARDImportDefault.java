package com.databasepreservation.modules.siard.in.input;

import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.modules.DatabaseHandler;
import com.databasepreservation.modules.DatabaseImportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARDImportDefault implements DatabaseImportModule {

	@Override
	public void getDatabase(DatabaseHandler databaseHandler) throws ModuleException, UnknownTypeException, InvalidDataException {

	}
}
