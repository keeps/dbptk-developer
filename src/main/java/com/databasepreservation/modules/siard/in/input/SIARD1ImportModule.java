package com.databasepreservation.modules.siard.in.input;

import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipReadStrategy;

import java.nio.file.Path;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD1ImportModule {
	private final ReadStrategy readStrategy;
	private final SIARDArchiveContainer mainContainer;

	public SIARD1ImportModule(Path siardPackage){
		readStrategy = new ZipReadStrategy();
		mainContainer = new SIARDArchiveContainer(siardPackage, SIARDArchiveContainer.OutputContainerType.INSIDE_ARCHIVE);
	}

	public DatabaseImportModule getDatabaseImportModule(){
		return new SIARDImportDefault(mainContainer, readStrategy);
	}
}
