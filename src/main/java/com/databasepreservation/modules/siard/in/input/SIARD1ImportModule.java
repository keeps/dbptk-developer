package com.databasepreservation.modules.siard.in.input;

import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipReadStrategy;
import com.databasepreservation.modules.siard.out.write.OutputContainer;

import java.nio.file.Path;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD1ImportModule {
	private final ReadStrategy readStrategy;
	private final OutputContainer mainContainer;

	public SIARD1ImportModule(Path siardPackage){
		readStrategy = new ZipReadStrategy();
		mainContainer = new OutputContainer(siardPackage, OutputContainer.OutputContainerType.INSIDE_ARCHIVE);
	}

	public DatabaseImportModule getDatabaseImportModule(){
		return new SIARDImportDefault(mainContainer, readStrategy);
	}
}
