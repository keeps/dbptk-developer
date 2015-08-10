package com.databasepreservation.modules.siard.in.input;

import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD1MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARD1ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipReadStrategy;

import java.nio.file.Path;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD1ImportModule {
	private final ReadStrategy readStrategy;
	private final SIARDArchiveContainer mainContainer;

	private final MetadataPathStrategy metadataPathStrategy;
	private final MetadataImportStrategy metadataStrategy;

	private final ContentPathImportStrategy contentPathStrategy;
	private final ContentImportStrategy contentStrategy;

	public SIARD1ImportModule(Path siardPackage){
		readStrategy = new ZipReadStrategy();
		mainContainer = new SIARDArchiveContainer(siardPackage, SIARDArchiveContainer.OutputContainerType.INSIDE_ARCHIVE);

		metadataPathStrategy = new SIARD1MetadataPathStrategy();
		metadataStrategy = null; //TODO

		contentPathStrategy = new SIARD1ContentPathImportStrategy();
		contentStrategy = null; //TODO
	}

	public DatabaseImportModule getDatabaseImportModule(){
		return new SIARDImportDefault(contentStrategy, mainContainer, readStrategy, metadataStrategy);
	}
}
