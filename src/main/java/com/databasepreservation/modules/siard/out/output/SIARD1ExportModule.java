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
public class SIARD1ExportModule {
	private final PathStrategy pathStrategy;

	private final OutputContainer mainContainer;
	private final WriteStrategy writeStrategy;

	private MetadataStrategy metadataStrategy;
	private ContentStrategy contentStrategy;

	public SIARD1ExportModule(Path siardPackage, boolean compressZip) {
		pathStrategy = new SIARD1PathStrategy();
		if(compressZip){
			writeStrategy = new ZipWriteStrategy(ZipWriteStrategy.CompressionMethod.DEFLATE);
		}else{
			writeStrategy = new ZipWriteStrategy(ZipWriteStrategy.CompressionMethod.STORE);
		}
		mainContainer = new OutputContainer(siardPackage, OutputContainer.OutputContainerType.INSIDE_ARCHIVE);

		contentStrategy = new SIARD1ContentStrategy(pathStrategy, writeStrategy,mainContainer);
		metadataStrategy = new SIARD1MetadataStrategy(pathStrategy, writeStrategy);
	}
	
	public DatabaseHandler getDatabaseHandler() {
		return new SIARDExportDefault(contentStrategy, pathStrategy, mainContainer, writeStrategy, metadataStrategy);
	}
}
