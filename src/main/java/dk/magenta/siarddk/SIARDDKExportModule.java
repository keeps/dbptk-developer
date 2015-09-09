/**
 * Factory for setting up SIARDDK strategies
 * 
 * @author Andreas Kring <andreas@magenta.dk>
 * 
 */

package dk.magenta.siarddk;

import java.nio.file.Path;
import java.util.List;

import com.databasepreservation.modules.DatabaseHandler;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.output.SIARDExportDefault;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.FolderWriteStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

import dk.magenta.common.StandardSIARDMarshaller;

public class SIARDDKExportModule {

	private MetadataExportStrategy metadataExportStrategy;
	private SIARDArchiveContainer mainContainer;
	private ContentExportStrategy contentExportStrategy;
	private WriteStrategy writeStrategy;
	private ContentPathExportStrategy contentPathExportStrategy;
	private List<String> exportModuleArgs;
	private FileIndexFileStrategy fileIndexFileStrategy;
	private Path siardPackage;
	
	public SIARDDKExportModule(Path siardPackage, List<String> exportModuleArgs) {
		mainContainer = new SIARDArchiveContainer(siardPackage, SIARDArchiveContainer.OutputContainerType.INSIDE_ARCHIVE);
		writeStrategy = new FolderWriteStrategy();
		fileIndexFileStrategy = new FileIndexFileStrategy(writeStrategy);
		contentPathExportStrategy = new SIARDDKContentExportPathStrategy();
		metadataExportStrategy = new SIARDDKMetadataExportStrategy(writeStrategy, new StandardSIARDMarshaller(), this);
		contentExportStrategy = new SIARDDKContentExportStrategy(contentPathExportStrategy, writeStrategy, mainContainer);
		
		this.exportModuleArgs = exportModuleArgs;
		this.siardPackage = siardPackage;
	}
	
	public DatabaseHandler getDatabaseHandler() {
		return new SIARDExportDefault(contentExportStrategy, mainContainer, writeStrategy, metadataExportStrategy);
	}
	
	public List<String> getExportModuleArgs() {
		return exportModuleArgs;
	}

	public FileIndexFileStrategy getFileIndexFileStrategy() {
		return fileIndexFileStrategy;
	}
	
	public Path getSiardPackage() {
		return siardPackage;
	}
}
