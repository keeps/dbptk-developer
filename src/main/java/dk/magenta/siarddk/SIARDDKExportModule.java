/**
 * Factory for setting up SIARDDK strategies
 * 
 * @author Andreas Kring <andreas@magenta.dk>
 * 
 */

package dk.magenta.siarddk;

import java.nio.file.Path;

import com.databasepreservation.modules.DatabaseHandler;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.output.SIARDExportDefault;
import com.databasepreservation.modules.siard.out.write.FolderWriteStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

public class SIARDDKExportModule {

	private MetadataExportStrategy metadataExportStrategy;
	private SIARDArchiveContainer mainContainer;
	private ContentExportStrategy contentExportStrategy;
	private WriteStrategy writeStrategy;
	
	public SIARDDKExportModule(Path siardPackage, boolean compressZip) {
		mainContainer = new SIARDArchiveContainer(siardPackage, SIARDArchiveContainer.OutputContainerType.INSIDE_ARCHIVE);
		contentExportStrategy = new SIARDDKContentExportStrategy();
		writeStrategy = new FolderWriteStrategy();
		metadataExportStrategy = new SIARDDKMetadataExportStrategy(writeStrategy);
		
	}
	
	public DatabaseHandler getDatabaseHandler() {
		return new SIARDExportDefault(contentExportStrategy, mainContainer, writeStrategy, metadataExportStrategy);
	}
}
