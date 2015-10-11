/**
 * Factory for setting up SIARDDK strategies
 * 
 * @author Andreas Kring <andreas@magenta.dk>
 * 
 */

package com.databasepreservation.modules.siard.out.output;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map;

import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARDDKMetadataPathStrategy;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;
import com.databasepreservation.modules.siard.out.content.SIARDDKContentExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.FileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDKMetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDMarshaller;
import com.databasepreservation.modules.siard.out.metadata.StandardSIARDMarshaller;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARDDKContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.FolderWriteStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDKExportModule {

  private MetadataExportStrategy metadataExportStrategy;
  private SIARDArchiveContainer mainContainer;
  private ContentExportStrategy contentExportStrategy;
  private WriteStrategy writeStrategy;
  private ContentPathExportStrategy contentPathExportStrategy;
  private MetadataPathStrategy metadataPathStrategy;
  private SIARDMarshaller siardMarshaller;

  private Map<String, String> exportModuleArgs;
  private FileIndexFileStrategy fileIndexFileStrategy;

  public SIARDDKExportModule(Map<String, String> exportModuleArgs) {
    this.exportModuleArgs = exportModuleArgs;

    Path rootPath = FileSystems.getDefault().getPath(exportModuleArgs.get("folder"));

    mainContainer = new SIARDArchiveContainer(rootPath, SIARDArchiveContainer.OutputContainerType.MAIN);
    writeStrategy = new FolderWriteStrategy();
    siardMarshaller = new StandardSIARDMarshaller();
    fileIndexFileStrategy = new FileIndexFileStrategy(this);
    contentPathExportStrategy = new SIARDDKContentPathExportStrategy();
    metadataPathStrategy = new SIARDDKMetadataPathStrategy();
    metadataExportStrategy = new SIARDDKMetadataExportStrategy(this);
    contentExportStrategy = new SIARDDKContentExportStrategy(this);
  }

  public DatabaseExportModule getDatabaseExportModule() {
    return new SIARDDKDatabaseExportModule(this);
  }

  public Map<String, String> getExportModuleArgs() {
    return exportModuleArgs;
  }

  public WriteStrategy getWriteStrategy() {
    return writeStrategy;
  }

  public FileIndexFileStrategy getFileIndexFileStrategy() {
    return fileIndexFileStrategy;
  }

  public SIARDMarshaller getSiardMarshaller() {
    return siardMarshaller;
  }

  public MetadataExportStrategy getMetadataExportStrategy() {
    return metadataExportStrategy;
  }

  public MetadataPathStrategy getMetadataPathStrategy() {
    return metadataPathStrategy;
  }

  public ContentExportStrategy getContentExportStrategy() {
    return contentExportStrategy;
  }

  public ContentPathExportStrategy getContentPathExportStrategy() {
    return contentPathExportStrategy;
  }

  public SIARDArchiveContainer getMainContainer() {
    return mainContainer;
  }
}
