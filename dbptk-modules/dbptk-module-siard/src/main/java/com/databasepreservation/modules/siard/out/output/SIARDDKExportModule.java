package com.databasepreservation.modules.siard.out.output;

import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARDDK1007MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;
import com.databasepreservation.modules.siard.out.content.LOBsTracker;
import com.databasepreservation.modules.siard.out.content.SIARDDK1007ContentExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK1007MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDKDocIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDKFileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDMarshaller;
import com.databasepreservation.modules.siard.out.metadata.StandardSIARDMarshaller;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARDDK1007ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.FolderWriteStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map;

public abstract class SIARDDKExportModule {
  private MetadataExportStrategy metadataExportStrategy;
  private SIARDArchiveContainer mainContainer;
  private ContentExportStrategy contentExportStrategy;
  private WriteStrategy writeStrategy;
  private ContentPathExportStrategy contentPathExportStrategy;
  private MetadataPathStrategy metadataPathStrategy;
  private SIARDMarshaller siardMarshaller;
  private LOBsTracker lobsTracker;

  private Map<String, String> exportModuleArgs;
  private SIARDDKFileIndexFileStrategy SIARDDKFileIndexFileStrategy;
  private SIARDDKDocIndexFileStrategy SIARDDKDocIndexFileStrategy;

  public SIARDDKExportModule(Map<String, String> exportModuleArgs) {
    this.exportModuleArgs = exportModuleArgs;

    Path rootPath = FileSystems.getDefault().getPath(exportModuleArgs.get("folder"));

    mainContainer = new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK, rootPath,
      SIARDArchiveContainer.OutputContainerType.MAIN);
    writeStrategy = new FolderWriteStrategy();
    siardMarshaller = new StandardSIARDMarshaller();
    SIARDDKFileIndexFileStrategy = createSIARDDKFileIndexFileStrategyInstance();
    SIARDDKDocIndexFileStrategy = createSIARDDKDocIndexFileStrategyInstance();
    lobsTracker = new LOBsTracker(Integer.parseInt(exportModuleArgs.get("lobs-per-folder")),
      Integer.parseInt(exportModuleArgs.get("lobs-folder-size")));
    //contentPathExportStrategy = new SIARDDK1007ContentPathExportStrategy(this);
    metadataPathStrategy = new SIARDDK1007MetadataPathStrategy();
    //metadataExportStrategy = new SIARDDK1007MetadataExportStrategy(this);
    //contentExportStrategy = new SIARDDK1007ContentExportStrategy(this);
  }

//  public DatabaseFilterModule getDatabaseExportModule() {
//    return new SIARDDK1007DatabaseExportModule(this);
//  }

  public Map<String, String> getExportModuleArgs() {
    return exportModuleArgs;
  }

  public WriteStrategy getWriteStrategy() {
    return writeStrategy;
  }

  public SIARDDKFileIndexFileStrategy getFileIndexFileStrategy() {
    return SIARDDKFileIndexFileStrategy;
  }

  /**
   * @return the docIndexFileStrategy
   */
  public SIARDDKDocIndexFileStrategy getDocIndexFileStrategy() {
    return SIARDDKDocIndexFileStrategy;
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

  /**
   * @return the lobsTracker
   */
  public LOBsTracker getLobsTracker() {
    return lobsTracker;
  }

  abstract SIARDDKFileIndexFileStrategy createSIARDDKFileIndexFileStrategyInstance();

  abstract SIARDDKDocIndexFileStrategy createSIARDDKDocIndexFileStrategyInstance();
}
