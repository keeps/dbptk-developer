/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
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

import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARDDK1007MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;
import com.databasepreservation.modules.siard.out.content.LOBsTracker;
import com.databasepreservation.modules.siard.out.content.SIARDDK1007ContentExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK1007FileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK1007DocIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK1007MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDMarshaller;
import com.databasepreservation.modules.siard.out.metadata.StandardSIARDMarshaller;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARDDK1007ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.FolderWriteStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDK1007ExportModule {

  private MetadataExportStrategy metadataExportStrategy;
  private SIARDArchiveContainer mainContainer;
  private ContentExportStrategy contentExportStrategy;
  private WriteStrategy writeStrategy;
  private ContentPathExportStrategy contentPathExportStrategy;
  private MetadataPathStrategy metadataPathStrategy;
  private SIARDMarshaller siardMarshaller;
  private LOBsTracker lobsTracker;

  private Map<String, String> exportModuleArgs;
  private SIARDDK1007FileIndexFileStrategy SIARDDK1007FileIndexFileStrategy;
  private SIARDDK1007DocIndexFileStrategy SIARDDK1007DocIndexFileStrategy;

  public SIARDDK1007ExportModule(Map<String, String> exportModuleArgs) {
    this.exportModuleArgs = exportModuleArgs;

    Path rootPath = FileSystems.getDefault().getPath(exportModuleArgs.get("folder"));

    mainContainer = new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK, rootPath,
      SIARDArchiveContainer.OutputContainerType.MAIN);
    writeStrategy = new FolderWriteStrategy();
    siardMarshaller = new StandardSIARDMarshaller();
    SIARDDK1007FileIndexFileStrategy = new SIARDDK1007FileIndexFileStrategy();
    SIARDDK1007DocIndexFileStrategy = new SIARDDK1007DocIndexFileStrategy();
    lobsTracker = new LOBsTracker(Integer.parseInt(exportModuleArgs.get("lobs-per-folder")),
      Integer.parseInt(exportModuleArgs.get("lobs-folder-size")));
    contentPathExportStrategy = new SIARDDK1007ContentPathExportStrategy(this);
    metadataPathStrategy = new SIARDDK1007MetadataPathStrategy();
    metadataExportStrategy = new SIARDDK1007MetadataExportStrategy(this);
    contentExportStrategy = new SIARDDK1007ContentExportStrategy(this);
  }

  public DatabaseFilterModule getDatabaseExportModule() {
    return new SIARDDK1007DatabaseExportModule(this);
  }

  public Map<String, String> getExportModuleArgs() {
    return exportModuleArgs;
  }

  public WriteStrategy getWriteStrategy() {
    return writeStrategy;
  }

  public SIARDDK1007FileIndexFileStrategy getFileIndexFileStrategy() {
    return SIARDDK1007FileIndexFileStrategy;
  }

  /**
   * @return the docIndexFileStrategy
   */
  public SIARDDK1007DocIndexFileStrategy getDocIndexFileStrategy() {
    return SIARDDK1007DocIndexFileStrategy;
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
}
