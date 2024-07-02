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
import com.databasepreservation.modules.siard.common.path.SIARDDKMetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;
import com.databasepreservation.modules.siard.out.content.LOBsTracker;
import com.databasepreservation.modules.siard.out.content.SIARDDK2010ContentExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK2010DocIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK2010FileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK2010MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDMarshaller;
import com.databasepreservation.modules.siard.out.metadata.StandardSIARDMarshaller;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARDDK2010ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.FolderWriteStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDK2010ExportModule {

  private MetadataExportStrategy metadataExportStrategy;
  private SIARDArchiveContainer mainContainer;
  private ContentExportStrategy contentExportStrategy;
  private WriteStrategy writeStrategy;
  private ContentPathExportStrategy contentPathExportStrategy;
  private MetadataPathStrategy metadataPathStrategy;
  private SIARDMarshaller siardMarshaller;
  private LOBsTracker lobsTracker;

  private Map<String, String> exportModuleArgs;
  private SIARDDK2010FileIndexFileStrategy SIARDDK2010FileIndexFileStrategy;
  private SIARDDK2010DocIndexFileStrategy SIARDDK2010DocIndexFileStrategy;

  public SIARDDK2010ExportModule(Map<String, String> exportModuleArgs) {
    this.exportModuleArgs = exportModuleArgs;

    Path rootPath = FileSystems.getDefault().getPath(exportModuleArgs.get("folder"));

    mainContainer = new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK, rootPath,
      SIARDArchiveContainer.OutputContainerType.MAIN);
    writeStrategy = new FolderWriteStrategy();
    siardMarshaller = new StandardSIARDMarshaller();
    SIARDDK2010FileIndexFileStrategy = new SIARDDK2010FileIndexFileStrategy();
    SIARDDK2010DocIndexFileStrategy = new SIARDDK2010DocIndexFileStrategy();
    lobsTracker = new LOBsTracker(Integer.parseInt(exportModuleArgs.get("lobs-per-folder")),
      Integer.parseInt(exportModuleArgs.get("lobs-folder-size")));
    contentPathExportStrategy = new SIARDDK2010ContentPathExportStrategy(this);
    metadataPathStrategy = new SIARDDKMetadataPathStrategy();
    metadataExportStrategy = new SIARDDK2010MetadataExportStrategy(this);
    contentExportStrategy = new SIARDDK2010ContentExportStrategy(this);
  }

  public DatabaseFilterModule getDatabaseExportModule() {
    return new SIARDDK2010DatabaseExportModule(this);
  }

  public Map<String, String> getExportModuleArgs() {
    return exportModuleArgs;
  }

  public WriteStrategy getWriteStrategy() {
    return writeStrategy;
  }

  public SIARDDK2010FileIndexFileStrategy getFileIndexFileStrategy() {
    return SIARDDK2010FileIndexFileStrategy;
  }

  /**
   * @return the docIndexFileStrategy
   */
  public SIARDDK2010DocIndexFileStrategy getDocIndexFileStrategy() {
    return SIARDDK2010DocIndexFileStrategy;
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
