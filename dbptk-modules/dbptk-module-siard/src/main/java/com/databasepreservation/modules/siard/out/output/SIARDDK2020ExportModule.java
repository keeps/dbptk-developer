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

import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARDDKMetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;
import com.databasepreservation.modules.siard.out.content.LOBsTracker;
import com.databasepreservation.modules.siard.out.content.SIARDDK2020ContentExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK2020DocIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK2020FileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK2020MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDMarshaller;
import com.databasepreservation.modules.siard.out.metadata.StandardSIARDMarshaller;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARDDK2020ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.FolderWriteStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDK2020ExportModule {

  private MetadataExportStrategy metadataExportStrategy;
  private SIARDArchiveContainer mainContainer;
  private ContentExportStrategy contentExportStrategy;
  private WriteStrategy writeStrategy;
  private ContentPathExportStrategy contentPathExportStrategy;
  private MetadataPathStrategy metadataPathStrategy;
  private SIARDMarshaller siardMarshaller;
  private LOBsTracker lobsTracker;

  private Map<String, String> exportModuleArgs;
  private SIARDDK2020FileIndexFileStrategy SIARDDK2020FileIndexFileStrategy;
  private SIARDDK2020DocIndexFileStrategy SIARDDK2020DocIndexFileStrategy;

  public SIARDDK2020ExportModule(Map<String, String> exportModuleArgs) {
    this.exportModuleArgs = exportModuleArgs;

    Path rootPath = FileSystems.getDefault().getPath(exportModuleArgs.get("folder"));

    mainContainer = new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK, rootPath,
      SIARDArchiveContainer.OutputContainerType.MAIN);
    writeStrategy = new FolderWriteStrategy();
    siardMarshaller = new StandardSIARDMarshaller();
    SIARDDK2020FileIndexFileStrategy = new SIARDDK2020FileIndexFileStrategy();
    SIARDDK2020DocIndexFileStrategy = new SIARDDK2020DocIndexFileStrategy();
    lobsTracker = new LOBsTracker(Integer.parseInt(exportModuleArgs.get("lobs-per-folder")),
      Integer.parseInt(exportModuleArgs.get("lobs-folder-size")));
    contentPathExportStrategy = new SIARDDK2020ContentPathExportStrategy(this);
    metadataPathStrategy = new SIARDDKMetadataPathStrategy();
    metadataExportStrategy = new SIARDDK2020MetadataExportStrategy(this);
    contentExportStrategy = new SIARDDK2020ContentExportStrategy(this);
  }

  public DatabaseFilterModule getDatabaseExportModule() {
    return new SIARDDK2020DatabaseExportModule(this);
  }

  public Map<String, String> getExportModuleArgs() {
    return exportModuleArgs;
  }

  public WriteStrategy getWriteStrategy() {
    return writeStrategy;
  }

  public SIARDDK2020FileIndexFileStrategy getFileIndexFileStrategy() {
    return SIARDDK2020FileIndexFileStrategy;
  }

  /**
   * @return the docIndexFileStrategy
   */
  public SIARDDK2020DocIndexFileStrategy getDocIndexFileStrategy() {
    return SIARDDK2020DocIndexFileStrategy;
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
