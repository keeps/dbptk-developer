package com.databasepreservation.modules.dbvtk;

import java.nio.file.Path;

import org.apache.commons.configuration.SystemConfiguration;

import com.databasepreservation.visualization.utils.ViewerAbstractConfiguration;

/**
 * Singleton configuration instance used by the DBPTK Solr Export Module
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class DbvtkModuleConfiguration extends ViewerAbstractConfiguration {
  private final Path lobFolder;

  /**
   * Private constructor, use getInstance() instead
   */
  private DbvtkModuleConfiguration(Path lobFolder) {
    super(new SystemConfiguration());
    this.lobFolder = lobFolder;
  }

  /*
   * Singleton instance
   * ____________________________________________________________________________________________________________________
   */
  private static DbvtkModuleConfiguration instance = null;

  public static DbvtkModuleConfiguration getInstance(Path lobFolder) {
    if (instance == null) {
      instance = new DbvtkModuleConfiguration(lobFolder);
    }
    return instance;
  }

  /*
   * Implementation-dependent parts
   * ____________________________________________________________________________________________________________________
   */
  @Override
  public void clearViewerCachableObjectsAfterConfigurationChange() {
    // never actually called via DBPTK, only from DBVTK which uses a different
    // implementation of ViewerAbstractConfiguration
  }

  @Override
  public Path getLobPath() {
    return lobFolder;
  }
}
