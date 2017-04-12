package com.databasepreservation.modules.solr;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.visualization.ViewerConstants;
import com.databasepreservation.visualization.utils.ViewerAbstractConfiguration;

/**
 * Singleton configuration instance used by the DBPTK Solr Export Module
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SolrModuleConfiguration extends ViewerAbstractConfiguration {
  private final Path moduleDirectory;
  private Path workspaceDirectory = null;
  private Path workspaceForLobs = null;

  /**
   * Private constructor, use getInstance() instead
   */
  private SolrModuleConfiguration(Path moduleDirectory) {
    super(new SystemConfiguration());
    this.moduleDirectory = moduleDirectory;
  }

  /*
   * Singleton instance
   * ____________________________________________________________________________________________________________________
   */
  private static SolrModuleConfiguration instance = null;

  public static SolrModuleConfiguration getInstance(Path moduleDirectory) {
    if (instance == null) {
      instance = new SolrModuleConfiguration(moduleDirectory);
    }
    return instance;
  }

  /*
   * Implementation-dependent parts
   * ____________________________________________________________________________________________________________________
   */
  @Override
  public void clearViewerCachableObjectsAfterConfigurationChange() {
    workspaceDirectory = null;
    workspaceForLobs = null;
  }

  @Override
  public Path getLobPath() {
    if (workspaceForLobs == null) {
      workspaceForLobs = getWorkspaceDirectory().resolve(ViewerConstants.VIEWER_LOBS_FOLDER);
    }
    return workspaceForLobs;
  }

  /*
   * "Internal" helper methods
   * ____________________________________________________________________________________________________________________
   */
  private Path getWorkspaceDirectory() {
    if (workspaceDirectory == null) {
      String property = System.getProperty(ViewerConstants.INSTALL_FOLDER_SYSTEM_PROPERTY);
      String env = System.getenv(ViewerConstants.INSTALL_FOLDER_ENVIRONMENT_VARIABLE);

      if (StringUtils.isNotBlank(property)) {
        workspaceDirectory = Paths.get(property);
      } else if (StringUtils.isNotBlank(env)) {
        workspaceDirectory = Paths.get(env);
      } else {
        workspaceDirectory = moduleDirectory;
      }
    }
    return workspaceDirectory;
  }
}
