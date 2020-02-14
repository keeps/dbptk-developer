package com.databasepreservation;

import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.utils.ModuleConfigurationUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class ModuleConfigurationManager {

  private static ModuleConfiguration moduleConfiguration = null;
  private static boolean initialized = false;
  private static ModuleConfigurationManager instance = null;

  public static ModuleConfigurationManager getInstance() {
    if (instance == null) {
      instance = new ModuleConfigurationManager();
    }

    return instance;
  }

  public void setup(ModuleConfiguration configuration) {
    if (!isInitialized()) {
      moduleConfiguration = configuration;
      initialized = true;
    }
  }

  public ModuleConfiguration getModuleConfiguration() {
    if (isInitialized()) {
      return moduleConfiguration;
    } else {
      return ModuleConfigurationUtils.getDefaultModuleConfiguration();
    }
  }

  public boolean isInitialized() {
    return initialized;
  }
}
