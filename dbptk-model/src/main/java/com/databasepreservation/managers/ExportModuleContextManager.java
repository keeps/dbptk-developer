package com.databasepreservation.managers;

import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameters;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class ExportModuleContextManager {
  private static ExportModuleContextManager instance = null;
  private String moduleName;
  private Parameters exportModuleParameters;

  public static ExportModuleContextManager getInstance() {
    if (instance == null) {
      instance = new ExportModuleContextManager();
    }

    return instance;
  }

  public static void destroy() {
    instance = null;
  }

  public void setup(DatabaseModuleFactory exportModuleFactory) throws UnsupportedModuleException {
    moduleName = exportModuleFactory.getModuleName();
    exportModuleParameters = exportModuleFactory.getExportModuleParameters();
  }

  public String getModuleName() {
    return moduleName;
  }

  public Parameters getExportModuleParameters() {
    return exportModuleParameters;
  }

  public boolean isSiadDKModule() {
    return moduleName.contains("siard-dk");
  }
}
