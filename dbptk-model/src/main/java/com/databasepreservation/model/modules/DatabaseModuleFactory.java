/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules;

import java.util.Map;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;

/**
 * Defines a factory used to create Import and Export Modules. This factory
 * should also be able to inform the parameters needed to create a new import or
 * export module.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface DatabaseModuleFactory {
  boolean producesImportModules();

  boolean producesExportModules();

  String getModuleName();

  boolean isEnabled();

  Map<String, Parameter> getAllParameters();

  Parameters getConnectionParameters() throws UnsupportedModuleException;

  Parameters getImportModuleParameters() throws UnsupportedModuleException;

  Parameters getExportModuleParameters() throws UnsupportedModuleException;

  DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter) throws ModuleException;

  DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, ModuleConfiguration moduleConfiguration,
    Reporter reporter) throws ModuleException;

  DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException, LicenseNotAcceptedException, ModuleException;

  class ExceptionBuilder {
    public static UnsupportedModuleException UnsupportedModuleExceptionForImportModule() {
      return new UnsupportedModuleException("Import module not available");
    }

    public static UnsupportedModuleException UnsupportedModuleExceptionForExportModule() {
      return new UnsupportedModuleException("Export module not available");
    }
  }
}
