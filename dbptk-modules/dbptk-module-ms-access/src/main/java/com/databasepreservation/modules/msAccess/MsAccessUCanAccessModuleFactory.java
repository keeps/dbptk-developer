/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.msAccess;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameter.INPUT_TYPE;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.msAccess.in.MsAccessUCanAccessImportModule;
import com.databasepreservation.utils.ModuleConfigurationUtils;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class MsAccessUCanAccessModuleFactory implements DatabaseModuleFactory {
  public static final String PARAMETER_FILE = "file";
  public static final String PARAMETER_PASSWORD = "password";

  private static final Parameter accessFilePath = new Parameter().shortName("f").longName(PARAMETER_FILE)
    .description("path to the Microsoft Access file").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter accessPassword = new Parameter().shortName("p").longName(PARAMETER_PASSWORD)
    .description("password to the Microsoft Access file").hasArgument(true).setOptionalArgument(false).required(false);

  @Override
  public boolean producesImportModules() {
    return true;
  }

  @Override
  public boolean producesExportModules() {
    return false;
  }

  @Override
  public String getModuleName() {
    return "microsoft-access";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<String, Parameter>();
    parameterHashMap.put(accessFilePath.longName(), accessFilePath);
    parameterHashMap.put(accessPassword.longName(), accessPassword);
    return parameterHashMap;
  }

  @Override
  public Parameters getConnectionParameters() {
    return new Parameters(Arrays.asList(accessFilePath.inputType(INPUT_TYPE.FILE_OPEN),
      accessPassword.longName("ms-password").inputType(INPUT_TYPE.PASSWORD)), null);
  }

  @Override
  public Parameters getImportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(accessFilePath, accessPassword), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws UnsupportedModuleException {
    throw DatabaseModuleFactory.ExceptionBuilder.UnsupportedModuleExceptionForExportModule();
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter) throws ModuleException {
    String pAccessFilePath = parameters.get(accessFilePath);

    String pAccessPassword = null;
    if (StringUtils.isNotBlank(parameters.get(accessPassword))) {
      pAccessPassword = parameters.get(accessPassword);
    }

    reporter.importModuleParameters(getModuleName(), PARAMETER_FILE, pAccessFilePath);
    if (pAccessPassword != null) {
      return new MsAccessUCanAccessImportModule(getModuleName(), pAccessFilePath, pAccessPassword);
    } else {
      return new MsAccessUCanAccessImportModule(getModuleName(), pAccessFilePath);
    }
  }

  @Override
  public DatabaseFilterModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException {
    throw DatabaseModuleFactory.ExceptionBuilder.UnsupportedModuleExceptionForExportModule();
  }
}
