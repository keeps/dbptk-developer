/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.dbml;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class DBMLModuleFactory implements DatabaseModuleFactory {
  public static final String PARAMETER_FILE = "file";

  private static final Parameter file = new Parameter().shortName("f").longName(PARAMETER_FILE)
    .description("Path to DBML file").hasArgument(true).setOptionalArgument(false).required(true);

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
    return "dbml-alpha";
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<String, Parameter>();
    parameterHashMap.put(file.longName(), file);
    return parameterHashMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(file), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws UnsupportedModuleException {
    throw DatabaseModuleFactory.ExceptionBuilder.UnsupportedModuleExceptionForExportModule();
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException, LicenseNotAcceptedException {
    Path pFile = Paths.get(parameters.get(file));

    reporter.importModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString());
    return new DBMLImportModule(pFile);
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException, LicenseNotAcceptedException {
    throw DatabaseModuleFactory.ExceptionBuilder.UnsupportedModuleExceptionForExportModule();
  }
}
