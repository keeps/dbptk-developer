/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.config;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.RequiredParameterException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.utils.ReflectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Exposes an export module that produces a list of tables contained in the
 * database. This list can then be used by other modules (e.g. the SIARD2 export
 * module) to specify the tables that should be processed.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ImportConfigurationModuleFactory implements DatabaseModuleFactory {
  public static final String PARAMETER_FILE = "file";

  private static final Parameter file = new Parameter().shortName("f").longName(PARAMETER_FILE)
    .description("Path to the import configuration file to be read by the SIARD export module").hasArgument(true)
    .setOptionalArgument(false).required(true);

  @Override
  public boolean producesImportModules() {
    return true;
  }

  @Override
  public boolean producesExportModules() {
    return true;
  }

  @Override
  public String getModuleName() {
    return "import-config";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    Map<String, Parameter> parameterHashMap = new HashMap<>();
    parameterHashMap.put(file.longName(), file);
    return parameterHashMap;
  }

  @Override
  public Parameters getConnectionParameters() throws UnsupportedModuleException {
    throw DatabaseModuleFactory.ExceptionBuilder.UnsupportedModuleExceptionForImportModule();
  }

  @Override
  public Parameters getImportModuleParameters() {
    return new Parameters(Collections.singletonList(file), null);
  }

  @Override
  public Parameters getExportModuleParameters() {
    return new Parameters(Collections.singletonList(file), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws ModuleException {
    Path pFile = Paths.get(parameters.get(file));

    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try {
      final ModuleConfiguration moduleConfiguration = mapper.readValue(pFile.toFile(), ModuleConfiguration.class);

      final String moduleName = moduleConfiguration.getImportModuleConfiguration().getModuleName();

      Set<DatabaseModuleFactory> databaseModuleFactories = ReflectionUtils.collectDatabaseModuleFactories();
      DatabaseModuleFactory actualFactory = null;
      for (DatabaseModuleFactory factory : databaseModuleFactories) {
        if (!factory.getModuleName().equals(getModuleName())) {
          if (factory.getModuleName().equals(moduleName) && factory.producesImportModules() && factory.isEnabled()) {
            actualFactory = factory;
          }
        }
      }

      if (actualFactory == null) {
        throw new UnsupportedModuleException().withMessage("Import module " + moduleName + " not recognized as valid");
      } else {
        final Parameters importModuleParameters = actualFactory.getImportModuleParameters();
        Map<Parameter, String> importParameters = new HashMap<>();

        for (Parameter parameter : importModuleParameters.getParameters()) {
          final String value = moduleConfiguration.getImportModuleParameterValue(parameter.longName());
          if (parameter.required()) {
            if (value == null) {
              throw new RequiredParameterException().withParameter(parameter.longName());
            } else {
              importParameters.put(parameter, value);
            }
          } else {
            if (value != null) {
              importParameters.put(parameter, value);
            }
          }
        }

        return actualFactory.buildImportModule(importParameters, moduleConfiguration, reporter);
      }
    } catch (IOException e) {
      throw new ModuleException()
        .withMessage("Could not read the configuration from file " + pFile.normalize().toAbsolutePath().toString())
        .withCause(e);
    }
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, ModuleConfiguration moduleConfiguration, Reporter reporter) throws ModuleException {
    return buildImportModule(parameters, reporter);
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter) {
    Path pFile = Paths.get(parameters.get(file));

    reporter.exportModuleParameters(this.getModuleName(), PARAMETER_FILE,
      pFile.normalize().toAbsolutePath().toString());
    return new ImportConfiguration(pFile);
  }
}
