/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.managers.ModuleConfigurationManager;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.RequiredParameterException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.utils.ModuleConfigurationUtils;
import com.databasepreservation.utils.ReflectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;

/**
 * Exposes an export module that produces a list of tables contained in the
 * database. This list can then be used by other modules (e.g. the SIARD2 export
 * module) to specify the tables that should be processed.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ImportConfigurationModuleFactory implements DatabaseModuleFactory {
  public static final String PARAMETER_FILE = "file";
  public static final String PARAMETER_PARAMETERS = "parameters";

  private static final Parameter file = new Parameter().shortName("f").longName(PARAMETER_FILE)
    .description("Path to the import configuration file").hasArgument(true)
    .setOptionalArgument(false).required(true);

  private static final Parameter templatingValues = new Parameter().shortName("p").longName(PARAMETER_PARAMETERS)
    .description("Pair of parameters to be resolved in the YAML configuration file. "
      + "To define a pair use this syntax: key:value;key:value;")
    .hasArgument(true).setOptionalArgument(false).required(false).numberOfArgs(2);

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
    parameterHashMap.put(templatingValues.longName(), templatingValues);
    return parameterHashMap;
  }

  @Override
  public Parameters getConnectionParameters() throws UnsupportedModuleException {
    throw DatabaseModuleFactory.ExceptionBuilder.UnsupportedModuleExceptionForImportModule();
  }

  @Override
  public Parameters getImportModuleParameters() {
    return new Parameters(Arrays.asList(file, templatingValues), null);
  }

  @Override
  public Parameters getExportModuleParameters() {
    return new Parameters(Collections.singletonList(file), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws ModuleException {
    Path pFile = Paths.get(parameters.get(file));
    final String pTemplateValues = parameters.get(templatingValues);

    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try {
      ModuleConfiguration moduleConfiguration;
      if (StringUtils.isNotBlank(pTemplateValues)) {
        moduleConfiguration = mapper.readValue(applyTemplatingTransformation(pFile, pTemplateValues),
          ModuleConfiguration.class);
      } else {
        moduleConfiguration = mapper.readValue(pFile.toFile(), ModuleConfiguration.class);
      }

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

        ModuleConfigurationManager.getInstance().setup(moduleConfiguration);

        return actualFactory.buildImportModule(importParameters, reporter);
      }
    } catch (IOException e) {
      throw new ModuleException()
        .withMessage("Could not read the configuration from file " + pFile.normalize().toAbsolutePath().toString())
        .withCause(e);
    }
  }

  @Override
  public DatabaseFilterModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter) {
    Path pFile = Paths.get(parameters.get(file));

    reporter.exportModuleParameters(this.getModuleName(), PARAMETER_FILE,
      pFile.normalize().toAbsolutePath().toString());

    final ModuleConfiguration defaultModuleConfiguration = ModuleConfigurationUtils.getDefaultModuleConfiguration();
    defaultModuleConfiguration.setFetchRows(false);
    defaultModuleConfiguration
      .setIgnore(ModuleConfigurationUtils.createIgnoreListExcept(true, DatabaseTechnicalFeatures.VIEWS));

    ModuleConfigurationManager.getInstance().setup(defaultModuleConfiguration);

    reporter.exportModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString());

    return new ImportConfiguration(pFile);
  }

  private Map<String, String> parseTemplateValues(String toParse) throws ModuleException {
    Map<String, String> map = new HashMap<>();

    final String trim = toParse.trim();
    try {
      Arrays.stream(trim.split(";")).forEach(pair -> {
        final String[] split = pair.split(":", 2);
        map.put(split[0], split[1]);
      });

    } catch (ArrayIndexOutOfBoundsException e) {
      throw new ModuleException().withCause(e)
        .withMessage("Failed to parse the template values. Please confirm the input for possible errors");
    }
    return map;
  }

  private String applyTemplatingTransformation(Path pFile, String pTemplateValues) throws ModuleException {
    final Map<String, String> map = parseTemplateValues(pTemplateValues);

    Handlebars handlebars = new Handlebars();
    try {
      Template template = handlebars.compileInline(new String(Files.readAllBytes(pFile), StandardCharsets.UTF_8));
      return template.apply(map);
    } catch (IOException e) {
      throw new ModuleException().withMessage("Failed to apply template system").withCause(e);
    }
  }
}
