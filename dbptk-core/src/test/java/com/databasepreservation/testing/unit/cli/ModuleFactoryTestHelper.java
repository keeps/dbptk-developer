/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.testing.unit.cli;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.ParseException;

import com.databasepreservation.cli.CLI;
import com.databasepreservation.model.NoOpReporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;

/**
 * Helper class to help test ModuleFactories
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
// TODO: incomplete because default values of optional parameters are not tested
public class ModuleFactoryTestHelper {
  private final Class<? extends DatabaseModuleFactory> moduleFactory;
  private final List<Class<? extends DatabaseModuleFactory>> moduleFactories;
  private final Class<? extends DatabaseImportModule> importModuleClass;
  private final Class<? extends DatabaseExportModule> exportModuleClass;

  protected ModuleFactoryTestHelper(Class<? extends DatabaseModuleFactory> moduleFactory,
    Class<? extends DatabaseImportModule> importModuleClass, Class<? extends DatabaseExportModule> exportModuleClass) {
    this.moduleFactory = moduleFactory;
    this.moduleFactories = new ArrayList<>();
    this.moduleFactories.add(this.moduleFactory);
    this.importModuleClass = importModuleClass;
    this.exportModuleClass = exportModuleClass;
  }

  private CLI buildCli(List<String> args) {
    return new CLI(args, moduleFactories);
  }

  private Map<Parameter, String> getImportModuleArguments(List<String> args) {
    return getModuleArguments(true, args);
  }

  private Map<Parameter, String> getExportModuleArguments(List<String> args) {
    return getModuleArguments(false, args);
  }

  private Map<Parameter, String> getModuleArguments(boolean forImportModule, List<String> args) {
    try {
      CLI cli = new CLI(args, moduleFactories);

      Method getModuleFactories = CLI.class.getDeclaredMethod("getModuleFactories", List.class);
      getModuleFactories.setAccessible(true);
      CLI.DatabaseModuleFactoriesPair databaseModuleFactoriesPair = (CLI.DatabaseModuleFactoriesPair) getModuleFactories
        .invoke(cli, args);

      Method getModuleArguments = CLI.class.getDeclaredMethod("getModuleArguments",
        CLI.DatabaseModuleFactoriesPair.class, List.class);
      getModuleArguments.setAccessible(true);
      CLI.DatabaseModuleFactoriesArguments databaseModuleFactoriesArguments = (CLI.DatabaseModuleFactoriesArguments) getModuleArguments
        .invoke(cli, databaseModuleFactoriesPair, args);

      if (forImportModule) {
        return databaseModuleFactoriesArguments.getImportModuleArguments();
      } else {
        return databaseModuleFactoriesArguments.getExportModuleArguments();
      }
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, Parameter> getModuleParameters(List<String> args) {
    try {
      CLI cli = new CLI(args, moduleFactories);

      Method getModuleFactories = CLI.class.getDeclaredMethod("getModuleFactories", List.class);
      getModuleFactories.setAccessible(true);
      CLI.DatabaseModuleFactoriesPair databaseModuleFactoriesPair = (CLI.DatabaseModuleFactoriesPair) getModuleFactories
        .invoke(cli, args);

      return databaseModuleFactoriesPair.getImportModuleFactory().getAllParameters();
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private void assertCorrectModuleClass(List<String> args) {
    CLI cli = buildCli(args);

    DatabaseImportModule databaseImportModule;
    DatabaseExportModule databaseExportModule;
    DatabaseModuleFactory databaseImportModuleFactory;
    DatabaseModuleFactory databaseExportModuleFactory;
    Map<Parameter, String> importModuleParameters;
    Map<Parameter, String> exportModuleParameters;
    try {
      databaseImportModuleFactory = cli.getImportModuleFactory();
      databaseExportModuleFactory = cli.getExportModuleFactory();
      importModuleParameters = cli.getImportModuleParameters();
      exportModuleParameters = cli.getExportModuleParameters();
    } catch (ParseException e) {
      throw new RuntimeException(e);
    } catch (LicenseNotAcceptedException e) {
      throw new RuntimeException(e);
    }

    assertThat("import module factory must be " + moduleFactory.toString(), databaseImportModuleFactory,
      instanceOf(moduleFactory));

    assertThat("export module factory must be " + moduleFactory.toString(), databaseExportModuleFactory,
      instanceOf(moduleFactory));

    try {
      databaseImportModule = databaseImportModuleFactory.buildImportModule(importModuleParameters, new NoOpReporter());
      databaseExportModule = databaseExportModuleFactory.buildExportModule(exportModuleParameters, new NoOpReporter());
    } catch (UnsupportedModuleException | LicenseNotAcceptedException e) {
      throw new RuntimeException(e);
    }

    assertThat("import module must be " + importModuleClass.toString(), databaseImportModule,
      instanceOf(importModuleClass));

    assertThat("export module must be " + exportModuleClass.toString(), databaseExportModule,
      instanceOf(exportModuleClass));
  }

  protected static void validate_arguments(ModuleFactoryTestHelper testFactory, List<String> args,
    HashMap<String, String> expectedImportValues, HashMap<String, String> expectedExportValues) {
    // verify that arguments create the correct import and export modules
    testFactory.assertCorrectModuleClass(args);

    // get information needed to test the actual parameters
    Map<String, Parameter> parameters = testFactory.getModuleParameters(args);
    Map<Parameter, String> importModuleArguments = testFactory.getImportModuleArguments(args);
    Map<Parameter, String> exportModuleArguments = testFactory.getExportModuleArguments(args);

    for (Map.Entry<String, String> entry : expectedImportValues.entrySet()) {
      String property = entry.getKey();
      String expectation = entry.getValue();
      assertThat("Property '" + property + "' must have value '" + expectation + "'",
        importModuleArguments.get(parameters.get(property)), equalTo(expectation));
    }

    for (Map.Entry<String, String> entry : expectedExportValues.entrySet()) {
      String property = entry.getKey();
      String expectation = entry.getValue();
      assertThat("Property '" + property + "' must have value '" + expectation + "'",
        exportModuleArguments.get(parameters.get(property)), equalTo(expectation));
    }
  }
}
