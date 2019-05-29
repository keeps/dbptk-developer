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

import com.databasepreservation.model.exception.ModuleException;
import org.apache.commons.cli.ParseException;

import com.databasepreservation.cli.CLI;
import com.databasepreservation.cli.CLIMigrate;
import com.databasepreservation.model.NoOpReporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.modules.edits.EditModuleFactory;
import com.databasepreservation.model.modules.filters.DatabaseFilterFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.utils.ReflectionUtils;

/**
 * Helper class to help test ModuleFactories
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
// TODO: incomplete because default values of optional parameters are not tested
public class ModuleFactoryTestHelper {
  private final Class<? extends DatabaseModuleFactory> moduleFactory;
  private final List<DatabaseModuleFactory> moduleFactories;
  private final Class<? extends DatabaseImportModule> importModuleClass;
  private final Class<? extends DatabaseExportModule> exportModuleClass;

  private final List<DatabaseFilterFactory> databaseFilterFactories;
  private final List<EditModuleFactory> editModuleFactories;

  protected ModuleFactoryTestHelper(Class<? extends DatabaseModuleFactory> moduleFactory,
    Class<? extends DatabaseImportModule> importModuleClass, Class<? extends DatabaseExportModule> exportModuleClass) {
    this.moduleFactory = moduleFactory;
    this.moduleFactories = new ArrayList<>(ReflectionUtils.collectDatabaseModuleFactories());
    this.databaseFilterFactories = new ArrayList<>(ReflectionUtils.collectDatabaseFilterFactory());
    this.editModuleFactories = new ArrayList<>(ReflectionUtils.collectEditModuleFactories());

    try {
      this.moduleFactories.add(this.moduleFactory.getConstructor().newInstance());
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
    this.importModuleClass = importModuleClass;
    this.exportModuleClass = exportModuleClass;
  }

  private CLI buildCli(List<String> args) {
    return new CLI(args, moduleFactories, databaseFilterFactories, editModuleFactories);
  }

  private Map<Parameter, String> getImportModuleArguments(List<String> args) {
    return getModuleArguments(true, args);
  }

  private Map<Parameter, String> getExportModuleArguments(List<String> args) {
    return getModuleArguments(false, args);
  }

  private Map<Parameter, String> getModuleArguments(boolean forImportModule, List<String> args) {
    try {
      CLI cli = new CLI(args, moduleFactories, databaseFilterFactories, editModuleFactories);

      CLIMigrate cliMigrate = cli.getCLIMigrate();

      Method getModuleFactories = CLIMigrate.class.getDeclaredMethod("getModuleFactories", List.class);
      getModuleFactories.setAccessible(true);
      CLIMigrate.DatabaseModuleFactories databaseModuleFactories = (CLIMigrate.DatabaseModuleFactories) getModuleFactories
        .invoke(cliMigrate, args);

      Method getModuleArguments = CLIMigrate.class.getDeclaredMethod("getModuleArguments",
        CLIMigrate.DatabaseModuleFactories.class, List.class);
      getModuleArguments.setAccessible(true);
      CLIMigrate.DatabaseModuleFactoriesArguments databaseModuleFactoriesArguments = (CLIMigrate.DatabaseModuleFactoriesArguments) getModuleArguments
        .invoke(cliMigrate, databaseModuleFactories, args);

      if (forImportModule) {
        return databaseModuleFactoriesArguments.getImportModuleArguments();
      } else {
        return databaseModuleFactoriesArguments.getExportModuleArguments();
      }
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, Parameter> getModuleParameters(List<String> args) {
    try {
      CLI cli = new CLI(args, moduleFactories, databaseFilterFactories, editModuleFactories);

      CLIMigrate cliMigrate = cli.getCLIMigrate();

      Method getModuleFactories = CLIMigrate.class.getDeclaredMethod("getModuleFactories", List.class);
      getModuleFactories.setAccessible(true);
      CLIMigrate.DatabaseModuleFactories databaseModuleFactories = (CLIMigrate.DatabaseModuleFactories) getModuleFactories
        .invoke(cliMigrate, args);

      return databaseModuleFactories.getImportModuleFactory().getAllParameters();
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
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
      databaseImportModuleFactory = cli.getCLIMigrate().getImportModuleFactory();
      databaseExportModuleFactory = cli.getCLIMigrate().getExportModuleFactory();
      importModuleParameters = cli.getCLIMigrate().getImportModuleParameters();
      exportModuleParameters = cli.getCLIMigrate().getExportModuleParameters();
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }

    assertThat("import module factory must be " + moduleFactory.toString(), databaseImportModuleFactory,
      instanceOf(moduleFactory));

    assertThat("export module factory must be " + moduleFactory.toString(), databaseExportModuleFactory,
      instanceOf(moduleFactory));

    try {
      databaseImportModule = databaseImportModuleFactory.buildImportModule(importModuleParameters, new NoOpReporter());
      databaseExportModule = databaseExportModuleFactory.buildExportModule(exportModuleParameters, new NoOpReporter());
    } catch (ModuleException e) {
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
