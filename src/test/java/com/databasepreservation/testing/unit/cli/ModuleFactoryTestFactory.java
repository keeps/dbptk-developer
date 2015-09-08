package com.databasepreservation.testing.unit.cli;

import com.databasepreservation.cli.CLI;
import com.databasepreservation.cli.Parameter;
import com.databasepreservation.modules.DatabaseExportModule;
import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.DatabaseModuleFactory;
import org.apache.commons.cli.ParseException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ModuleFactoryTestFactory {
        private final Class<? extends DatabaseModuleFactory> module;

        protected ModuleFactoryTestFactory(Class<? extends DatabaseModuleFactory> module) {
                this.module = module;
        }

        protected CLI buildCli(String... args) {
                return new CLI(Arrays.asList(args), module);
        }

        protected HashMap<Parameter, String> getImportModuleArguments(String... args) {
                return getModuleArguments(true, args);
        }

        protected HashMap<Parameter, String> getExportModuleArguments(String... args) {
                return getModuleArguments(false, args);
        }

        private HashMap<Parameter, String> getModuleArguments(boolean forImportModule, String... args) {
                try {
                        CLI cli = new CLI(Arrays.asList(args), module);

                        Method getModuleFactories = CLI.class.getDeclaredMethod("getModuleFactories", List.class);
                        getModuleFactories.setAccessible(true);
                        CLI.DatabaseModuleFactoriesPair databaseModuleFactoriesPair = (CLI.DatabaseModuleFactoriesPair) getModuleFactories
                          .invoke(cli, Arrays.asList(args));

                        Method getModuleArguments = CLI.class
                          .getDeclaredMethod("getModuleArguments", CLI.DatabaseModuleFactoriesPair.class, List.class);
                        getModuleArguments.setAccessible(true);
                        CLI.DatabaseModuleFactoriesArguments databaseModuleFactoriesArguments = (CLI.DatabaseModuleFactoriesArguments) getModuleArguments
                          .invoke(cli, databaseModuleFactoriesPair, Arrays.asList(args));

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

        protected Map<String, Parameter> getModuleParameters(String... args) {
                try {
                        CLI cli = new CLI(Arrays.asList(args), module);

                        Method getModuleFactories = CLI.class.getDeclaredMethod("getModuleFactories", List.class);
                        getModuleFactories.setAccessible(true);
                        CLI.DatabaseModuleFactoriesPair databaseModuleFactoriesPair = (CLI.DatabaseModuleFactoriesPair) getModuleFactories
                          .invoke(cli, Arrays.asList(args));

                        return databaseModuleFactoriesPair.getImportModuleFactory().getAllParameters();
                } catch (NoSuchMethodException e) {
                        throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                        throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                }
        }

        protected void assertCorrectModuleClass(Class<? extends DatabaseImportModule> importModuleClass,
          Class<? extends DatabaseExportModule> exportModuleClass, String... args) {
                CLI cli = buildCli(args);

                DatabaseImportModule databaseImportModule;
                DatabaseExportModule databaseExportModule;
                try {
                        databaseImportModule = cli.getImportModule();
                        databaseExportModule = cli.getExportModule();
                } catch (ParseException e) {
                        throw new RuntimeException(e);
                }

                assertThat("import module must be " + importModuleClass.toString(), databaseImportModule,
                  instanceOf(importModuleClass));

                assertThat("export module must be " + exportModuleClass.toString(), databaseExportModule,
                  instanceOf(exportModuleClass));
        }
}
