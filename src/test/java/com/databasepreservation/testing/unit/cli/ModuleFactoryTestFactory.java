package com.databasepreservation.testing.unit.cli;

import com.databasepreservation.cli.CLI;
import com.databasepreservation.modules.DatabaseExportModule;
import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.DatabaseModuleFactory;
import org.apache.commons.cli.ParseException;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ModuleFactoryTestFactory {
        private final Class<? extends DatabaseModuleFactory> module;

        protected ModuleFactoryTestFactory(Class<? extends DatabaseModuleFactory> module){
                this.module = module;
        }

        protected CLI buildCli(String... args){
                return new CLI(Arrays.asList(args), module);
        }

        protected DatabaseImportModule getImportModule(Class<? extends DatabaseImportModule> importModuleClass,String... args){
                DatabaseImportModule module;
                try {
                        module = buildCli(args).getImportModule();
                } catch (ParseException e) {
                        throw new RuntimeException(e);
                }

                assertThat("import module must be " + importModuleClass.toString(), module,
                  instanceOf(importModuleClass));
                return module;
        }

        protected DatabaseExportModule getExportModule(Class<? extends DatabaseExportModule> exportModuleClass,String... args){
                DatabaseExportModule module;
                try {
                        module = buildCli(args).getExportModule();
                } catch (ParseException e) {
                        throw new RuntimeException(e);
                }

                assertThat("export module must be " + exportModuleClass.toString(), module,
                  instanceOf(exportModuleClass));
                return module;
        }
}
