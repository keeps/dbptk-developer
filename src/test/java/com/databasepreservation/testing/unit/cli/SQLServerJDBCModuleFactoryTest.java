package com.databasepreservation.testing.unit.cli;

import com.databasepreservation.modules.DatabaseExportModule;
import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.sqlServer.SQLServerJDBCModuleFactory;
import com.databasepreservation.modules.sqlServer.in.SQLServerJDBCImportModule;
import com.databasepreservation.modules.sqlServer.out.SQLServerJDBCExportModule;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
@Test(groups = {"cli"}) public class SQLServerJDBCModuleFactoryTest {
        private static Class<? extends DatabaseImportModule> importModuleClass = SQLServerJDBCImportModule.class;
        private static Class<? extends DatabaseExportModule> exportModuleClass = SQLServerJDBCExportModule.class;

        private static ModuleFactoryTestFactory testFactory = new ModuleFactoryTestFactory(
          SQLServerJDBCModuleFactory.class, importModuleClass, exportModuleClass);

        @Test public void arguments_only_necessary_long() {
                List<String> args = Arrays
                  .asList("--import=SQLServerJDBC", "--iusername=name-user", "--ipassword=abc1 23=456",
                    "--iserver-name=the-server-name", "--idatabase=dbname", "--export=SQLServerJDBC",
                    "--eusername=name-another-user", "--epassword=2bcd123=456", "--eserver-name=another-server",
                    "--edatabase=another-db-name");

                // test parameters for import module
                HashMap<String, String> expectedValuesImport = new HashMap<String, String>();
                expectedValuesImport.put("username", "name-user");
                expectedValuesImport.put("password", "abc1 23=456");
                expectedValuesImport.put("server-name", "the-server-name");
                expectedValuesImport.put("database", "dbname");

                // test parameters for export module
                HashMap<String, String> expectedValuesExport = new HashMap<String, String>();
                expectedValuesExport.put("username", "name-another-user");
                expectedValuesExport.put("password", "2bcd123=456");
                expectedValuesExport.put("server-name", "another-server");
                expectedValuesExport.put("database", "another-db-name");

                ModuleFactoryTestFactory
                  .validate_arguments(testFactory, args, expectedValuesImport, expectedValuesExport);
        }

        @Test public void arguments_only_necessary_short() {
                List<String> args = Arrays
                  .asList("-i", "SQLServerJDBC", "-iu", "name-user", "-ip", "abc1 23=456", "-is", "the-server-name",
                    "-idb", "dbname", "-e", "SQLServerJDBC", "-eu", "name-another-user", "-ep", "2bcd123=456", "-es",
                    "another-server", "-edb", "another-db-name");

                // test parameters for import module
                HashMap<String, String> expectedValuesImport = new HashMap<String, String>();
                expectedValuesImport.put("username", "name-user");
                expectedValuesImport.put("password", "abc1 23=456");
                expectedValuesImport.put("server-name", "the-server-name");
                expectedValuesImport.put("database", "dbname");

                // test parameters for export module
                HashMap<String, String> expectedValuesExport = new HashMap<String, String>();
                expectedValuesExport.put("username", "name-another-user");
                expectedValuesExport.put("password", "2bcd123=456");
                expectedValuesExport.put("server-name", "another-server");
                expectedValuesExport.put("database", "another-db-name");

                ModuleFactoryTestFactory
                  .validate_arguments(testFactory, args, expectedValuesImport, expectedValuesExport);
        }
}
