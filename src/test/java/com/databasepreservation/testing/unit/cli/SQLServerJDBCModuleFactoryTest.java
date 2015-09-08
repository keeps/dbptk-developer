package com.databasepreservation.testing.unit.cli;

import com.databasepreservation.cli.Parameter;
import com.databasepreservation.modules.DatabaseExportModule;
import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.sqlServer.SQLServerJDBCModuleFactory;
import com.databasepreservation.modules.sqlServer.in.SQLServerJDBCImportModule;
import com.databasepreservation.modules.sqlServer.out.SQLServerJDBCExportModule;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
@Test(groups = {"cli"}) public class SQLServerJDBCModuleFactoryTest {
        private static ModuleFactoryTestFactory testFactory = new ModuleFactoryTestFactory(
          SQLServerJDBCModuleFactory.class);

        private static Class<? extends DatabaseImportModule> importModuleClass = SQLServerJDBCImportModule.class;
        private static Class<? extends DatabaseExportModule> exportModuleClass = SQLServerJDBCExportModule.class;

        @Test public void arguments_only_necessary_long() {
                String[] args = new String[] {"--import=SQLServerJDBC", "--iusername=name-user",
                  "--ipassword=abc1 23=456", "--iserver-name=the-server-name", "--idatabase=dbname",
                  "--export=SQLServerJDBC", "--eusername=name-another-user", "--epassword=2bcd123=456",
                  "--eserver-name=another-server", "--edatabase=another-db-name"};

                // verify that arguments create the correct import and export modules
                testFactory.assertCorrectModuleClass(importModuleClass, exportModuleClass, args);

                // get information needed to test the actual parameters
                Map<String, Parameter> parameters = testFactory.getModuleParameters(args);
                HashMap<Parameter, String> importModuleArguments = testFactory.getImportModuleArguments(args);
                HashMap<Parameter, String> exportModuleArguments = testFactory.getExportModuleArguments(args);

                HashMap<String, String> expectedValues;

                // test parameters for import module
                expectedValues = new HashMap<String, String>();
                expectedValues.put("username", "name-user");
                expectedValues.put("password", "abc1 23=456");
                expectedValues.put("server-name", "the-server-name");
                expectedValues.put("database", "dbname");

                for (Map.Entry<String, String> entry : expectedValues.entrySet()) {
                        String property = entry.getKey();
                        String expectation = entry.getValue();
                        assertThat("Property '" + property + "' must have value '" + expectation + "'",
                          importModuleArguments.get(parameters.get(property)), equalTo(expectation));
                }

                // test parameters for export module
                expectedValues = new HashMap<String, String>();
                expectedValues.put("username", "name-another-user");
                expectedValues.put("password", "2bcd123=456");
                expectedValues.put("server-name", "another-server");
                expectedValues.put("database", "another-db-name");

                for (Map.Entry<String, String> entry : expectedValues.entrySet()) {
                        String property = entry.getKey();
                        String expectation = entry.getValue();
                        assertThat("Property '" + property + "' must have value '" + expectation + "'",
                          exportModuleArguments.get(parameters.get(property)), equalTo(expectation));
                }
        }

        @Test public void arguments_only_necessary_short() {
                String[] args = new String[] {"-i", "SQLServerJDBC", "-iu", "name-user", "-ip", "abc1 23=456", "-is",
                  "the-server-name", "-idb", "dbname", "-e", "SQLServerJDBC", "-eu", "name-another-user", "-ep",
                  "2bcd123=456", "-es", "another-server", "-edb", "another-db-name"};

                // verify that arguments create the correct import and export modules
                testFactory.assertCorrectModuleClass(importModuleClass, exportModuleClass, args);

                // get information needed to test the actual parameters
                Map<String, Parameter> parameters = testFactory.getModuleParameters(args);
                HashMap<Parameter, String> importModuleArguments = testFactory.getImportModuleArguments(args);
                HashMap<Parameter, String> exportModuleArguments = testFactory.getExportModuleArguments(args);

                HashMap<String, String> expectedValues;

                // test parameters for import module
                expectedValues = new HashMap<String, String>();
                expectedValues.put("username", "name-user");
                expectedValues.put("password", "abc1 23=456");
                expectedValues.put("server-name", "the-server-name");
                expectedValues.put("database", "dbname");

                for (Map.Entry<String, String> entry : expectedValues.entrySet()) {
                        String property = entry.getKey();
                        String expectation = entry.getValue();
                        assertThat("Property '" + property + "' must have value '" + expectation + "'",
                          importModuleArguments.get(parameters.get(property)), equalTo(expectation));
                }

                // test parameters for export module
                expectedValues = new HashMap<String, String>();
                expectedValues.put("username", "name-another-user");
                expectedValues.put("password", "2bcd123=456");
                expectedValues.put("server-name", "another-server");
                expectedValues.put("database", "another-db-name");

                for (Map.Entry<String, String> entry : expectedValues.entrySet()) {
                        String property = entry.getKey();
                        String expectation = entry.getValue();
                        assertThat("Property '" + property + "' must have value '" + expectation + "'",
                          exportModuleArguments.get(parameters.get(property)), equalTo(expectation));
                }
        }
}
