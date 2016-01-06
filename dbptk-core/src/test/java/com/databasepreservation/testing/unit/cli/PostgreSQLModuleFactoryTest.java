package com.databasepreservation.testing.unit.cli;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.testng.annotations.Test;

import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.modules.postgreSql.PostgreSQLModuleFactory;
import com.databasepreservation.modules.postgreSql.in.PostgreSQLJDBCImportModule;
import com.databasepreservation.modules.postgreSql.out.PostgreSQLJDBCExportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
@Test(groups = {"cli"})
public class PostgreSQLModuleFactoryTest {
  private static Class<? extends DatabaseImportModule> importModuleClass = PostgreSQLJDBCImportModule.class;
  private static Class<? extends DatabaseExportModule> exportModuleClass = PostgreSQLJDBCExportModule.class;

  private static ModuleFactoryTestHelper testHelper = new ModuleFactoryTestHelper(PostgreSQLModuleFactory.class,
    importModuleClass, exportModuleClass);

  @Test
  public void arguments_required_long() {
    List<String> args = Arrays.asList("--import=postgresql", "--import-username=name-user",
      "--import-password=abc1 23=456", "--import-hostname=the-server-name", "--import-database=dbname",
      "--export=postgresql", "--export-username=name-another-user", "--export-password=2bcd123=456",
      "--export-hostname=another-server", "--export-database=another-db-name", "--import-disable-encryption");

    // test parameters for import module
    HashMap<String, String> expectedValuesImport = new HashMap<String, String>();
    expectedValuesImport.put("hostname", "the-server-name");
    expectedValuesImport.put("database", "dbname");
    expectedValuesImport.put("username", "name-user");
    expectedValuesImport.put("password", "abc1 23=456");
    expectedValuesImport.put("disable-encryption", "true");

    // test parameters for export module
    HashMap<String, String> expectedValuesExport = new HashMap<String, String>();
    expectedValuesExport.put("hostname", "another-server");
    expectedValuesExport.put("database", "another-db-name");
    expectedValuesExport.put("username", "name-another-user");
    expectedValuesExport.put("password", "2bcd123=456");

    ModuleFactoryTestHelper.validate_arguments(testHelper, args, expectedValuesImport, expectedValuesExport);
  }

  @Test
  public void arguments_required_short() {
    List<String> args = Arrays.asList("-i", "postgresql", "-iu", "name-user", "-ip", "abc1 23=456", "-ih",
      "the-server-name", "-idb", "dbname", "-e", "postgresql", "-eu", "name-another-user", "-ep", "2bcd123=456", "-eh",
      "another-server", "-edb", "another-db-name", "-ede");

    // test parameters for import module
    HashMap<String, String> expectedValuesImport = new HashMap<String, String>();
    expectedValuesImport.put("hostname", "the-server-name");
    expectedValuesImport.put("database", "dbname");
    expectedValuesImport.put("username", "name-user");
    expectedValuesImport.put("password", "abc1 23=456");
    // expectedValuesImport.put("disable-encryption", "");

    // test parameters for export module
    HashMap<String, String> expectedValuesExport = new HashMap<String, String>();
    expectedValuesExport.put("hostname", "another-server");
    expectedValuesExport.put("database", "another-db-name");
    expectedValuesExport.put("username", "name-another-user");
    expectedValuesExport.put("password", "2bcd123=456");
    expectedValuesExport.put("disable-encryption", "true");

    ModuleFactoryTestHelper.validate_arguments(testHelper, args, expectedValuesImport, expectedValuesExport);
  }

  @Test
  public void arguments_portNumber_long() {
    List<String> args = Arrays.asList("--import=postgresql", "--import-username=name-user",
      "--import-password=abc1 23=456", "--import-hostname=the-server-name", "--import-database=dbname",
      "--export=postgresql", "--export-username=name-another-user", "--export-password=2bcd123=456",
      "--export-hostname=another-server", "--export-database=another-db-name", "--import-disable-encryption",
      "--import-port-number=1234", "--export-port-number=4321");

    // test parameters for import module
    HashMap<String, String> expectedValuesImport = new HashMap<String, String>();
    expectedValuesImport.put("hostname", "the-server-name");
    expectedValuesImport.put("database", "dbname");
    expectedValuesImport.put("username", "name-user");
    expectedValuesImport.put("password", "abc1 23=456");
    expectedValuesImport.put("disable-encryption", "true");
    expectedValuesImport.put("port-number", "1234");

    // test parameters for export module
    HashMap<String, String> expectedValuesExport = new HashMap<String, String>();
    expectedValuesExport.put("hostname", "another-server");
    expectedValuesExport.put("database", "another-db-name");
    expectedValuesExport.put("username", "name-another-user");
    expectedValuesExport.put("password", "2bcd123=456");
    expectedValuesExport.put("port-number", "4321");

    ModuleFactoryTestHelper.validate_arguments(testHelper, args, expectedValuesImport, expectedValuesExport);
  }

  @Test
  public void arguments_portNumber_short() {
    List<String> args = Arrays.asList("-i", "postgresql", "-iu", "name-user", "-ip", "abc1 23=456", "-ih",
      "the-server-name", "-idb", "dbname", "-e", "postgresql", "-eu", "name-another-user", "-ep", "2bcd123=456", "-eh",
      "another-server", "-edb", "another-db-name", "-ede", "-ipn", "4567", "-epn", "7654");

    // test parameters for import module
    HashMap<String, String> expectedValuesImport = new HashMap<String, String>();
    expectedValuesImport.put("hostname", "the-server-name");
    expectedValuesImport.put("database", "dbname");
    expectedValuesImport.put("username", "name-user");
    expectedValuesImport.put("password", "abc1 23=456");
    expectedValuesImport.put("port-number", "4567");

    // test parameters for export module
    HashMap<String, String> expectedValuesExport = new HashMap<String, String>();
    expectedValuesExport.put("hostname", "another-server");
    expectedValuesExport.put("database", "another-db-name");
    expectedValuesExport.put("username", "name-another-user");
    expectedValuesExport.put("password", "2bcd123=456");
    expectedValuesExport.put("disable-encryption", "true");
    expectedValuesExport.put("port-number", "7654");

    ModuleFactoryTestHelper.validate_arguments(testHelper, args, expectedValuesImport, expectedValuesExport);
  }
}
