/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.testing.unit.cli;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.testng.annotations.Test;

import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.modules.sqlServer.SQLServerJDBCModuleFactory;
import com.databasepreservation.modules.sqlServer.in.SQLServerJDBCImportModule;
import com.databasepreservation.modules.sqlServer.out.SQLServerJDBCExportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
@Test(groups = {"cli"})
public class SQLServerJDBCModuleFactoryTest {
  private static Class<? extends DatabaseImportModule> importModuleClass = SQLServerJDBCImportModule.class;
  private static Class<? extends DatabaseExportModule> exportModuleClass = SQLServerJDBCExportModule.class;

  private static ModuleFactoryTestHelper testHelper = new ModuleFactoryTestHelper(SQLServerJDBCModuleFactory.class,
    importModuleClass, exportModuleClass);

  @Test
  public void arguments_required_long() {
    List<String> args = Arrays.asList("--import=microsoft-sql-server", "--import-username=name-user",
      "--import-password=abc1 23=456", "--import-server-name=the-server-name", "--import-database=dbname",
      "--import-disable-encryption", "--export=microsoft-sql-server", "--export-username=name-another-user",
      "--export-password=2bcd123=456", "--export-server-name=another-server", "--export-database=another-db-name");

    // test parameters for import module
    HashMap<String, String> expectedValuesImport = new HashMap<String, String>();
    expectedValuesImport.put("username", "name-user");
    expectedValuesImport.put("password", "abc1 23=456");
    expectedValuesImport.put("server-name", "the-server-name");
    expectedValuesImport.put("database", "dbname");
    expectedValuesImport.put("disable-encryption", "true");

    // test parameters for export module
    HashMap<String, String> expectedValuesExport = new HashMap<String, String>();
    expectedValuesExport.put("username", "name-another-user");
    expectedValuesExport.put("password", "2bcd123=456");
    expectedValuesExport.put("server-name", "another-server");
    expectedValuesExport.put("database", "another-db-name");

    ModuleFactoryTestHelper.validate_arguments(testHelper, args, expectedValuesImport, expectedValuesExport);
  }

  @Test
  public void arguments_required_short() {
    List<String> args = Arrays.asList("-i", "microsoft-sql-server", "-iu", "name-user", "-ip", "abc1 23=456", "-is",
      "the-server-name", "-idb", "dbname", "-e", "microsoft-sql-server", "-eu", "name-another-user", "-ep",
      "2bcd123=456", "-es", "another-server", "-ede", "-edb", "another-db-name");

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
    expectedValuesExport.put("disable-encryption", "true");

    ModuleFactoryTestHelper.validate_arguments(testHelper, args, expectedValuesImport, expectedValuesExport);
  }

  @Test
  public void arguments_instanceName_short() {
    List<String> args = Arrays.asList("-i", "microsoft-sql-server", "-iu", "name-user", "-ip", "abc1 23=456", "-is",
      "the-server-name", "-idb", "dbname", "-iin", "name-for-the-instance", "-e", "microsoft-sql-server", "-eu",
      "name-another-user", "-ep", "2bcd123=456", "-es", "another-server", "-edb", "another-db-name", "-ein",
      "name-for-another-instance");

    // test parameters for import module
    HashMap<String, String> expectedValuesImport = new HashMap<String, String>();
    expectedValuesImport.put("username", "name-user");
    expectedValuesImport.put("password", "abc1 23=456");
    expectedValuesImport.put("server-name", "the-server-name");
    expectedValuesImport.put("database", "dbname");
    expectedValuesImport.put("instance-name", "name-for-the-instance");

    // test parameters for export module
    HashMap<String, String> expectedValuesExport = new HashMap<String, String>();
    expectedValuesExport.put("username", "name-another-user");
    expectedValuesExport.put("password", "2bcd123=456");
    expectedValuesExport.put("server-name", "another-server");
    expectedValuesExport.put("database", "another-db-name");
    expectedValuesExport.put("instance-name", "name-for-another-instance");

    ModuleFactoryTestHelper.validate_arguments(testHelper, args, expectedValuesImport, expectedValuesExport);
  }

  @Test
  public void arguments_instanceName_long() {
    List<String> args = Arrays.asList("--import=microsoft-sql-server", "--import-username=name-user",
      "--import-password=abc1 23=456", "--import-server-name=the-server-name", "--import-database=dbname",
      "--import-instance-name=name-for-the-instance", "--export=microsoft-sql-server",
      "--export-username=name-another-user", "--export-password=2bcd123=456", "--export-server-name=another-server",
      "--export-database=another-db-name", "--export-instance-name=name-for-another-instance");

    // test parameters for import module
    HashMap<String, String> expectedValuesImport = new HashMap<String, String>();
    expectedValuesImport.put("username", "name-user");
    expectedValuesImport.put("password", "abc1 23=456");
    expectedValuesImport.put("server-name", "the-server-name");
    expectedValuesImport.put("database", "dbname");
    expectedValuesImport.put("instance-name", "name-for-the-instance");

    // test parameters for export module
    HashMap<String, String> expectedValuesExport = new HashMap<String, String>();
    expectedValuesExport.put("username", "name-another-user");
    expectedValuesExport.put("password", "2bcd123=456");
    expectedValuesExport.put("server-name", "another-server");
    expectedValuesExport.put("database", "another-db-name");
    expectedValuesExport.put("instance-name", "name-for-another-instance");

    ModuleFactoryTestHelper.validate_arguments(testHelper, args, expectedValuesImport, expectedValuesExport);
  }

  @Test
  public void arguments_portNumber_short() {
    List<String> args = Arrays.asList("-i", "microsoft-sql-server", "-iu", "name-user", "-ip", "abc1 23=456", "-is",
      "the-server-name", "-idb", "dbname", "-ipn", "1234", "-e", "microsoft-sql-server", "-eu", "name-another-user",
      "-ep", "2bcd123=456", "-es", "another-server", "-edb", "another-db-name", "-epn", "4321");

    // test parameters for import module
    HashMap<String, String> expectedValuesImport = new HashMap<String, String>();
    expectedValuesImport.put("username", "name-user");
    expectedValuesImport.put("password", "abc1 23=456");
    expectedValuesImport.put("server-name", "the-server-name");
    expectedValuesImport.put("database", "dbname");
    expectedValuesImport.put("port-number", "1234");

    // test parameters for export module
    HashMap<String, String> expectedValuesExport = new HashMap<String, String>();
    expectedValuesExport.put("username", "name-another-user");
    expectedValuesExport.put("password", "2bcd123=456");
    expectedValuesExport.put("server-name", "another-server");
    expectedValuesExport.put("database", "another-db-name");
    expectedValuesExport.put("port-number", "4321");

    ModuleFactoryTestHelper.validate_arguments(testHelper, args, expectedValuesImport, expectedValuesExport);
  }

  @Test
  public void arguments_portNumber_long() {
    List<String> args = Arrays.asList("--import=microsoft-sql-server", "--import-username=name-user",
      "--import-password=abc1 23=456", "--import-server-name=the-server-name", "--import-database=dbname",
      "--import-port-number=1234", "--export=microsoft-sql-server", "--export-username=name-another-user",
      "--export-password=2bcd123=456", "--export-server-name=another-server", "--export-database=another-db-name",
      "--export-port-number=4321");

    // test parameters for import module
    HashMap<String, String> expectedValuesImport = new HashMap<String, String>();
    expectedValuesImport.put("username", "name-user");
    expectedValuesImport.put("password", "abc1 23=456");
    expectedValuesImport.put("server-name", "the-server-name");
    expectedValuesImport.put("database", "dbname");
    expectedValuesImport.put("port-number", "1234");

    // test parameters for export module
    HashMap<String, String> expectedValuesExport = new HashMap<String, String>();
    expectedValuesExport.put("username", "name-another-user");
    expectedValuesExport.put("password", "2bcd123=456");
    expectedValuesExport.put("server-name", "another-server");
    expectedValuesExport.put("database", "another-db-name");
    expectedValuesExport.put("port-number", "4321");

    ModuleFactoryTestHelper.validate_arguments(testHelper, args, expectedValuesImport, expectedValuesExport);
  }
}
