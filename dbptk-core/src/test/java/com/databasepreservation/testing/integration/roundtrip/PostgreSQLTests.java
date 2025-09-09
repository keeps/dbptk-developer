package com.databasepreservation.testing.integration.roundtrip;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;

import com.databasepreservation.modules.postgresql.PostgreSQLModuleFactory;
import com.databasepreservation.testing.utils.TestingHelper;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.databasepreservation.Main;
import com.databasepreservation.testing.integration.roundtrip.differences.DumpDiffExpectations;
import com.databasepreservation.testing.integration.roundtrip.differences.PostgreSqlDumpDiffExpectations;

/**
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */

@Test(groups = {"database-roundtrip"})
public class PostgreSQLTests {

  // Test containers for source and target databases
  private final PostgreSQLContainer<?> postgresSource = new PostgreSQLContainer<>(
    DockerImageName.parse("postgres:16-alpine")).withUsername("testuser").withPassword("testpass")
    .withDatabaseName("testdb");

  private final PostgreSQLContainer<?> postgresTarget = new PostgreSQLContainer<>(
    DockerImageName.parse("postgres:16-alpine")).withUsername("testuser").withPassword("testpass")
    .withDatabaseName("targettestdb");

  private Path tmpFolderSIARD;
  private Path siardPath;

  @BeforeClass
  void startContainer() throws SQLException {
    postgresSource.start();
    postgresTarget.start();

    populateInitialData(postgresSource, postgresTarget);
  }

  @AfterClass
  void stopContainer() {
    postgresSource.stop();
    postgresTarget.stop();
  }

  @BeforeMethod
  void setUp() throws IOException {
    tmpFolderSIARD = Files.createTempDirectory("dpttest_siard");
    siardPath = tmpFolderSIARD.resolve("dbptk.siard");
  }

  @AfterMethod
  void cleanUp() throws IOException, SQLException {
    TestingHelper.cleanTargetDatabase(postgresTarget);

    if (Files.notExists(tmpFolderSIARD)) {
      return; // Nothing to delete
    }

    // Walk the file tree, sort in reverse order so files are deleted before
    // directories
    try (var walk = Files.walk(tmpFolderSIARD)) {
      walk.sorted(Comparator.reverseOrder()).forEach(p -> {
        try {
          Files.delete(p);
        } catch (IOException e) {
          throw new RuntimeException("Failed to delete " + p, e);
        }
      });
    }
  }

  @Test
  public void testConnections() throws Exception {
    // Test main database connection
    try (
      Connection conn = DriverManager.getConnection(postgresSource.getJdbcUrl(), postgresSource.getUsername(),
        postgresSource.getPassword());
      Statement stmt = conn.createStatement()) {
      assertTrue(conn.isValid(2), "Main DB connection should be valid");
      ResultSet rs = stmt.executeQuery("SELECT current_database()");
      rs.next();
      assertEquals(rs.getString(1), "testdb", "Database name should be 'testdb'");
    }

    // Test db_two connection

    try (
      Connection conn = DriverManager.getConnection(postgresTarget.getJdbcUrl(), postgresTarget.getUsername(),
        postgresTarget.getPassword());
      Statement stmt = conn.createStatement()) {
      assertTrue(conn.isValid(2), "Main DB connection should be valid");
      ResultSet rs = stmt.executeQuery("SELECT current_database()");
      rs.next();
      assertEquals(rs.getString(1), "targettestdb", "Database name should be 'targettestdb'");
    }
  }

  @Test(dependsOnMethods = {"testConnections"})
  public void round_trip_test_siard_1() throws IOException, InterruptedException {
    // convert from the database to SIARD
    PostgreSQLModuleFactory postgresModuleFactory = new PostgreSQLModuleFactory();
    String[] forwardConversionArguments = TestingHelper.getCommandToExportToSIARD1(postgresModuleFactory.getModuleName(),
      postgresSource, siardPath.toString(), false);
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(forwardConversionArguments), 0);

    // and if that succeeded, convert back to the database
    String[] backwardConversionArguments = TestingHelper
      .getCommandToImportFromSIARD1(postgresModuleFactory.getModuleName(), postgresTarget, siardPath.toString());
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(backwardConversionArguments), 0);

    // Get the dumps to compare
    Container.ExecResult sourceDump = postgresSource
      .execInContainer(TestingHelper.getDatabaseDumpCommand(postgresSource));
    Container.ExecResult targetDump = postgresTarget
      .execInContainer(TestingHelper.getDatabaseDumpCommand(postgresTarget));
    DumpDiffExpectations dumpDiffExpectations = new PostgreSqlDumpDiffExpectations();
    dumpDiffExpectations.dumpsRepresentTheSameInformation(sourceDump.getStdout(), targetDump.getStdout());
  }

  @Test(dependsOnMethods = {"testConnections"})
  public void round_trip_test_siard_2_1() throws IOException, InterruptedException {
    // convert from the database to SIARD
    PostgreSQLModuleFactory postgresModuleFactory = new PostgreSQLModuleFactory();
    String[] forwardConversionArguments = TestingHelper.getExportToSIARD2Command(postgresModuleFactory.getModuleName(),
      postgresSource, siardPath.toString(), "2.1", false);
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(forwardConversionArguments), 0);

    // and if that succeeded, convert back to the database
    String[] backwardConversionArguments = TestingHelper
      .getImportFromSIARD2Command(postgresModuleFactory.getModuleName(), postgresTarget, siardPath.toString());
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(backwardConversionArguments), 0);

    // Get the dumps to compare
    Container.ExecResult sourceDump = postgresSource
      .execInContainer(TestingHelper.getDatabaseDumpCommand(postgresSource));
    Container.ExecResult targetDump = postgresTarget
      .execInContainer(TestingHelper.getDatabaseDumpCommand(postgresTarget));
    DumpDiffExpectations dumpDiffExpectations = new PostgreSqlDumpDiffExpectations();
    dumpDiffExpectations.dumpsRepresentTheSameInformation(sourceDump.getStdout(), targetDump.getStdout());
  }

  @Test(dependsOnMethods = {"testConnections"})
  public void round_trip_test_siard_2_1_with_external_lobs() throws IOException, InterruptedException {
    // convert from the database to SIARD
    PostgreSQLModuleFactory postgresModuleFactory = new PostgreSQLModuleFactory();
    String[] forwardConversionArguments = TestingHelper.getExportToSIARD2Command(postgresModuleFactory.getModuleName(),
      postgresSource, siardPath.toString(), "2.1", true);
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(forwardConversionArguments), 0);

    // and if that succeeded, convert back to the database
    String[] backwardConversionArguments = TestingHelper
      .getImportFromSIARD2Command(postgresModuleFactory.getModuleName(), postgresTarget, siardPath.toString());
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(backwardConversionArguments), 0);

    // Get the dumps to compare
    Container.ExecResult sourceDump = postgresSource
      .execInContainer(TestingHelper.getDatabaseDumpCommand(postgresSource));
    Container.ExecResult targetDump = postgresTarget
      .execInContainer(TestingHelper.getDatabaseDumpCommand(postgresTarget));
    DumpDiffExpectations dumpDiffExpectations = new PostgreSqlDumpDiffExpectations();
    dumpDiffExpectations.dumpsRepresentTheSameInformation(sourceDump.getStdout(), targetDump.getStdout());
  }

  @Test(dependsOnMethods = {"testConnections"})
  public void round_trip_test_siard_2_2() throws IOException, InterruptedException {
    // convert from the database to SIARD
    PostgreSQLModuleFactory postgresModuleFactory = new PostgreSQLModuleFactory();
    String[] forwardConversionArguments = TestingHelper.getExportToSIARD2Command(postgresModuleFactory.getModuleName(),
      postgresSource, siardPath.toString(), "2.2", false);
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(forwardConversionArguments), 0);

    // and if that succeeded, convert back to the database
    String[] backwardConversionArguments = TestingHelper
      .getImportFromSIARD2Command(postgresModuleFactory.getModuleName(), postgresTarget, siardPath.toString());
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(backwardConversionArguments), 0);

    // Get the dumps to compare
    Container.ExecResult sourceDump = postgresSource
      .execInContainer(TestingHelper.getDatabaseDumpCommand(postgresSource));
    Container.ExecResult targetDump = postgresTarget
      .execInContainer(TestingHelper.getDatabaseDumpCommand(postgresTarget));
    DumpDiffExpectations dumpDiffExpectations = new PostgreSqlDumpDiffExpectations();
    dumpDiffExpectations.dumpsRepresentTheSameInformation(sourceDump.getStdout(), targetDump.getStdout());
  }

  @Test(dependsOnMethods = {"testConnections"})
  public void round_trip_test_siard_2_2_with_external_lobs() throws IOException, InterruptedException {
    // convert from the database to SIARD
    PostgreSQLModuleFactory postgresModuleFactory = new PostgreSQLModuleFactory();
    String[] forwardConversionArguments = TestingHelper.getExportToSIARD2Command(postgresModuleFactory.getModuleName(),
      postgresSource, siardPath.toString(), "2.2", true);
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(forwardConversionArguments), 0);

    // and if that succeeded, convert back to the database
    String[] backwardConversionArguments = TestingHelper
      .getImportFromSIARD2Command(postgresModuleFactory.getModuleName(), postgresTarget, siardPath.toString());
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(backwardConversionArguments), 0);

    // Get the dumps to compare
    Container.ExecResult sourceDump = postgresSource
      .execInContainer(TestingHelper.getDatabaseDumpCommand(postgresSource));
    Container.ExecResult targetDump = postgresTarget
      .execInContainer(TestingHelper.getDatabaseDumpCommand(postgresTarget));
    DumpDiffExpectations dumpDiffExpectations = new PostgreSqlDumpDiffExpectations();
    dumpDiffExpectations.dumpsRepresentTheSameInformation(sourceDump.getStdout(), targetDump.getStdout());
  }

  private void populateInitialData(PostgreSQLContainer<?> postgresSource, PostgreSQLContainer<?> postgresTarget)
    throws SQLException {
    try (
      Connection conn = DriverManager.getConnection(postgresSource.getJdbcUrl(), postgresSource.getUsername(),
        postgresSource.getPassword());

      Connection connTarget = DriverManager.getConnection(postgresTarget.getJdbcUrl(), postgresTarget.getUsername(),
        postgresTarget.getPassword());

      Statement stmtTarget = connTarget.createStatement();

      Statement stmtSource = conn.createStatement()) {
      // Drop table if it exists
      stmtSource.execute("DROP TABLE IF EXISTS dataTypes");
      stmtTarget.execute("DROP TABLE IF EXISTS dataTypes");

      // Create table with all the listed data types
      stmtSource.execute("""
            CREATE TABLE dataTypes (
                col_bit_varying BIT VARYING(5),
                col_bit_fixed BIT(5),
                col_char CHAR NOT NULL,
                col_bigint BIGINT,
                col_boolean BOOLEAN,
                col_bytea1 BYTEA,
                col_bytea2 BYTEA,
                col_bytea3 BYTEA,
                col_char1 CHARACTER(1),
                col_varchar CHARACTER VARYING,
                col_date DATE,
                col_double DOUBLE PRECISION,
                col_integer INTEGER,
                col_name NAME,
                col_numeric NUMERIC,
                col_real REAL,
                col_smallint SMALLINT,
                col_text TEXT,
                col_time_tz1 TIME WITH TIME ZONE,
                col_time_tz2 TIME WITH TIME ZONE
            )
        """);

      // Insert values
      stmtSource.execute("""
            INSERT INTO dataTypes
            (col_bit_varying, col_bit_fixed, col_char, col_bigint, col_boolean,
             col_bytea1, col_bytea2, col_bytea3, col_char1, col_varchar,
             col_date, col_double, col_integer, col_name, col_numeric,
             col_real, col_smallint, col_text, col_time_tz1, col_time_tz2)
            VALUES (
                B'101', B'01010', 'a', 123, TRUE,
                decode('013d7d16d7ad4fefb61bd95b765c8ceb','hex'),
                decode('00000000000000000000000000000000','hex'),
                NULL, 'a', 'abc',
                '2015-01-01', 0.123456789012345, 2147483647, 'abc', 2147483647,
                0.123456, 32767, 'abc',
                '23:59:59.999 PST', '23:59:59.999+05:30'
            )
        """);
    }
  }
}
