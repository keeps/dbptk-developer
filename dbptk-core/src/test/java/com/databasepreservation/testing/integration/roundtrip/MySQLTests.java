/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
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

import org.testcontainers.containers.Container;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.databasepreservation.Main;
import com.databasepreservation.modules.mysql.MySQLModuleFactory;
import com.databasepreservation.testing.integration.roundtrip.differences.DumpDiffExpectations;
import com.databasepreservation.testing.integration.roundtrip.differences.MySqlDumpDiffExpectations;
import com.databasepreservation.testing.utils.TestingHelper;

/**
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@Test(groups = {"database-roundtrip"})
public class MySQLTests {

  // Test containers for source and target databases
  private final MySQLContainer<?> mySQLContainerSource = new MySQLContainer<>(DockerImageName.parse("mysql:5.7.34"))
    .withDatabaseName("testdb").withUsername("root").withPassword("rootpass").withCommand("--max_allowed_packet=64M");

  private final MySQLContainer<?> mySQLContainerTarget = new MySQLContainer<>(DockerImageName.parse("mysql:5.7.34"))
    .withUsername("root").withPassword("rootpass").withDatabaseName("targettestdb")
    .withCommand("--max_allowed_packet=64M");

  private Path tmpFolderSIARD;
  private Path siardPath;

  @BeforeClass
  void startContainer() throws SQLException {
    mySQLContainerSource.start();
    mySQLContainerTarget.start();

    populateInitialData(mySQLContainerSource, mySQLContainerSource);
  }

  @AfterClass
  void stopContainer() {
    mySQLContainerSource.stop();
    mySQLContainerTarget.stop();
  }

  @BeforeMethod
  void setUp() throws IOException {
    tmpFolderSIARD = Files.createTempDirectory("dpttest_siard");
    siardPath = tmpFolderSIARD.resolve("dbptk.siard");
  }

  @AfterMethod
  void cleanUp() throws IOException, SQLException {
    TestingHelper.cleanDatabase(mySQLContainerTarget);

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
      Connection conn = DriverManager.getConnection(mySQLContainerSource.getJdbcUrl(),
        mySQLContainerSource.getUsername(), mySQLContainerSource.getPassword());
      Statement stmt = conn.createStatement()) {
      assertTrue(conn.isValid(2), "Main DB connection should be valid");
      ResultSet rs = stmt.executeQuery("SELECT DATABASE()");
      rs.next();
      assertEquals(rs.getString(1), "testdb", "Database name should be 'testdb'");
    }

    // Test db_two connection

    try (
      Connection conn = DriverManager.getConnection(mySQLContainerTarget.getJdbcUrl(),
        mySQLContainerTarget.getUsername(), mySQLContainerTarget.getPassword());
      Statement stmt = conn.createStatement()) {
      assertTrue(conn.isValid(2), "Main DB connection should be valid");
      ResultSet rs = stmt.executeQuery("SELECT DATABASE()");
      rs.next();
      assertEquals(rs.getString(1), "targettestdb", "Database name should be 'targettestdb'");
    }
  }

  @Test(dependsOnMethods = {"testConnections"})
  public void round_trip_test_siard_1() throws IOException, InterruptedException {
    // convert from the database to SIARD
    MySQLModuleFactory mySQLModuleFactory = new MySQLModuleFactory();
    String[] forwardConversionArguments = TestingHelper.getCommandToExportToSIARD1(mySQLModuleFactory.getModuleName(),
      mySQLContainerSource, siardPath.toString(), false);
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(forwardConversionArguments), 0);

    // and if that succeeded, convert back to the database
    String[] backwardConversionArguments = TestingHelper
      .getCommandToImportFromSIARD1(mySQLModuleFactory.getModuleName(), mySQLContainerTarget, siardPath.toString());
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(backwardConversionArguments), 0);

    // Get the dumps to compare
    Container.ExecResult sourceDump = mySQLContainerSource
      .execInContainer(TestingHelper.getDatabaseDumpCommand(mySQLContainerSource));
    Container.ExecResult targetDump = mySQLContainerTarget
      .execInContainer(TestingHelper.getDatabaseDumpCommand(mySQLContainerTarget));
    DumpDiffExpectations dumpDiffExpectations = new MySqlDumpDiffExpectations();
    dumpDiffExpectations.dumpsRepresentTheSameInformation(sourceDump.getStdout(), targetDump.getStdout());
  }

  @Test(dependsOnMethods = {"testConnections"})
  public void round_trip_test_siard_2_1() throws IOException, InterruptedException {
    // convert from the database to SIARD
    MySQLModuleFactory mySQLModuleFactory = new MySQLModuleFactory();
    String[] forwardConversionArguments = TestingHelper.getCommandToExportToSIARD2(mySQLModuleFactory.getModuleName(),
      mySQLContainerSource, siardPath.toString(), "2.1", false);
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(forwardConversionArguments), 0);

    // and if that succeeded, convert back to the database
    String[] backwardConversionArguments = TestingHelper.getImportFromSIARD2Command(mySQLModuleFactory.getModuleName(),
      mySQLContainerTarget, siardPath.toString());
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(backwardConversionArguments), 0);

    // Get the dumps to compare
    Container.ExecResult sourceDump = mySQLContainerSource
      .execInContainer(TestingHelper.getDatabaseDumpCommand(mySQLContainerSource));
    Container.ExecResult targetDump = mySQLContainerTarget
      .execInContainer(TestingHelper.getDatabaseDumpCommand(mySQLContainerTarget));
    DumpDiffExpectations dumpDiffExpectations = new MySqlDumpDiffExpectations();
    dumpDiffExpectations.dumpsRepresentTheSameInformation(sourceDump.getStdout(), targetDump.getStdout());
  }

  @Test(dependsOnMethods = {"testConnections"})
  public void round_trip_test_siard_2_1_with_external_lobs() throws IOException, InterruptedException {
    // convert from the database to SIARD
    MySQLModuleFactory mySQLModuleFactory = new MySQLModuleFactory();
    String[] forwardConversionArguments = TestingHelper.getCommandToExportToSIARD2(mySQLModuleFactory.getModuleName(),
      mySQLContainerSource, siardPath.toString(), "2.1", true);
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(forwardConversionArguments), 0);

    // and if that succeeded, convert back to the database
    String[] backwardConversionArguments = TestingHelper.getImportFromSIARD2Command(mySQLModuleFactory.getModuleName(),
      mySQLContainerTarget, siardPath.toString());
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(backwardConversionArguments), 0);

    // Get the dumps to compare
    Container.ExecResult sourceDump = mySQLContainerSource
      .execInContainer(TestingHelper.getDatabaseDumpCommand(mySQLContainerSource));
    Container.ExecResult targetDump = mySQLContainerTarget
      .execInContainer(TestingHelper.getDatabaseDumpCommand(mySQLContainerTarget));
    DumpDiffExpectations dumpDiffExpectations = new MySqlDumpDiffExpectations();
    dumpDiffExpectations.dumpsRepresentTheSameInformation(sourceDump.getStdout(), targetDump.getStdout());
  }

  @Test(dependsOnMethods = {"testConnections"})
  public void round_trip_test_siard_2_2() throws IOException, InterruptedException {
    // convert from the database to SIARD
    MySQLModuleFactory mySQLModuleFactory = new MySQLModuleFactory();
    String[] forwardConversionArguments = TestingHelper.getCommandToExportToSIARD2(mySQLModuleFactory.getModuleName(),
      mySQLContainerSource, siardPath.toString(), "2.2", false);
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(forwardConversionArguments), 0);

    // and if that succeeded, convert back to the database
    String[] backwardConversionArguments = TestingHelper.getImportFromSIARD2Command(mySQLModuleFactory.getModuleName(),
      mySQLContainerTarget, siardPath.toString());
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(backwardConversionArguments), 0);

    // Get the dumps to compare
    Container.ExecResult sourceDump = mySQLContainerSource
      .execInContainer(TestingHelper.getDatabaseDumpCommand(mySQLContainerSource));
    Container.ExecResult targetDump = mySQLContainerTarget
      .execInContainer(TestingHelper.getDatabaseDumpCommand(mySQLContainerTarget));
    DumpDiffExpectations dumpDiffExpectations = new MySqlDumpDiffExpectations();
    dumpDiffExpectations.dumpsRepresentTheSameInformation(sourceDump.getStdout(), targetDump.getStdout());
  }

  @Test(dependsOnMethods = {"testConnections"})
  public void round_trip_test_siard_2_2_with_external_lobs() throws IOException, InterruptedException {
    // convert from the database to SIARD
    MySQLModuleFactory mySQLModuleFactory = new MySQLModuleFactory();
    String[] forwardConversionArguments = TestingHelper.getCommandToExportToSIARD2(mySQLModuleFactory.getModuleName(),
      mySQLContainerSource, siardPath.toString(), "2.2", true);
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(forwardConversionArguments), 0);

    // and if that succeeded, convert back to the database
    String[] backwardConversionArguments = TestingHelper.getImportFromSIARD2Command(mySQLModuleFactory.getModuleName(),
      mySQLContainerTarget, siardPath.toString());
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(backwardConversionArguments), 0);

    // Get the dumps to compare
    Container.ExecResult sourceDump = mySQLContainerSource
      .execInContainer(TestingHelper.getDatabaseDumpCommand(mySQLContainerSource));
    Container.ExecResult targetDump = mySQLContainerTarget
      .execInContainer(TestingHelper.getDatabaseDumpCommand(mySQLContainerTarget));
    DumpDiffExpectations dumpDiffExpectations = new MySqlDumpDiffExpectations();
    dumpDiffExpectations.dumpsRepresentTheSameInformation(sourceDump.getStdout(), targetDump.getStdout());
  }

  private void populateInitialData(MySQLContainer<?> mySQLContainerSource, MySQLContainer<?> mySQLContainerTarget)
    throws SQLException {
    try (
      Connection conn = DriverManager.getConnection(mySQLContainerSource.getJdbcUrl(),
        mySQLContainerSource.getUsername(), mySQLContainerSource.getPassword());

      Connection connTarget = DriverManager.getConnection(mySQLContainerTarget.getJdbcUrl(),
        mySQLContainerTarget.getUsername(), mySQLContainerTarget.getPassword());

      Statement stmtTarget = connTarget.createStatement();

      Statement stmtSource = conn.createStatement()) {
      // Drop table if it exists
      stmtSource.execute("DROP TABLE IF EXISTS dataTypes");
      stmtTarget.execute("DROP TABLE IF EXISTS dataTypes");

      // Create a table with comments
      stmtSource.execute("""
        CREATE TABLE test_comments (ID INT COMMENT 'this is an integer') COMMENT 'this is a table';
        """);
      stmtSource.execute("""
        INSERT INTO test_comments VALUE (1);
        """);

      // Create table with all the listed data types
      stmtSource.execute("""
        CREATE TABLE dataTypes (
            tinyint10         TINYINT(10),
            tinyint_col       TINYINT,
            smallint_col      SMALLINT,
            mediumint10       MEDIUMINT(10),
            mediumint_col     MEDIUMINT,
            int_col           INT,
            bigint30          BIGINT(30),
            bigint_col        BIGINT,
            decimal_col       DECIMAL,
            numeric_col       NUMERIC,
            float1            FLOAT,
            float2            FLOAT,
            float9            FLOAT(9),
            float12           FLOAT(12),
            float12_0         FLOAT(12,0),
            float53           FLOAT(53),
            float8_3          FLOAT(8,3),
            double_col        DOUBLE,
            double22_0        DOUBLE(22,0),
            double10_2        DOUBLE(10,2),
            date1             DATE,
            date2             DATE,
            datetime6         DATETIME(6),
            year1             YEAR(4),
            year2             YEAR(4),
            year3             YEAR(4),
            year4             YEAR(4),
            year5             YEAR(4),
            year6             YEAR(4),
            year7             YEAR(4),
            year8             YEAR(4),
            year9             YEAR(4),
            year10            YEAR(4),
            char3             CHAR(3),
            char253_null      CHAR(253),
            char253_notnull   CHAR(253) NOT NULL,
            char255_null      CHAR(255),
            char255_notnull   CHAR(255) NOT NULL,
            varchar1024       VARCHAR(1024),
            varchar255        VARCHAR(255),
            tinyblob_col      TINYBLOB,
            blob_col          BLOB,
            mediumblob_col    MEDIUMBLOB,
            longblob_col      LONGBLOB,
            tinytext_col      TINYTEXT,
            text_col          TEXT,
            mediumtext_col    MEDIUMTEXT,
            longtext_col      LONGTEXT
        )""");

      // Insert values
      stmtSource.execute("""
        INSERT INTO dataTypes VALUES (
            1, 1, 123, 123, 123, 123, -9223372036854775808, 9223372036854775807,
            123, 123, 12345.123, 123456789012, 12345.123, 12345.123, 12345.123,
            12345.123, 12345.123, 1234567890.12345, 1234567890.12345, 34567890.12,
            '9999-12-31', '2015-01-01', '9999-12-31 23:59:59.999999',
            2015, '0', 1, '1', 99, '99', 70, '70', 69, '69',
            'abc', NULL, RPAD('a',253,'a'), NULL, RPAD('b',255,'b'),
            NULL, RPAD('c',255,'c'), NULL, NULL, NULL, NULL, NULL, NULL, NULL, REPEAT('long text ',10000)
        )""");
    }
  }
}
