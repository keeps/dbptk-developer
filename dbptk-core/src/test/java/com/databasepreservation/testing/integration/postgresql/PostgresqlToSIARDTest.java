/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.testing.integration.postgresql;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.databasepreservation.Main;
import com.databasepreservation.modules.postgresql.PostgreSQLModuleFactory;
import com.databasepreservation.testing.utils.TestingHelper;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */

@Test(groups = {"database-test"})
public class PostgresqlToSIARDTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresqlToSIARDTest.class);

  // Test containers for source and target databases
  private final PostgreSQLContainer<?> postgresSource = new PostgreSQLContainer<>(
    DockerImageName.parse("postgres:16-alpine")).withUsername("testuser").withPassword("testpass")
    .withDatabaseName("testdb");
  private Path tmpFolderSIARD;

  @BeforeClass
  void startContainer() {
    postgresSource.start();
  }

  @AfterClass
  void stopContainer() {
    postgresSource.stop();
  }

  @BeforeMethod
  void setUp() throws IOException {
    tmpFolderSIARD = Files.createTempDirectory("dbptk-postgres-siard-test-");
  }

  @AfterMethod
  void cleanUp() throws IOException, SQLException {
    LOGGER.debug("Cleaning up after test...");
    TestingHelper.cleanDatabase(postgresSource);

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

    tmpFolderSIARD.toFile().delete();
  }

  @Test
  void testConnections() throws Exception {
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
  }

  @Test
  void testExternalLobsSIARD22TypeClob() throws SQLException, IOException {
    LOGGER.info("Starting testExternalLobsSIARD22TypeClob test...");
    try (Connection conn = DriverManager.getConnection(postgresSource.getJdbcUrl(), postgresSource.getUsername(),
      postgresSource.getPassword());) {
      // 9 "byte" (character) string and 12 "byte" string
      String clob9 = "123456789";
      String clob12 = "ABCDEFGHIJKL";

      String tableName = TestingHelper.createUniqueTableName("data_storage");

      String createTableSQL = String
        .format("CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, payload TEXT NOT NULL)", tableName);

      TestingHelper.createTableIfNotExists(postgresSource, createTableSQL);

      String insertSql = "INSERT INTO " + tableName + " (payload) VALUES (?)";
      try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
        pstmt.setString(1, clob9);
        pstmt.executeUpdate();
        LOGGER.info("Success! 9 bytes stored.");
      }

      try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
        pstmt.setString(1, clob12);
        pstmt.executeUpdate();
        LOGGER.info("Success! 12 bytes stored.");
      }
    }

    Path siardPath = tmpFolderSIARD.resolve("siard22-external-clobs-postgres-test.siard");

    // convert from the database to SIARD
    PostgreSQLModuleFactory postgresModuleFactory = new PostgreSQLModuleFactory();
    String[] commandToExportToSIARD2 = TestingHelper.getCommandToExportToSIARD2(postgresModuleFactory.getModuleName(),
      postgresSource, siardPath.toString(), "2.2", true, "10", "10");
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(commandToExportToSIARD2), 0);

    // Verify that the SIARD file was created
    assertTrue(Files.exists(siardPath), "SIARD file should exist after export.");

    assertTrue(Files.exists(tmpFolderSIARD.resolve("siard22-external-clobs-postgres-test.siard_lobs")),
      "External LOBs folder should exist.");

    // Files.list returns a Stream of all files in the directory
    try (Stream<Path> stream = Files.find(tmpFolderSIARD.resolve("siard22-external-clobs-postgres-test.siard_lobs"), 3,
      (path, attr) -> attr.isRegularFile() && path.getFileName().toString().endsWith(".txt"))) {

      assertTrue(stream.findAny().isPresent(), "The file with .txt extension should exist in the external LOB folder.");
    }

    try (
      FileSystem zipfs = FileSystems.newFileSystem(URI.create("jar:" + siardPath.toUri()), Map.of("create", "false"))) {
      Path root = zipfs.getPath("content/");
      boolean folderFound = false;
      // 5. Walk through the ENTIRE zip (recursive) to find any matching folder
      try (Stream<Path> allPaths = Files.walk(root)) {

        // Search for any directory that matches "lob*"
        Path match = allPaths.filter(Files::isDirectory) // Only look at folders
          .filter(path -> {
            // Get the name of the current folder in the path
            Path fileName = path.getFileName();
            return fileName != null && fileName.toString().startsWith("lob");
          }).findFirst().orElse(null);

        if (match != null) {
          folderFound = true;
        }
      }

      assertFalse(folderFound, "All LOB must be stored inline");
    }
  }

  @Test
  void testExternalLobsSIARD22TypeBlob() throws SQLException, IOException {
    LOGGER.info("Starting testExternalLobsSIARD22TypeBlob test...");
    try (Connection conn = DriverManager.getConnection(postgresSource.getJdbcUrl(), postgresSource.getUsername(),
      postgresSource.getPassword());) {
      // Your 9 bytes of data
      byte[] blob9 = new byte[] {(byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x04, (byte) 0x05, (byte) 0x06,
        (byte) 0x07, (byte) 0x08, (byte) 0x09};

      // Your 12 bytes of data
      byte[] blob12 = new byte[] {(byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x04, (byte) 0x05, (byte) 0x06,
        (byte) 0x07, (byte) 0x08, (byte) 0x09, (byte) 0x09, (byte) 0x09, (byte) 0x09};

      String tableName = TestingHelper.createUniqueTableName("data_storage");

      String createTableSQL = String
        .format("CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, payload BYTEA NOT NULL)", tableName);

      TestingHelper.createTableIfNotExists(postgresSource, createTableSQL);

      // 2. Insert the 9 bytes
      String insertSql = "INSERT INTO " + tableName + " (payload) VALUES (?)";
      try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
        pstmt.setBytes(1, blob9);
        pstmt.executeUpdate();
        LOGGER.info("Success! 9 bytes stored.");
      }

      try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
        pstmt.setBytes(1, blob12);
        pstmt.executeUpdate();
        LOGGER.info("Success! 12 bytes stored.");
      }
    }

    Path siardPath = tmpFolderSIARD.resolve("siard22-external-lobs-postgres-test.siard");

    // convert from the database to SIARD
    PostgreSQLModuleFactory postgresModuleFactory = new PostgreSQLModuleFactory();
    String[] commandToExportToSIARD2 = TestingHelper.getCommandToExportToSIARD2(postgresModuleFactory.getModuleName(),
      postgresSource, siardPath.toString(), "2.2", true, "10", "10");
    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(commandToExportToSIARD2), 0);

    // Verify that the SIARD file was created
    assertTrue(Files.exists(siardPath), "SIARD file should exist after export.");

    assertTrue(Files.exists(tmpFolderSIARD.resolve("siard22-external-lobs-postgres-test.siard_lobs")),
      "LOBs folder should exist.");

    // Files.list returns a Stream of all files in the directory
    try (Stream<Path> stream = Files.find(tmpFolderSIARD.resolve("siard22-external-lobs-postgres-test.siard_lobs"), 3,
      (path, attr) -> attr.isRegularFile() && path.getFileName().toString().endsWith(".bin"))) {

      assertTrue(stream.findAny().isPresent(), "The file with .bin extension should exist in the external LOB folder.");
    }

    try (
      FileSystem zipfs = FileSystems.newFileSystem(URI.create("jar:" + siardPath.toUri()), Map.of("create", "false"))) {
      Path pathInZip = zipfs.getPath("content/schema1/table1/lob2/record1.bin");

      assertTrue(Files.exists(pathInZip),
        "The file content/schema1/table1/lob2/record1.bin should exist inside the ZIP archive.");
    }
  }
}
