/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.testing.integration.postgresql;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.databasepreservation.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;

/**
 * Integration test that sets up a PostgreSQL container with a table containing
 * external references to files stored in a MinIO container.
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@Test(groups = {"database-test"})
public class PostgreSQLWithMinIOExternalLobsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLWithMinIOExternalLobsTest.class);

  private static final String BUCKET_NAME = "external-lobs";
  private static final String TABLE_NAME = "documents";

  // Stressed to 20,000 rows
  private static final int NUM_ROWS = 20000;

  private static final int MINIO_PORT = 9000;
  private static final String MINIO_ACCESS_KEY = "minioadmin";
  private static final String MINIO_SECRET_KEY = "minioadmin";
  private List<String> fileNames;
  private PostgreSQLContainer<?> postgresContainer;
  private GenericContainer<?> minioContainer;
  private MinioClient minioClient;
  private Path tmpFolderSIARD;
  private Path importConfigPath;

  @BeforeClass
  void setUp() throws Exception {
    tmpFolderSIARD = Files.createTempDirectory("dbptk-postgres-minio-external-filter-siard-test-");

    // Start PostgreSQL container
    postgresContainer = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine")).withUsername("testuser")
      .withPassword("testpass").withDatabaseName("testdb");
    postgresContainer.start();

    // Start MinIO container
    minioContainer = new GenericContainer<>(DockerImageName.parse("minio/minio:latest")).withExposedPorts(MINIO_PORT)
      .withEnv("MINIO_ROOT_USER", MINIO_ACCESS_KEY).withEnv("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
      .withCommand("server /data")
      .waitingFor(new HttpWaitStrategy().forPath("/minio/health/ready").forPort(MINIO_PORT));
    minioContainer.start();

    // Create MinIO client
    String minioEndpoint = "http://" + minioContainer.getHost() + ":" + minioContainer.getMappedPort(MINIO_PORT);
    minioClient = MinioClient.builder().endpoint(minioEndpoint).credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY).build();

    // Create bucket
    minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).build());

    // Pre-generate the 100KB payload padding ONCE to save memory and CPU
    StringBuilder sb = new StringBuilder();
    String line = "This is line content padding to ensure the file exceeds 100KB in size.\n";
    while (sb.length() < 100 * 1024) {
      sb.append(line);
    }
    final byte[] paddingBytes = sb.toString().getBytes(StandardCharsets.UTF_8);

    // Generate list of file names
    fileNames = IntStream.rangeClosed(1, NUM_ROWS).mapToObj(i -> "document_" + i + ".txt").collect(Collectors.toList());

    LOGGER.info("Starting parallel upload of {} files to MinIO...", NUM_ROWS);

    // Upload files in PARALLEL to prevent the test from timing out, adding a UNIQUE
    // mark to each
    IntStream.rangeClosed(1, NUM_ROWS).parallel().forEach(i -> {
      String fileName = "document_" + i + ".txt";
      String rowMark = "ROW_INDEX:" + i + "\n";
      byte[] markBytes = rowMark.getBytes(StandardCharsets.UTF_8);

      // Concatenate the unique mark and the reused padding
      byte[] fileBytes = new byte[markBytes.length + paddingBytes.length];
      System.arraycopy(markBytes, 0, fileBytes, 0, markBytes.length);
      System.arraycopy(paddingBytes, 0, fileBytes, markBytes.length, paddingBytes.length);

      try {
        minioClient.putObject(PutObjectArgs.builder().bucket(BUCKET_NAME).object(fileName)
          .stream(new ByteArrayInputStream(fileBytes), fileBytes.length, -1).contentType("text/plain").build());
      } catch (Exception e) {
        throw new RuntimeException("Failed to upload file to MinIO: " + fileName, e);
      }
    });

    LOGGER.info("Finished uploading {} files to MinIO.", NUM_ROWS);

    // Create PostgreSQL table and insert rows using JDBC BATCHING
    try (Connection conn = DriverManager.getConnection(postgresContainer.getJdbcUrl(), postgresContainer.getUsername(),
      postgresContainer.getPassword())) {

      String createTableSQL = "CREATE TABLE " + TABLE_NAME + " (" + "id SERIAL PRIMARY KEY, "
        + "title VARCHAR(255) NOT NULL, " + "author VARCHAR(255) NOT NULL, " + "category VARCHAR(255) NOT NULL, "
        + "\"external-reference\" VARCHAR(255) NOT NULL" + ")";

      try (Statement stmt = conn.createStatement()) {
        stmt.execute(createTableSQL);
        LOGGER.info("Created table '{}'", TABLE_NAME);
      }

      String insertSQL = "INSERT INTO " + TABLE_NAME
        + " (title, author, category, \"external-reference\") VALUES (?, ?, ?, ?)";

      // Disable auto-commit for fast batch inserts
      conn.setAutoCommit(false);
      try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
        for (int i = 0; i < NUM_ROWS; i++) {
          pstmt.setString(1, "Document Title " + (i + 1));
          pstmt.setString(2, "Author " + (i + 1));
          pstmt.setString(3, "Category " + ((i % 3) + 1));
          pstmt.setString(4, fileNames.get(i));
          pstmt.addBatch();

          // Execute batch every 1,000 rows
          if ((i + 1) % 1000 == 0) {
            pstmt.executeBatch();
          }
        }
        pstmt.executeBatch(); // Flush any remaining rows
        conn.commit();
        LOGGER.info("Inserted {} rows into table '{}'", NUM_ROWS, TABLE_NAME);
      }
    }

    // Generate the import configuration YAML with actual container endpoints
    importConfigPath = generateImportConfig();
    LOGGER.info("Generated import config at: {}", importConfigPath);
  }

  private Path generateImportConfig() throws IOException {
    String minioEndpoint = "http://" + minioContainer.getHost() + ":" + minioContainer.getMappedPort(MINIO_PORT);

    try (InputStream templateStream = getClass().getClassLoader()
      .getResourceAsStream("postgresql-minio-external-lobs-config.yaml")) {
      assertNotNull(templateStream, "YAML config template should exist in test resources");

      String template = new String(templateStream.readAllBytes(), StandardCharsets.UTF_8);

      String resolvedConfig = template.replace("${POSTGRES_HOST}", postgresContainer.getHost())
        .replace("${POSTGRES_PORT}", String.valueOf(postgresContainer.getMappedPort(5432)))
        .replace("${MINIO_ENDPOINT}", minioEndpoint);

      Path configPath = tmpFolderSIARD.resolve("import-config.yaml");
      Files.writeString(configPath, resolvedConfig);
      return configPath;
    }
  }

  @AfterClass
  void tearDown() throws IOException {
    if (postgresContainer != null) {
      postgresContainer.stop();
    }
    if (minioContainer != null) {
      minioContainer.stop();
    }
  }

  @Test
  void testPostgresContainerIsRunning() {
    assertTrue(postgresContainer.isRunning(), "PostgreSQL container should be running");
  }

  @Test
  void testMinioContainerIsRunning() {
    assertTrue(minioContainer.isRunning(), "MinIO container should be running");
  }

  @Test
  void testMinioBucketExists() throws Exception {
    boolean exists = minioClient.bucketExists(BucketExistsArgs.builder().bucket(BUCKET_NAME).build());
    assertTrue(exists, "Bucket '" + BUCKET_NAME + "' should exist in MinIO");
  }

  @Test
  void testMinioFilesExist() {
    // Check files in parallel to prevent test timeout
    fileNames.parallelStream().forEach(fileName -> {
      try (GetObjectResponse response = minioClient
        .getObject(GetObjectArgs.builder().bucket(BUCKET_NAME).object(fileName).build())) {
        assertNotNull(response, "File '" + fileName + "' should exist in MinIO bucket");
      } catch (Exception e) {
        Assert.fail("Failed to retrieve file from MinIO: " + fileName, e);
      }
    });
  }

  @Test
  void testPostgresTableHas20000Rows() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection(postgresContainer.getJdbcUrl(), postgresContainer.getUsername(),
        postgresContainer.getPassword());
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
      rs.next();
      assertEquals(rs.getInt(1), NUM_ROWS, "Table should have " + NUM_ROWS + " rows");
    }
  }

  @Test
  void testExternalReferencesMatchMinioFiles() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection(postgresContainer.getJdbcUrl(), postgresContainer.getUsername(),
        postgresContainer.getPassword());
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT \"external-reference\" FROM " + TABLE_NAME + " ORDER BY id")) {

      int count = 0;
      while (rs.next()) {
        assertEquals(rs.getString("external-reference"), fileNames.get(count));
        count++;
      }
      assertEquals(count, NUM_ROWS, "Should have " + NUM_ROWS + " external references");
    }
  }

  @Test
  void testPostgresTableStructure() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection(postgresContainer.getJdbcUrl(), postgresContainer.getUsername(),
        postgresContainer.getPassword());
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT column_name, data_type FROM information_schema.columns "
        + "WHERE table_name = '" + TABLE_NAME + "' ORDER BY ordinal_position")) {

      rs.next();
      assertEquals(rs.getString("column_name"), "id", "First column should be 'id'");
      rs.next();
      assertEquals(rs.getString("column_name"), "title", "Second column should be 'title'");
      rs.next();
      assertEquals(rs.getString("column_name"), "author", "Third column should be 'author'");
      rs.next();
      assertEquals(rs.getString("column_name"), "category", "Fourth column should be 'category'");
      rs.next();
      assertEquals(rs.getString("column_name"), "external-reference", "Fifth column should be 'external-reference'");
    }
  }

  @Test
  void testExternalLobsFilter() throws IOException {

    System.setProperty("dbptk.external-lobs-filter.s3.max-queue-size", "10000");

    Path siardPath = tmpFolderSIARD.resolve("siard22-external-lobs-minio-postgres-test.siard");

    // Use the import config YAML to drive the migration with external LOBs from
    // MinIO
    String[] command = new String[] {"migrate", "--import=import-config",
      "--import-file=" + importConfigPath.toAbsolutePath(), "--export=siard-2",
      "--export-file=" + siardPath.toAbsolutePath(), "--export-version=2.2", "--filter=external-lobs"};

    Assert.assertEquals(Main.internalMainUsedOnlyByTestClasses(command), 0);
    assertTrue(Files.exists(siardPath), "SIARD file should exist after export");

    // Verify that the SIARD archive contains the files AND the exact sequential
    // ordering
    try (
      FileSystem zipfs = FileSystems.newFileSystem(URI.create("jar:" + siardPath.toUri()), Map.of("create", "false"))) {
      Path contentRoot = zipfs.getPath("content/");
      assertTrue(Files.exists(contentRoot), "SIARD content/ directory should exist");

      try (Stream<Path> lobFiles = Files.walk(contentRoot)) {
        List<Path> binFiles = lobFiles.filter(Files::isRegularFile).filter(p -> {
          String name = p.getFileName().toString();
          return name.startsWith("record") && name.endsWith(".bin");
        }).collect(Collectors.toList());

        assertEquals(binFiles.size(), NUM_ROWS,
          "SIARD archive should contain " + NUM_ROWS + " LOB files (record*.bin), found " + binFiles.size());

        // Crack open each record file inside the ZIP and verify the marker
        for (Path p : binFiles) {
          String name = p.getFileName().toString();

          // Extract the number from 'recordXXXX.bin'
          String numStr = name.replace("record", "").replace(".bin", "");
          int recordNum = Integer.parseInt(numStr);

          try (InputStream is = Files.newInputStream(p)) {
            byte[] buffer = new byte[64]; // Read just enough to capture the marker
            int bytesRead = is.read(buffer);
            String content = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);

            // Validate that the async queue did not mess up the row ordering
            String expectedMark = "ROW_INDEX:" + recordNum + "\n";
            assertTrue(content.startsWith(expectedMark),
              "ORDERING FAILED: File " + name + " contains wrong data! Expected '" + expectedMark.trim()
                + "' but got: '" + content.split("\n")[0] + "'");
          }
        }
      }
    }
  }
}