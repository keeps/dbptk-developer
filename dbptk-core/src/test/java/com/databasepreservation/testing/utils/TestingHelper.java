/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.testing.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class TestingHelper {
  private TestingHelper() {
    // Utility class
  }

  public static String createUniqueTableName(String baseTableName) {
    return baseTableName + "_" + System.currentTimeMillis();
  }

  public static void createTableIfNotExists(JdbcDatabaseContainer<?> container, String tableCreationSQL)
    throws SQLException {
    // 1. Get the utils from the cached factory
    DatabaseUtils utils = getUtils(container);

    try (Connection connection = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
      container.getPassword());) {
      utils.createTableIfNotExists(connection, tableCreationSQL);
    }
  }

  public static void cleanDatabase(JdbcDatabaseContainer<?> container) throws SQLException {
    // 1. Get the utils from the cached factory
    DatabaseUtils utils = getUtils(container);

    try (Connection connTarget = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
      container.getPassword());) {
      utils.dropAllTables(connTarget, container.getDatabaseName());
    }
  }

  public static String[] getCommandToExportToSIARDDK(String importModule, JdbcDatabaseContainer<?> container,
    String siardPath) {
    List<String> command = new ArrayList<>();
    command.add("migrate");
    command.add(String.format("--import=%s", importModule));
    command.add("--import-hostname=" + container.getHost());
    command.add("--import-port-number=" + container.getFirstMappedPort());
    command.add("--import-database");
    command.add(container.getDatabaseName());
    command.add("--import-username");
    command.add(container.getUsername());
    command.add("--import-password");
    command.add(container.getPassword());
    command.add("--import-disable-encryption");
    command.add("--export=siard-dk-1007");
    command.add("--export-folder");
    command.add(siardPath);
    return command.toArray(new String[0]);
  }

  public static String[] getCommandToExportToSIARD1(String importModule, JdbcDatabaseContainer<?> container,
    String siardPath, boolean externalLobs) {
    List<String> command = new ArrayList<>();
    command.add("migrate");
    command.add(String.format("--import=%s", importModule));
    command.add("--import-hostname=" + container.getHost());
    command.add("--import-port-number=" + container.getFirstMappedPort());
    command.add("--import-database");
    command.add(container.getDatabaseName());
    command.add("--import-username");
    command.add(container.getUsername());
    command.add("--import-password");
    command.add(container.getPassword());
    command.add("--import-disable-encryption");
    command.add("--export=siard-1");
    command.add("--export-file");
    command.add(siardPath);
    command.add("--export-pretty-xml");
    if (externalLobs) {
      command.add("--export-external-lobs");
      command.add("--export-external-lobs-blob-threshold-limit=0");
      command.add("--export-external-lobs-clob-threshold-limit=0");
    }

    return command.toArray(new String[0]);
  }

  public static String[] getCommandToImportFromSIARDDK(String exportModule, JdbcDatabaseContainer<?> container,
    String siardPath) {
    List<String> command = new ArrayList<>();

    command.add("migrate");
    command.add("--import=siard-dk-1007");
    command.add("--import-as-schema=targettestdb");
    command.add("--import-folder");
    command.add(siardPath);
    command.add(String.format("--export=%s", exportModule));
    command.add("--export-hostname=" + container.getHost());
    command.add("--export-port-number=" + container.getFirstMappedPort());
    command.add("--export-database");
    command.add(container.getDatabaseName());
    command.add("--export-username");
    command.add(container.getUsername());
    command.add("--export-password");
    command.add(container.getPassword());
    command.add("--export-disable-encryption");
    return command.toArray(new String[0]);
  }

  public static String[] getCommandToImportFromSIARD1(String exportModule, JdbcDatabaseContainer<?> container,
    String siardPath) {
    List<String> command = new ArrayList<>();
    command.add("migrate");
    command.add("--import=siard-1");
    command.add("--import-file");
    command.add(siardPath);
    command.add(String.format("--export=%s", exportModule));
    command.add("--export-hostname=" + container.getHost());
    command.add("--export-port-number=" + container.getFirstMappedPort());
    command.add("--export-database");
    command.add(container.getDatabaseName());
    command.add("--export-username");
    command.add(container.getUsername());
    command.add("--export-password");
    command.add(container.getPassword());
    command.add("--export-disable-encryption");
    return command.toArray(new String[0]);
  }

  public static String[] getDatabaseDumpCommand(JdbcDatabaseContainer<?> container) {
    DatabaseUtils databaseUtils = DatabaseUtilsFactory
      .getDatabaseUtils(DbTypeResolver.fromDriverClassName(container.getDriverClassName()));
    return databaseUtils.getDumpDatabaseCommand(container);
  }

  public static String[] getImportFromSIARD2Command(String exportModule, JdbcDatabaseContainer<?> container,
    String siardPath) {
    List<String> command = new ArrayList<>();
    command.add("migrate");
    command.add("--import=siard-2");
    command.add("--import-file");
    command.add(siardPath);
    command.add(String.format("--export=%s", exportModule));
    command.add("--export-hostname=" + container.getHost());
    command.add("--export-port-number=" + container.getFirstMappedPort());
    command.add("--export-database");
    command.add(container.getDatabaseName());
    command.add("--export-username");
    command.add(container.getUsername());
    command.add("--export-password");
    command.add(container.getPassword());
    command.add("--export-disable-encryption");
    return command.toArray(new String[0]);
  }

  public static String[] getCommandToExportToSIARD2(String importModule, JdbcDatabaseContainer<?> container,
    String siardPath, String exportVersion, boolean externalLobs, String clobThreshold, String blobThreshold) {
    List<String> command = new ArrayList<>();
    command.add("migrate");
    command.add(String.format("--import=%s", importModule));
    command.add("--import-hostname=" + container.getHost());
    command.add("--import-port-number=" + container.getFirstMappedPort());
    command.add("--import-database");
    command.add(container.getDatabaseName());
    command.add("--import-username");
    command.add(container.getUsername());
    command.add("--import-password");
    command.add(container.getPassword());
    command.add("--import-disable-encryption");
    command.add("--export=siard-2");
    command.add("--export-version");
    command.add(exportVersion);
    command.add("--export-file");
    command.add(siardPath);
    command.add("--export-pretty-xml");
    if (externalLobs) {
      command.add("--export-external-lobs");
      command.add("--export-external-lobs-blob-threshold-limit=" + blobThreshold);
      command.add("--export-external-lobs-clob-threshold-limit=" + clobThreshold);
    }
    return command.toArray(new String[0]);
  }

  public static String[] getCommandToExportToSIARD2(String importModule, JdbcDatabaseContainer<?> container,
    String siardPath, String exportVersion, boolean externalLobs) {
    return getCommandToExportToSIARD2(importModule, container, siardPath, exportVersion, externalLobs, "0", "0");
  }

  // Private internal helper to reduce boilerplate
  private static DatabaseUtils getUtils(JdbcDatabaseContainer<?> container) {
    return DatabaseUtilsFactory.getDatabaseUtils(DbTypeResolver.fromDriverClassName(container.getDriverClassName()));
  }
}
