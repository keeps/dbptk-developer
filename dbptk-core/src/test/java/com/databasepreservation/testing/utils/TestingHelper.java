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

  public static void cleanTargetDatabase(JdbcDatabaseContainer<?> jdbcDatabaseContainer) throws SQLException {
    try (Connection connTarget = DriverManager.getConnection(jdbcDatabaseContainer.getJdbcUrl(),
      jdbcDatabaseContainer.getUsername(), jdbcDatabaseContainer.getPassword());) {
      DatabaseUtils databaseUtils = DatabaseUtilsFactory
        .getDatabaseUtils(DbTypeResolver.fromDriverClassName(jdbcDatabaseContainer.getDriverClassName()));

      databaseUtils.dropAllTables(connTarget, jdbcDatabaseContainer.getDatabaseName());
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

  public static String[] getExportToSIARD2Command(String importModule, JdbcDatabaseContainer<?> container,
    String siardPath, String exportVersion, boolean externalLobs) {
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
      command.add("--export-external-lobs-blob-threshold-limit=0");
      command.add("--export-external-lobs-clob-threshold-limit=0");
    }
    return command.toArray(new String[0]);
  }

  private TestingHelper() {
    // Utility class
  }
}
