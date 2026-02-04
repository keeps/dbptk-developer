/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.testing.utils;

import org.testcontainers.containers.JdbcDatabaseContainer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class PostgreSQLDatabaseUtils extends DatabaseUtils {
  @Override
  public void dropAllTables(Connection conn, String schema) throws SQLException {
    List<String> tables = new ArrayList<>();

    try (Statement statement = conn.createStatement()) {
      try (ResultSet rs = statement.executeQuery("select tablename from pg_tables where schemaname = 'public'")) {
        while (rs.next()) {
          tables.add(rs.getString(1));
        }
      }
    }

    try (Statement stmt = conn.createStatement()) {
      for (String table : tables) {
        stmt.executeUpdate("DROP TABLE IF EXISTS \"" + "public" + "\".\"" + table + "\" CASCADE");
      }
    }

    log("Dropped " + tables.size() + " tables from PostgreSQL schema: " + schema);
  }

  @Override
  public void createTableIfNotExists(Connection conn, String tableCreationSQL) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(tableCreationSQL);
    }
  }

  @Override
  public String[] getDumpDatabaseCommand(JdbcDatabaseContainer<?> container) {
    List<String> command = new ArrayList<>();
    command.add("pg_dump");
    command.add("-h");
    command.add(container.getHost());
    command.add("-U");
    command.add(container.getUsername());
    command.add("-d");
    command.add(container.getDatabaseName());
    command.add("--restrict-key");
    command.add("12345");
    command.add("--format");
    command.add("plain");
    command.add("--no-owner");
    command.add("--no-privileges");
    command.add("--column-inserts");
    command.add("--no-security-labels");
    command.add("--no-tablespaces");
    return command.toArray(new String[0]);
  }
}
