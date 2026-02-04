/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.testing.utils;

import org.apache.commons.lang3.NotImplementedException;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class MySQLDatabaseUtils extends DatabaseUtils {
  @Override
  public void dropAllTables(Connection conn, String databaseName) throws SQLException {
    List<String> tables = new ArrayList<>();

    try (PreparedStatement ps = conn
      .prepareStatement("SELECT table_name FROM information_schema.tables WHERE table_schema = ?")) {
      ps.setString(1, databaseName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          tables.add(rs.getString(1));
        }
      }
    }

    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET FOREIGN_KEY_CHECKS=0");
      for (String table : tables) {
        stmt.executeUpdate("DROP TABLE IF EXISTS `" + table + "`");
      }
      stmt.execute("SET FOREIGN_KEY_CHECKS=1");
    }

    log("Dropped " + tables.size() + " tables from MySQL database: " + databaseName);
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
    command.add("mysqldump");
    command.add("-h");
    command.add(container.getHost());
    command.add("-u");
    command.add(container.getUsername());
    command.add(String.format("-p%s", container.getPassword()));
    command.add("--compact");
    command.add(container.getDatabaseName());
    return command.toArray(new String[0]);
  }
}
