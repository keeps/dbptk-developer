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
import java.sql.SQLException;

/**
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public abstract class DatabaseUtils {
  public abstract void dropAllTables(Connection conn, String databaseOrSchema) throws SQLException;

  public abstract String[] getDumpDatabaseCommand(JdbcDatabaseContainer<?> container);

  // Common helper methods can go here
  protected void log(String message) {
    System.out.println("[DatabaseUtils] " + message);
  }
}
