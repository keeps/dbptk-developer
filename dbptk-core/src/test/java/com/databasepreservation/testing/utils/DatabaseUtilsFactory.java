/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.testing.utils;

/**
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class DatabaseUtilsFactory {

  public enum DbType {
    MYSQL, POSTGRESQL, UNKNOWN
  }

  public static DatabaseUtils getDatabaseUtils(DbType type) {
    return switch (type) {
      case MYSQL -> new MySQLDatabaseUtils();
      case POSTGRESQL -> new PostgreSQLDatabaseUtils();
      default -> throw new IllegalArgumentException("Unsupported database type: " + type);
    };
  }
}
