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
