/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.testing.utils;

import java.util.Map;

/**
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class DatabaseUtilsFactory {
  // Cache the instances
  private static final Map<DbType, DatabaseUtils> CACHE = Map.of(DbType.MYSQL, new MySQLDatabaseUtils(),
    DbType.POSTGRESQL, new PostgreSQLDatabaseUtils());

  public static DatabaseUtils getDatabaseUtils(DbType type) {
    DatabaseUtils utils = CACHE.get(type);
    if (utils == null) {
      throw new IllegalArgumentException("Unsupported database type: " + type);
    }
    return utils;
  }

  public enum DbType {
    MYSQL, POSTGRESQL, UNKNOWN
  }
}
