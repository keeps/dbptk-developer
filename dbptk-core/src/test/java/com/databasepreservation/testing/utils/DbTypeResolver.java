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
public class DbTypeResolver {

  private static final Map<String, DatabaseUtilsFactory.DbType> DRIVER_MAP = Map.of("com.mysql.cj.jdbc.Driver",
    DatabaseUtilsFactory.DbType.MYSQL, "org.postgresql.Driver", DatabaseUtilsFactory.DbType.POSTGRESQL);

  public static DatabaseUtilsFactory.DbType fromDriverClassName(String driverClassName) {
    return DRIVER_MAP.getOrDefault(driverClassName, DatabaseUtilsFactory.DbType.UNKNOWN);
  }
}
