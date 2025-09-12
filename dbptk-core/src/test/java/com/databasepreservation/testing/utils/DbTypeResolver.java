package com.databasepreservation.testing.utils;

/**
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class DbTypeResolver {

  public static DatabaseUtilsFactory.DbType fromDriverClassName(String driverClassName) {
    if (driverClassName == null) {
      return DatabaseUtilsFactory.DbType.UNKNOWN;
    }

    String driver = driverClassName.toLowerCase();

    if (driver.contains("mysql")) {
      return DatabaseUtilsFactory.DbType.MYSQL;
    } else if (driver.contains("postgresql")) {
      return DatabaseUtilsFactory.DbType.POSTGRESQL;
    } else {
      return DatabaseUtilsFactory.DbType.UNKNOWN;
    }
  }

}
